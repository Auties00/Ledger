package com.github.auties00.ledger.engine;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32C;

/// Opens the frames of one segment of an encrypted log ahead of recovery, on virtual threads, while recovery replays
/// the frames before them.
///
/// Authenticating the frames is most of what recovery spends on a log whose commits it applies without reading the
/// trees. The JIT compiles the JDK's AES-GCM down to its intrinsic only once the cipher has been called thousands of
/// times, and a log holds a few hundred frames of hundreds of KiB each, so a recovery that makes no other call to the
/// cipher opens its whole log in the cipher's Java code, many times slower than the intrinsic. Opening the frames ahead
/// on the other processors takes that time off the thread that replays them.
///
/// What it reads ahead is a guess at the frames recovery meets: it follows the lengths the frame headers declare, and
/// the padding after a group to the next group's first frame, and stops at the first header that is not a whole header
/// of the segment's use. Recovery takes a frame read ahead only at the position it reached and only if the header it
/// read there is the one read ahead, byte for byte, so the body it takes is the one it would have opened itself, or the
/// same failure to authenticate; a frame it does not find ahead it opens itself, and decides, as before, what an
/// invalid frame means. Frames read ahead past the one where recovery stops are abandoned unread or waited for, so none
/// is opened once [#close()] returns, and nothing read ahead changes the store.
///
/// At most one frame per processor but the replaying one is open ahead, and at most [#AHEAD_BYTES] of sealed bytes
/// unless a single frame is longer; a frame longer than that is left to recovery. Used by the thread that replays the
/// segment only.
final class FrameReadAhead implements AutoCloseable {
    /// The sealed bytes the frames read ahead hold at most, unless one frame alone is longer.
    static final long AHEAD_BYTES = 8L << 20;
    // How many frames to read ahead in a store opened from now on, or -1 for one per processor but one; for tests
    private static volatile int framesForTests = -1;
    // The frames recovery took as read ahead, in any store; for tests
    private static final LongAdder TAKEN = new LongAdder();

    private final Path path;
    private final long size;
    private final int frames;
    private final ArrayDeque<Ahead> pending;
    private final CRC32C crc;
    // The arrays of the frames whose bodies were read and opened, for the next frames to read into
    private final ConcurrentLinkedQueue<byte[]> spareArrays;
    private final ConcurrentLinkedQueue<Cipher> ciphers;
    private long tag;
    private long useTag;
    private SecretKeySpec segmentKey;
    private FileChannel channel;
    // The position of the next frame to read ahead, or -1 once the guess stopped
    private long cursor;
    private long pendingBytes;

    /// Creates the read-ahead of the segment at `path`, holding `size` bytes, which reads nothing before [#begin].
    FrameReadAhead(Path path, long size) {
        this.path = path;
        this.size = size;
        var forTests = framesForTests;
        this.frames = forTests >= 0 ? forTests : Runtime.getRuntime().availableProcessors() - 1;
        this.pending = new ArrayDeque<>();
        this.crc = new CRC32C();
        this.spareArrays = new ConcurrentLinkedQueue<>();
        this.ciphers = new ConcurrentLinkedQueue<>();
        this.cursor = -1;
    }

    /// Sets how many frames a read-ahead created from now on opens ahead, or -1 for one per processor but one; for tests.
    static void framesForTests(int frames) {
        framesForTests = frames;
    }

    /// {@return the frames recovery took as read ahead since the process started, in any store; for tests}
    static long takenForTests() {
        return TAKEN.sum();
    }

    /// {@return whether it opens the frames of an encrypted segment ahead once it begins, rather than none}
    boolean opensFrames() {
        return frames > 0;
    }

    /// Starts reading ahead the frames of the use whose tag is `useTag` of the segment whose salt has `tag`, opened
    /// under `segmentKey`; a segment of a plain log, `segmentKey` `null`, is not read ahead.
    void begin(long tag, long useTag, SecretKeySpec segmentKey) {
        this.tag = tag;
        this.useTag = useTag;
        this.segmentKey = frames > 0 ? segmentKey : null;
    }

    /// {@return the frame at `position`, whose header recovery read as `header` and whose sealed body is
    /// `sealedLength` bytes, if it was read ahead: its body, or `null` for a frame that failed authentication; `null` if
    /// recovery must open it itself}
    ///
    /// It also reads ahead the frames after it, before it waits for the one asked for, so they open while recovery
    /// replays this one, or while recovery opens it itself.
    Opened take(byte[] header, long position, int sealedLength) {
        if (segmentKey == null) {
            return null;
        }
        Ahead found = null;
        while (!pending.isEmpty() && pending.peekFirst().position <= position) {
            var first = pending.pollFirst();
            pendingBytes -= first.sealedLength;
            if (first.position == position && first.sealedLength == sealedLength && Arrays.equals(first.header, header)) {
                found = first;
                break;
            }
            first.abandon();
        }
        if (found == null && pending.isEmpty()) {
            // The guess stopped or went another way: guess again from the frame recovery reached
            cursor = position + LogWriter.FRAME_HEADER + sealedLength;
        }
        fill();
        if (found == null) {
            return null;
        }
        var opened = found.await();
        if (opened != null) {
            TAKEN.increment();
        }
        return opened;
    }

    /// Reads ahead the frames from [#cursor] while fewer than [#frames] are open ahead and they hold less than
    /// [#AHEAD_BYTES].
    private void fill() {
        while (cursor >= 0 && pending.size() < frames && (pending.isEmpty() || pendingBytes < AHEAD_BYTES)) {
            try {
                var header = new byte[LogWriter.FRAME_HEADER];
                var position = cursor;
                if (!frameAt(header, position)) {
                    // Inside the padding after a group, which ends at a boundary where the next group's first frame
                    // may start; recovery also checks that the padding is zero, which the guess does not
                    position = (position / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
                    if (cursor % LogWriter.ALIGNMENT == 0 || !frameAt(header, position) || (header[17] & LogWriter.GROUP_START) == 0) {
                        cursor = -1;
                        return;
                    }
                }
                var sealedLength = ByteBuffer.wrap(header).getInt(4);
                var end = position + LogWriter.FRAME_HEADER + sealedLength;
                if (sealedLength < LogWriter.TAG_BYTES || end > size) {
                    cursor = -1;
                    return;
                }
                if (sealedLength <= AHEAD_BYTES) {
                    var ahead = new Ahead(position, header, sealedLength);
                    pending.addLast(ahead);
                    pendingBytes += sealedLength;
                    try {
                        Thread.ofVirtual().name("ledger-recovery-open").start(ahead);
                    } catch (Throwable _) {
                        // Recovery opens it on its own thread when it takes it
                    }
                }
                cursor = end;
            } catch (IOException _) {
                // Recovery reads the segment itself, and finds what failed
                cursor = -1;
                return;
            }
        }
    }

    /// Reads into `header` the frame header at `position`.
    ///
    /// @return whether it is a whole header of the segment's use
    private boolean frameAt(byte[] header, long position) throws IOException {
        return readFully(header, position) && LogRecovery.validFrameHeader(header, tag, useTag, crc);
    }

    /// Reads `target.length` bytes at `position` into `target`.
    ///
    /// @return whether the segment holds them all
    private boolean readFully(byte[] target, long position) throws IOException {
        return position >= 0 && position + target.length <= size && read(ByteBuffer.wrap(target), position);
    }

    /// Reads `buffer`'s remaining bytes at `position`, opening the segment on the first read.
    private boolean read(ByteBuffer buffer, long position) throws IOException {
        var opened = channel;
        if (opened == null) {
            opened = channel = FileChannel.open(path, StandardOpenOption.READ);
        }
        var start = buffer.position();
        while (buffer.hasRemaining()) {
            if (opened.read(buffer, position + buffer.position() - start) < 0) {
                return false;
            }
        }
        return true;
    }

    /// Abandons every frame read ahead, waiting for those being opened, then closes the segment.
    @Override
    public void close() throws IOException {
        while (!pending.isEmpty()) {
            pending.pollFirst().abandon();
        }
        pendingBytes = 0;
        cursor = -1;
        if (channel != null) {
            channel.close();
        }
    }

    /// A frame read ahead: its body, or `null` if it failed authentication.
    record Opened(byte[] body) {
    }

    /// A frame being read and opened ahead, by its own virtual thread or by recovery's when that takes it first.
    private final class Ahead implements Runnable {
        private static final int NEW = 0;
        private static final int RUNNING = 1;
        private static final int DONE = 2;

        private final long position;
        private final byte[] header;
        private final int sealedLength;
        private final AtomicInteger state;
        private final CountDownLatch done;
        // Written by the thread that runs it, before done is released
        private Opened opened;

        private Ahead(long position, byte[] header, int sealedLength) {
            this.position = position;
            this.header = header;
            this.sealedLength = sealedLength;
            this.state = new AtomicInteger(NEW);
            this.done = new CountDownLatch(1);
        }

        @Override
        public void run() {
            if (!state.compareAndSet(NEW, RUNNING)) {
                return;
            }
            try {
                opened = open();
            } catch (IOException | GeneralSecurityException | RuntimeException _) {
                // Recovery opens it itself, and finds what failed
                opened = null;
            } finally {
                state.set(DONE);
                done.countDown();
            }
        }

        /// {@return the frame's body, opened, or a frame with a `null` body if it failed authentication; `null` if
        /// it could not be read}
        private Opened open() throws IOException, GeneralSecurityException {
            var sealed = spareArrays.poll();
            if (sealed == null || sealed.length < sealedLength) {
                sealed = new byte[sealedLength];
            }
            try {
                if (!read(ByteBuffer.wrap(sealed, 0, sealedLength), position + LogWriter.FRAME_HEADER)) {
                    return null;
                }
                var cipher = ciphers.poll();
                if (cipher == null) {
                    cipher = Cipher.getInstance("AES/GCM/NoPadding");
                }
                try {
                    cipher.init(Cipher.DECRYPT_MODE, segmentKey, new GCMParameterSpec(LogWriter.TAG_BYTES * 8, LogWriter.nonce(position)));
                    cipher.updateAAD(header, 0, LogWriter.FRAME_HEADER);
                    return new Opened(cipher.doFinal(sealed, 0, sealedLength));
                } catch (AEADBadTagException _) {
                    return new Opened(null);
                } finally {
                    ciphers.offer(cipher);
                }
            } finally {
                spareArrays.offer(sealed);
            }
        }

        /// {@return the frame as [#run()] opened it, on this thread if no other started it; `null` if it could not}
        ///
        /// Waits whatever the interrupt status of the caller, which it keeps.
        private Opened await() {
            run();
            awaitDone();
            return opened;
        }

        /// Makes sure the frame is never opened, or waits for the thread opening it.
        private void abandon() {
            if (!state.compareAndSet(NEW, DONE)) {
                awaitDone();
            }
        }

        private void awaitDone() {
            var interrupted = false;
            while (true) {
                try {
                    done.await();
                    break;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

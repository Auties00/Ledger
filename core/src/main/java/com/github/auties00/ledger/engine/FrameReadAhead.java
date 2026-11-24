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
///
/// The frames are read into, and opened into, arrays of the [Spares] the read-aheads of one recovery share with it: a
/// frame's body is opened out of place into an array of its own, which recovery gives back once nothing it replayed
/// refers to it, so the frames of a whole log take a few arrays rather than two new ones each.
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
    // The arrays and ciphers of the recovery, which the frames are read and opened into
    private final Spares spares;
    private long tag;
    private long useTag;
    private SecretKeySpec segmentKey;
    private FileChannel channel;
    // The position of the next frame to read ahead, or -1 once the guess stopped
    private long cursor;
    private long pendingBytes;

    /// Creates the read-ahead of the segment at `path`, holding `size` bytes, which reads nothing before [#begin] and
    /// reads and opens its frames into arrays of `spares`.
    FrameReadAhead(Path path, long size, Spares spares) {
        this.path = path;
        this.size = size;
        this.spares = spares;
        var forTests = framesForTests;
        this.frames = forTests >= 0 ? forTests : Runtime.getRuntime().availableProcessors() - 1;
        this.pending = new ArrayDeque<>();
        this.crc = new CRC32C();
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
    /// `sealedLength` bytes, if it was read ahead: its body, on an array of the [Spares] that recovery owns from now on,
    /// or `null` for a frame that failed authentication; `null` if recovery must open it itself}
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

    /// A frame opened: its body in the first `length` bytes of `body`, an array of the [Spares] whose bytes after them
    /// may hold those of an earlier frame; or `body` `null` if it failed authentication.
    record Opened(byte[] body, int length) {
    }

    /// The arrays and ciphers the read-aheads of one recovery share with it, so the frames of every segment are read
    /// and opened into the arrays the earlier ones were.
    ///
    /// An array is handed out for a frame of at least its length: a spare long enough, or a new one of twice its
    /// length, so a frame longer than every one before it, up to twice the length of the one an array was made for,
    /// still finds a spare rather than dropping the spares in turn, and a log of growing frames makes a few arrays
    /// rather than one per new longest frame. The room never takes an array past a multiple of [#STEP], so it never
    /// gives an array more regions of G1 than its frame's length needs, nor makes one humongous that its length alone
    /// would not: every frame longer than half a [#STEP] takes the whole multiple that holds it. A spare shorter than
    /// the frame is dropped, as is one more than four times as long and longer than [#STEP], so a few long frames leave
    /// no long arrays to the short ones after them; an array for a frame longer than [#AHEAD_BYTES], which the
    /// read-ahead leaves to recovery, is never kept. Each array passes between the frame it holds and the queue, so the
    /// spares hold at most the arrays of the frames open at once.
    ///
    /// Shared by the replaying thread and the read-aheads' virtual threads. One recovery makes one and drops it after,
    /// and takes from it the bodies of the frames it reads itself too, those of a plain log included.
    static final class Spares {
        /// Half the smallest region of G1, the granularity of the room an array is given.
        static final int STEP = 1 << 19;
        // The bytes G1 adds to the elements of an array, its header, rounded up
        private static final int HEADER = 64;
        // The arrays handed out again, by any recovery; for tests
        private static final LongAdder REUSED = new LongAdder();

        private final ConcurrentLinkedQueue<byte[]> sealed = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<byte[]> bodies = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Cipher> ciphers = new ConcurrentLinkedQueue<>();

        /// {@return an array of at least `length` bytes for the sealed bytes of a frame, which goes back through
        /// [#giveSealed(byte\[\])]}
        byte[] takeSealed(int length) {
            return take(sealed, length);
        }

        /// Gives back `array`, which [#takeSealed(int)] handed out and nothing reads or writes any more.
        void giveSealed(byte[] array) {
            give(sealed, array);
        }

        /// {@return an array of at least `length` bytes for the body of a frame, which goes back through
        /// [#giveBody(byte\[\])]}
        byte[] takeBody(int length) {
            return take(bodies, length);
        }

        /// Gives back `body`, which [#takeBody(int)] handed out and nothing reads or writes any more, for a later frame
        /// to be opened into.
        void giveBody(byte[] body) {
            give(bodies, body);
        }

        /// {@return an AES-GCM cipher, which goes back through [#giveCipher(Cipher)]: an idle one of these spares, or
        /// one of the process's ([CipherPool])}
        Cipher takeCipher() {
            var cipher = ciphers.poll();
            return cipher != null ? cipher : CipherPool.GCM.take();
        }

        /// Gives back `cipher`, which [#takeCipher()] handed out and whose last call did not fail other than by
        /// authentication.
        void giveCipher(Cipher cipher) {
            ciphers.offer(cipher);
        }

        /// Gives the ciphers of these spares to the process's pool, once the recovery has closed every read-ahead and
        /// takes none any more.
        void releaseCiphers() {
            Cipher cipher;
            while ((cipher = ciphers.poll()) != null) {
                CipherPool.GCM.giveBack(cipher);
            }
        }

        /// {@return the arrays handed out again since the process started, by any recovery; for tests}
        static long reusedForTests() {
            return REUSED.sum();
        }

        private static byte[] take(ConcurrentLinkedQueue<byte[]> spares, int length) {
            if (length > AHEAD_BYTES) {
                return new byte[length];
            }
            byte[] spare;
            while ((spare = spares.poll()) != null) {
                if (spare.length >= length && (spare.length <= STEP || spare.length / 4 <= length)) {
                    REUSED.increment();
                    return spare;
                }
            }
            return new byte[room(length)];
        }

        private static void give(ConcurrentLinkedQueue<byte[]> spares, byte[] array) {
            if (array.length <= AHEAD_BYTES) {
                spares.offer(array);
            }
        }

        /// {@return the length of a new array for a frame of `length` bytes, at most [#AHEAD_BYTES]: twice `length`,
        /// rounded up to 4 KiB, but never past the multiple of [#STEP] that holds an array of `length` bytes, nor past
        /// [#AHEAD_BYTES]}
        static int room(int length) {
            var roomy = (2L * length + 4095) & ~4095L;
            var limit = ((long) length + HEADER + STEP - 1) / STEP * STEP - HEADER;
            return (int) Math.min(Math.min(roomy, limit), AHEAD_BYTES);
        }
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
            var sealed = spares.takeSealed(sealedLength);
            try {
                if (!read(ByteBuffer.wrap(sealed, 0, sealedLength), position + LogWriter.FRAME_HEADER)) {
                    return null;
                }
                var cipher = spares.takeCipher();
                var body = spares.takeBody(sealedLength - LogWriter.TAG_BYTES);
                var reusable = false;
                try {
                    cipher.init(Cipher.DECRYPT_MODE, segmentKey, new GCMParameterSpec(LogWriter.TAG_BYTES * 8, LogWriter.nonce(position)));
                    cipher.updateAAD(header, 0, LogWriter.FRAME_HEADER);
                    // Out of place, into an array other than the sealed one, which keeps the combined intrinsic
                    var opened = new Opened(body, cipher.doFinal(sealed, 0, sealedLength, body, 0));
                    body = null;
                    reusable = true;
                    return opened;
                } catch (AEADBadTagException _) {
                    reusable = true;
                    return new Opened(null, 0);
                } finally {
                    if (body != null) {
                        spares.giveBody(body);
                    }
                    // A cipher that failed otherwise is left to the collector
                    if (reusable) {
                        spares.giveCipher(cipher);
                    }
                }
            } finally {
                spares.giveSealed(sealed);
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

        /// Makes sure the frame is never opened, or waits for the thread opening it and gives back the body it opened,
        /// which recovery never takes.
        private void abandon() {
            if (!state.compareAndSet(NEW, DONE)) {
                awaitDone();
                var abandoned = opened;
                if (abandoned != null && abandoned.body() != null) {
                    spares.giveBody(abandoned.body());
                }
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

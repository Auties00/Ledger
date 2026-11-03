package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerIoException;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/// Appends frames to the log and makes them durable in groups.
///
/// Commits queue their frames in order, under the store's writer lock. One flush task at a time, on the store's
/// [com.github.auties00.ledger.IoPool], takes everything queued, seals the frames, writes them with one call, forces
/// the segment once and wakes the commits waiting for any of them: commits that arrive while a force runs make the
/// next group, so concurrent commits share forces. Each group is padded with zeros to a multiple of [#ALIGNMENT], where
/// the next one starts, so the pages of acknowledged frames are never written again.
///
/// Segments come from [LogFiles] prepared: once a segment is half written, the next one is prepared on the store's
/// pool, so a rollover only forces the full segment and switches to the next, and no file is created or renamed on
/// the way to a force. A prepared
/// file may hold the frames of an earlier use, which every frame header tells apart by the tag of its segment's salt.
///
/// A segment is a file of [#SEGMENT_HEADER] bytes of header followed by frames, written by one open of the store
/// only. Its key derives from its number and a random salt of its own, so the offset of a frame is a unique nonce
/// under that key, even if a segment number were ever used twice:
///
/// ```text
/// segment header   0  "LGSEGMNT"
///                  8  version
///                 12  encryption (1 if the frames are encrypted)
///                 16  segment number
///                 24  store id, 16 bytes
///                 40  salt of the segment key, 16 random bytes
///                 56  CRC-32C of bytes 0 to 55
/// frame header     0  magic "LGF1"
///                  4  length of the sealed body
///                  8  commit sequence
///                 16  frame type, flags (1: first frame after a force)
///                 20  tag: the first 8 bytes of the segment's salt
///                 28  CRC-32C of bytes 0 to 27
/// sealed body     AES-256-GCM of the body under the segment key, the frame's offset as nonce and the header as
///                 associated data; without encryption, the body and the CRC-32C of header and body
/// ```
///
/// A failed write or force stops the writer for good: [#awaitDurable(long)] and every later [#append] throw.
final class LogWriter {
    static final int SEGMENT_HEADER = 4_096;
    static final int ALIGNMENT = 4_096;
    static final int FRAME_HEADER = 32;
    static final int MAGIC = 0x4C474631;
    static final byte GROUP_START = 1;
    static final long SEGMENT_MAGIC = 0x4C475345474D4E54L;
    static final int SEGMENT_VERSION = 1;
    static final int TAG_BYTES = 16;
    static final int SALT_OFFSET = 40;
    static final int SALT_BYTES = 16;
    static final int HEADER_CRC_OFFSET = 56;
    private final LogFiles files;
    private final StoreKeys keys;
    private final byte[] storeId;
    private final long segmentSize;
    private final Executor executor;
    private final long commitWindowNanos;

    // Owned by the flush task
    private FileChannel channel;
    private long segmentNumber;
    private long position;
    private SecretKeySpec segmentKey;
    private long segmentTag;
    // The end of the last group's frames, before its padding, for tests
    private volatile long groupEnd;
    // The preparation of the segment after the current one, run on the pool or by the flush task itself
    private FutureTask<LogFiles.Prepared> next;
    private final Cipher cipher;
    private final CRC32C crc;
    private byte[] output;
    private byte[] flushing;
    // Written by the flush task only
    private volatile long forces;
    // The segment the flush task writes, for a checkpoint to read
    private volatile long currentSegment;

    private final ReentrantLock queueLock;
    private byte[] queue;
    private int queued;
    private long lastQueued;
    // Frames queued since the writer opened; a catalog frame carries a sequence already durable, so close() waits on
    // frames rather than on sequences
    private long appended;
    // The bytes of the frames queued since the writer opened, written under the queue lock
    private volatile long appendedBytes;
    private final AtomicBoolean scheduled;

    private final ReentrantLock durabilityLock;
    private final Condition durabilityChanged;
    private volatile long durable;
    private long written;
    private volatile LedgerIoException failure;

    /// Prepares a segment to write frames to.
    ///
    /// @param files the files of the log
    /// @param keys the store's keys
    /// @param storeId the store's id
    /// @param firstSegment the number of the segment to prepare, above every existing one
    /// @param durable the sequence the log already holds durably
    /// @param segmentSize the size past which a new segment starts
    /// @param executor runs the flush task on platform threads
    /// @param commitWindow how long a flush waits for more commits before it starts
    /// @throws IOException if the segment cannot be prepared
    LogWriter(LogFiles files, StoreKeys keys, byte[] storeId, long firstSegment, long durable, long segmentSize,
              Executor executor, Duration commitWindow) throws IOException {
        this.files = files;
        this.keys = keys;
        this.storeId = storeId;
        this.segmentSize = segmentSize;
        this.executor = executor;
        this.commitWindowNanos = commitWindow.toNanos();
        try {
            this.cipher = keys.encrypted() ? Cipher.getInstance("AES/GCM/NoPadding") : null;
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("AES-GCM is unavailable", exception);
        }
        this.crc = new CRC32C();
        this.output = new byte[64 * 1024];
        this.flushing = new byte[64 * 1024];
        this.queueLock = new ReentrantLock();
        this.queue = new byte[64 * 1024];
        this.scheduled = new AtomicBoolean();
        this.durabilityLock = new ReentrantLock();
        this.durabilityChanged = durabilityLock.newCondition();
        this.durable = durable;
        this.lastQueued = durable;
        open(files.prepare(firstSegment));
    }

    static String segmentName(long number) {
        return LogFiles.segmentName(number);
    }

    /// Starts writing `segment`.
    private void open(LogFiles.Prepared segment) throws IOException {
        this.channel = FileChannel.open(segment.path(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.segmentNumber = segment.number();
        this.currentSegment = segment.number();
        this.position = SEGMENT_HEADER;
        this.groupEnd = SEGMENT_HEADER;
        this.segmentTag = ByteBuffer.wrap(segment.salt()).getLong();
        this.segmentKey = keys.encrypted() ? keys.segmentKey(storeId, segment.number(), segment.salt()) : null;
        this.next = null;
    }

    /// Prepares the segment after the current one on the pool.
    private void prepareNext() {
        var number = segmentNumber + 1;
        var preparation = new FutureTask<>(() -> files.prepare(number));
        next = preparation;
        executor.execute(preparation);
    }

    /// Switches to the prepared segment after the current one, preparing it here if the pool has not.
    private void rollover() throws IOException {
        if (next == null) {
            prepareNext();
        }
        var preparation = next;
        // Runs it on this thread unless it started elsewhere, so a pool of one thread never waits on itself
        preparation.run();
        LogFiles.Prepared prepared;
        try {
            prepared = preparation.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while preparing a log segment", exception);
        } catch (ExecutionException exception) {
            throw exception.getCause() instanceof IOException failure ? failure
                    : new IOException("A log segment could not be prepared", exception.getCause());
        }
        open(prepared);
    }

    /// Queues a frame holding the first `body.size()` bytes of `body`, copying them.
    ///
    /// Called with the store's writer lock held, so frames queue in the order of their sequences.
    ///
    /// @throws LedgerIoException if the writer has stopped
    void append(byte type, long sequence, FrameBody body) {
        checkHealthy();
        var length = body.size();
        queueLock.lock();
        try {
            var needed = queued + 13 + length;
            if (needed > queue.length) {
                queue = Arrays.copyOf(queue, Math.max(needed, queue.length * 2));
            }
            queue[queued] = type;
            ByteBuffer.wrap(queue, queued + 1, 12).putLong(sequence).putInt(length);
            System.arraycopy(body.array(), 0, queue, queued + 13, length);
            queued = needed;
            lastQueued = sequence;
            appended++;
            appendedBytes += 13 + length;
        } finally {
            queueLock.unlock();
        }
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
    }

    /// Throws if the writer has stopped after a failed write.
    ///
    /// @throws LedgerIoException if it has
    void checkHealthy() {
        var stopped = failure;
        if (stopped != null) {
            throw stopped;
        }
    }

    private void flush() {
        try {
            while (true) {
                if (commitWindowNanos > 0) {
                    LockSupport.parkNanos(commitWindowNanos);
                }
                byte[] records;
                int length;
                long sequence;
                long frames;
                queueLock.lock();
                try {
                    if (queued == 0) {
                        scheduled.set(false);
                        return;
                    }
                    records = queue;
                    length = queued;
                    sequence = lastQueued;
                    frames = appended;
                    queue = flushing;
                    queued = 0;
                } finally {
                    queueLock.unlock();
                }
                write(records, length);
                flushing = records;
                publish(sequence, frames);
            }
        } catch (IOException exception) {
            stop(exception);
        } catch (RuntimeException | Error throwable) {
            stop(new IOException("The log writer failed", throwable));
        }
    }

    private void write(byte[] records, int length) throws IOException {
        var used = 0;
        var groupStart = true;
        var offset = 0;
        while (offset < length) {
            var type = records[offset];
            var fields = ByteBuffer.wrap(records, offset + 1, 12);
            var sequence = fields.getLong();
            var bodyLength = fields.getInt();
            var bodyOffset = offset + 13;
            var sealedLength = bodyLength + (keys.encrypted() ? TAG_BYTES : Integer.BYTES);
            var frameLength = FRAME_HEADER + sealedLength;
            if (position + used + frameLength > segmentSize && position + used > SEGMENT_HEADER) {
                writeFully(used);
                channel.force(false);
                forces++;
                channel.close();
                rollover();
                used = 0;
                groupStart = true;
            }
            if (used + frameLength > output.length) {
                output = Arrays.copyOf(output, Math.max(used + frameLength, output.length * 2));
            }
            seal(type, sequence, records, bodyOffset, bodyLength, sealedLength, position + used, used, groupStart);
            used += frameLength;
            groupStart = false;
            offset = bodyOffset + bodyLength;
        }
        // Zeros to the next block, where the next group starts: a reused file holds older bytes there
        var end = position + used;
        var padded = (int) ((end + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT - position);
        if (padded > output.length) {
            output = Arrays.copyOf(output, padded);
        }
        Arrays.fill(output, used, padded, (byte) 0);
        writeFully(padded);
        channel.force(false);
        forces++;
        groupEnd = end;
        if (next == null && position >= segmentSize / 2) {
            prepareNext();
        }
    }

    private void seal(byte type, long sequence, byte[] records, int bodyOffset, int bodyLength, int sealedLength,
                      long frameOffset, int at, boolean groupStart) throws IOException {
        var header = ByteBuffer.wrap(output, at, FRAME_HEADER);
        header.putInt(MAGIC).putInt(sealedLength).putLong(sequence).put(type).put(groupStart ? GROUP_START : 0)
                .putShort((short) 0).putLong(segmentTag);
        crc.reset();
        crc.update(output, at, FRAME_HEADER - Integer.BYTES);
        header.putInt((int) crc.getValue());
        if (cipher != null) {
            try {
                cipher.init(Cipher.ENCRYPT_MODE, segmentKey, new GCMParameterSpec(TAG_BYTES * 8, nonce(frameOffset)));
                cipher.updateAAD(output, at, FRAME_HEADER);
                cipher.doFinal(records, bodyOffset, bodyLength, output, at + FRAME_HEADER);
            } catch (GeneralSecurityException exception) {
                throw new IOException("A log frame could not be sealed", exception);
            }
        } else {
            System.arraycopy(records, bodyOffset, output, at + FRAME_HEADER, bodyLength);
            crc.reset();
            crc.update(output, at, FRAME_HEADER + bodyLength);
            ByteBuffer.wrap(output, at + FRAME_HEADER + bodyLength, Integer.BYTES).putInt((int) crc.getValue());
        }
    }

    /// {@return the GCM nonce of the frame at `offset` of its segment}
    static byte[] nonce(long offset) {
        return ByteBuffer.allocate(12).putInt(0).putLong(offset).array();
    }

    private void writeFully(int length) throws IOException {
        var buffer = ByteBuffer.wrap(output, 0, length);
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }

    private void publish(long sequence, long frames) {
        durabilityLock.lock();
        try {
            durable = sequence;
            written = frames;
            durabilityChanged.signalAll();
        } finally {
            durabilityLock.unlock();
        }
    }

    private void stop(IOException cause) {
        durabilityLock.lock();
        try {
            failure = new LedgerIoException("The log could not be written: commits since the last force may be lost", cause);
            durabilityChanged.signalAll();
        } finally {
            durabilityLock.unlock();
        }
    }

    /// {@return whether the commit at `sequence` is durable}
    ///
    /// @throws LedgerIoException if the writer stopped before it was
    boolean isDurable(long sequence) {
        if (durable >= sequence) {
            return true;
        }
        checkHealthy();
        return false;
    }

    /// Waits until the commit at `sequence` is durable.
    ///
    /// @throws InterruptedException if interrupted while waiting
    /// @throws LedgerIoException if the writer stopped before it was
    void awaitDurable(long sequence) throws InterruptedException {
        if (durable >= sequence) {
            return;
        }
        durabilityLock.lockInterruptibly();
        try {
            while (durable < sequence) {
                checkHealthy();
                durabilityChanged.await();
            }
        } finally {
            durabilityLock.unlock();
        }
    }

    /// Waits until the commit at `sequence` is durable or `timeout` elapses.
    ///
    /// @return whether it is durable
    /// @throws InterruptedException if interrupted while waiting
    /// @throws LedgerIoException if the writer stopped before it was
    boolean awaitDurable(long sequence, Duration timeout) throws InterruptedException {
        if (durable >= sequence) {
            return true;
        }
        var remaining = timeout.toNanos();
        durabilityLock.lockInterruptibly();
        try {
            while (durable < sequence) {
                checkHealthy();
                if (remaining <= 0) {
                    return false;
                }
                remaining = durabilityChanged.awaitNanos(remaining);
            }
            return true;
        } finally {
            durabilityLock.unlock();
        }
    }

    /// {@return the number of the segment frames are written to now; every later frame goes to it or a later one}
    long currentSegment() {
        return currentSegment;
    }

    /// {@return the bytes of the frames queued since the writer opened}
    long appendedBytes() {
        return appendedBytes;
    }

    /// {@return the offset in the current segment where the frames of the last group end, before their padding}
    long groupEnd() {
        return groupEnd;
    }

    /// {@return the forces made so far}
    long forces() {
        return forces;
    }

    /// Makes everything queued durable, then closes the segment.
    ///
    /// @throws LedgerIoException if the queued frames cannot be made durable
    void close() {
        long frames;
        queueLock.lock();
        try {
            frames = appended;
        } finally {
            queueLock.unlock();
        }
        try {
            durabilityLock.lock();
            try {
                while (written < frames) {
                    checkHealthy();
                    durabilityChanged.awaitUninterruptibly();
                }
            } finally {
                durabilityLock.unlock();
            }
        } finally {
            // The flush task has nothing left to write, or has stopped
            closeChannel();
            spareNext();
        }
    }

    /// Turns the segment prepared after the current one, which nothing wrote, back into a spare.
    private void spareNext() {
        var preparation = next;
        // A preparation that has not started never runs; one still running leaves an unused segment to the next open
        if (preparation == null || preparation.cancel(false)) {
            return;
        }
        try {
            files.spare(preparation.get().number());
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | IOException _) {
            // An unused segment left behind holds no frame; the next open turns it into a spare
        }
    }

    private void closeChannel() {
        // No flush runs: everything queued is durable, or the writer stopped
        try {
            channel.close();
        } catch (IOException _) {
            // The data is durable already; nothing to recover from a failed close
        }
    }

}

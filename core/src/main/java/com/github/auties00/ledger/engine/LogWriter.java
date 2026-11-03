package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerIoException;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executor;
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
/// next group, so concurrent commits share forces. After each force the next group starts at a multiple of
/// [#ALIGNMENT], so the pages of acknowledged frames are never written again.
///
/// A segment is a file of [#SEGMENT_HEADER] bytes of header followed by frames, written by one open of the store
/// only, so the offset of a frame is a unique nonce under the segment's key:
///
/// ```text
/// segment header  "LGSEGMNT", version, encryption, segment number, store id, CRC-32C of the header
/// frame header     0  magic "LGF1"
///                  4  length of the sealed body
///                  8  commit sequence
///                 16  frame type, flags (1: first frame after a force)
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

    private final Path directory;
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
    private final Cipher cipher;
    private final CRC32C crc;
    private byte[] output;
    private byte[] flushing;
    // Written by the flush task only
    private volatile long forces;

    private final ReentrantLock queueLock;
    private byte[] queue;
    private int queued;
    private long lastQueued;
    // Frames queued since the writer opened; a catalog frame carries a sequence already durable, so close() waits on
    // frames rather than on sequences
    private long appended;
    private final AtomicBoolean scheduled;

    private final ReentrantLock durabilityLock;
    private final Condition durabilityChanged;
    private volatile long durable;
    private long written;
    private volatile LedgerIoException failure;

    /// Opens a new segment to write frames to.
    ///
    /// @param directory the directory of the segments
    /// @param keys the store's keys
    /// @param storeId the store's id
    /// @param firstSegment the number of the segment to create, above every existing one
    /// @param durable the sequence the log already holds durably
    /// @param segmentSize the size past which a new segment starts
    /// @param executor runs the flush task on platform threads
    /// @param commitWindow how long a flush waits for more commits before it starts
    /// @throws IOException if the segment cannot be created
    LogWriter(Path directory, StoreKeys keys, byte[] storeId, long firstSegment, long durable, long segmentSize,
              Executor executor, Duration commitWindow) throws IOException {
        this.directory = directory;
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
        createSegment(firstSegment);
    }

    static String segmentName(long number) {
        return "%016x.log".formatted(number);
    }

    private void createSegment(long number) throws IOException {
        var path = directory.resolve(segmentName(number));
        var created = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            var header = ByteBuffer.allocate(SEGMENT_HEADER);
            header.putLong(0, SEGMENT_MAGIC);
            header.putInt(8, SEGMENT_VERSION);
            header.put(12, (byte) (keys.encrypted() ? 1 : 0));
            header.putLong(16, number);
            header.put(24, storeId);
            crc.reset();
            crc.update(header.array(), 0, 40);
            header.putInt(40, (int) crc.getValue());
            while (header.hasRemaining()) {
                created.write(header, header.position());
            }
            created.force(true);
            Durability.forceDirectory(directory);
        } catch (IOException | RuntimeException exception) {
            created.close();
            throw exception;
        }
        this.channel = created;
        this.segmentNumber = number;
        this.position = SEGMENT_HEADER;
        this.segmentKey = keys.encrypted() ? keys.segmentKey(storeId, number) : null;
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
                createSegment(segmentNumber + 1);
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
        writeFully(used);
        channel.force(false);
        forces++;
        position = (position + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
    }

    private void seal(byte type, long sequence, byte[] records, int bodyOffset, int bodyLength, int sealedLength,
                      long frameOffset, int at, boolean groupStart) throws IOException {
        var header = ByteBuffer.wrap(output, at, FRAME_HEADER);
        header.putInt(MAGIC).putInt(sealedLength).putLong(sequence).put(type).put(groupStart ? GROUP_START : 0)
                .putShort((short) 0).putInt(0).putInt(0);
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

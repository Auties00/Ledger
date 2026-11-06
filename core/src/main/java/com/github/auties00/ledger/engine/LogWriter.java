package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerIoException;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
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
/// Commits queue their frames in order, holding the store's publishing role. One flush task at a time, on the store's
/// [com.github.auties00.ledger.IoPool], takes everything queued, seals the frames, writes them with one call, forces
/// the segment once and wakes the commits waiting for any of them: commits that arrive while a force runs make the
/// next group, so concurrent commits share forces. Each group is padded with zeros to a multiple of [#ALIGNMENT], where
/// the next one starts, so the pages of acknowledged frames are never written again.
///
/// Segments come from [LogFiles] prepared: once a segment takes its first group, the next one is prepared on a thread
/// of its own, so a rollover only forces the full segment and switches to the next, and no file is created or renamed
/// on the way to a force. The first one is prepared the same way while the store opens, and the first group waits for
/// it: forcing a new file and its directory took most of an open on Windows and macOS. A prepared file may hold the
/// frames of an earlier use, which every frame header tells apart by the tag of its segment's salt.
///
/// A new file is written with zeros first only while the commits wait for the forces and the log fills slower than
/// [#ZEROED_RATE]: the zeros halve the time of a force on Linux, but double the bytes the device takes, which slowed an
/// ingest of a history, whose commits do not wait, and 256 committers writing 100 MB a second, by a fifth.
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
    /// The size of the buffer frames are sealed into and written from.
    private static final int OUTPUT = 1 << 20;
    /// The bytes of a frame body sealed in one call, a multiple of the AES block.
    private static final int SLICE = 64 << 10;
    /// The bytes of frame bodies the queue holds before commits wait for the device.
    private static final long QUEUE_LIMIT = 64L << 20;
    /// The bytes a second of log below which a new segment is written with zeros first, if the commits wait for it.
    private static final long ZEROED_RATE = 64L << 20;
    private final LogFiles files;
    private final StoreKeys keys;
    private final byte[] storeId;
    private final long segmentSize;
    private final Executor executor;
    private final long commitWindowNanos;

    // Owned by the flush task; null until the first group opens the first segment
    private FileChannel channel;
    // The file of the current segment, which an interrupted write reopens
    private java.nio.file.Path segmentPath;
    // Whether an interrupt closed the channel during the flush, which then gives the interrupt back to its thread
    private boolean interruptedWrite;
    // Empties the drive's cache for the segment on macOS, or null
    private FullSync.Handle fullSync;
    private long segmentNumber;
    private long position;
    private SecretKeySpec segmentKey;
    private long segmentTag;
    // The end of the last group's frames, before its padding, for tests
    private volatile long groupEnd;
    // Where the frames of the last group start, for tests
    private volatile long groupStart;
    // The preparation of the segment after the current one, run on the pool or by the flush task itself
    private FutureTask<LogFiles.Prepared> next;
    // Set by the preparation in next when it starts, or by a close first, which it then leaves undone
    private AtomicBoolean nextClaimed;
    // When the current segment opened, and how long the one before it took to fill, or -1; owned by the flush task
    private long segmentOpened;
    private long lastFill = -1;
    // Whether a commit waited for a force since the segment before the current one opened
    private volatile boolean waited;
    private boolean waitedBefore;
    private final Cipher cipher;
    private final CRC32C crc;
    // The buffer frames are sealed into and written from, a slice at a time whatever their size
    private final byte[] output;
    // The bytes of output not written yet
    private int used;
    // The emptied queue, which the next swap reuses
    private ArrayList<Queued> flushing;
    // Written by the flush task only
    private volatile long forces;
    // The segment the flush task writes, for a checkpoint to read
    private volatile long currentSegment;

    private final ReentrantLock queueLock;
    // Signalled when a group is durable, for the commits waiting on a full queue
    private final Condition queueDrained;
    private ArrayList<Queued> queue;
    // The bytes of the frame bodies queued and not durable yet
    private long pendingBytes;
    private long lastQueued;
    // Frames queued since the writer opened; a catalog frame carries a sequence already durable, so close() waits on
    // frames rather than on sequences
    private long appended;
    // The bytes of the frames queued since the writer opened, and of the units of BlobFile their commits wrote, written
    // under the queue lock
    private volatile long appendedBytes;
    // Whether a queued frame's commit wrote a unit of BlobFile, which the next group forces; guarded by the queue lock
    private boolean queuedUnits;
    // The file of large values the commits write to, or null
    private volatile BlobFile blobs;
    private final AtomicBoolean scheduled;
    // Held by the one flush that writes groups, on the pool or on the thread of a commit waiting for its group
    private final AtomicBoolean running;
    // The commits waiting for durability
    private final ConcurrentLinkedQueue<Waiter> waiters;

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
        this.output = new byte[OUTPUT];
        this.flushing = new ArrayList<>();
        this.queueLock = new ReentrantLock();
        this.queueDrained = queueLock.newCondition();
        this.queue = new ArrayList<>();
        this.scheduled = new AtomicBoolean();
        this.running = new AtomicBoolean();
        this.waiters = new ConcurrentLinkedQueue<>();
        this.durabilityLock = new ReentrantLock();
        this.durabilityChanged = durabilityLock.newCondition();
        this.durable = durable;
        this.lastQueued = durable;
        // The first group switches to it, as a rollover does
        this.segmentNumber = firstSegment - 1;
        this.currentSegment = firstSegment;
        prepareNext();
    }

    static String segmentName(long number) {
        return LogFiles.segmentName(number);
    }

    /// Starts writing `segment`.
    private void open(LogFiles.Prepared segment) throws IOException {
        var now = System.nanoTime();
        if (channel != null) {
            lastFill = now - segmentOpened;
        }
        segmentOpened = now;
        waitedBefore = waited;
        waited = false;
        this.channel = FileChannel.open(segment.path(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.segmentPath = segment.path();
        this.fullSync = FullSync.open(segment.path());
        this.segmentNumber = segment.number();
        this.currentSegment = segment.number();
        this.position = SEGMENT_HEADER;
        this.groupEnd = SEGMENT_HEADER;
        this.segmentTag = ByteBuffer.wrap(segment.salt()).getLong();
        this.segmentKey = keys.encrypted() ? keys.segmentKey(storeId, segment.number(), segment.salt()) : null;
        this.next = null;
    }

    /// Prepares the segment after the current one on a platform thread of its own.
    ///
    /// Not on the store's pool: creating and forcing a file costs tens of milliseconds on Windows, and a flush queued
    /// behind it would make every commit waiting on that flush wait as long.
    private void prepareNext() {
        var number = segmentNumber + 1;
        var claimed = new AtomicBoolean();
        // Filled slower than the rate, by commits that waited for its forces
        var zeros = (waited || waitedBefore) && lastFill >= 0 && lastFill > segmentSize * 1_000_000_000L / ZEROED_RATE;
        var preparation = new FutureTask<>(() -> claimed.compareAndSet(false, true) ? files.prepare(number, segmentSize, zeros) : null);
        next = preparation;
        nextClaimed = claimed;
        Thread.ofPlatform().daemon().name("ledger-prepare").start(preparation);
    }

    /// Switches to the prepared segment after the current one, preparing it here if its thread has not.
    private void rollover() throws IOException {
        if (next == null) {
            prepareNext();
        }
        var preparation = next;
        // Runs it on this thread unless it started elsewhere, so a rollover never waits on a thread yet to start
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

    /// Queues a frame holding the first `body.size()` bytes of `body`, which the writer owns from now on.
    ///
    /// Called holding the store's publishing role, so frames queue in the order of their sequences. While the queue holds
    /// more than [#QUEUE_LIMIT] bytes not durable yet, it waits for the device, which holds back every commit behind
    /// it: memory stays bounded when commits come faster than the device takes them. A frame larger than the limit
    /// waits for the queue to empty.
    ///
    /// @throws LedgerIoException if the writer has stopped
    void append(byte type, long sequence, FrameBody body) {
        append(type, sequence, body, 0);
    }

    /// Queues a frame as [#append(byte, long, FrameBody)] does, for a commit that wrote `unitBytes` of units of the
    /// file of large values.
    ///
    /// The group that holds the frame forces that file beside the segment. The units count toward the growth that
    /// starts a checkpoint, as if their values were in the frame: memory holds them until a checkpoint takes them.
    ///
    /// @return the frames queued since the writer opened, the new one included
    /// @throws LedgerIoException if the writer has stopped
    long append(byte type, long sequence, FrameBody body, long unitBytes) {
        checkHealthy();
        var length = body.size();
        long frames;
        queueLock.lock();
        try {
            while (pendingBytes > 0 && pendingBytes + length > QUEUE_LIMIT && failure == null) {
                queueDrained.awaitUninterruptibly();
            }
            checkHealthy();
            queue.add(new Queued(type, sequence, body));
            pendingBytes += length;
            lastQueued = sequence;
            frames = ++appended;
            appendedBytes += 13 + length + unitBytes;
            queuedUnits |= unitBytes > 0;
        } finally {
            queueLock.unlock();
        }
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
        return frames;
    }

    /// Queues the frames of `batch`, in its order, as [#append(byte, long, FrameBody, long)] queues each, and empties
    /// it: one hold of the queue lock for a whole pass of the store's publisher.
    ///
    /// The batch waits on a full queue as one frame of its total size would. Either every frame is queued or, if the
    /// writer has stopped, none is.
    ///
    /// @throws LedgerIoException if the writer has stopped
    void appendAll(Batch batch) {
        if (batch.frames.isEmpty()) {
            return;
        }
        checkHealthy();
        queueLock.lock();
        try {
            while (pendingBytes > 0 && pendingBytes + batch.bodyBytes > QUEUE_LIMIT && failure == null) {
                queueDrained.awaitUninterruptibly();
            }
            checkHealthy();
            queue.addAll(batch.frames);
            pendingBytes += batch.bodyBytes;
            lastQueued = batch.frames.getLast().sequence();
            appended += batch.frames.size();
            appendedBytes += batch.appendedBytes;
            queuedUnits |= batch.units;
        } finally {
            queueLock.unlock();
            batch.clear();
        }
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
    }

    /// Makes `blobs` the file of large values the commits write units to, which the groups force.
    void useBlobs(BlobFile blobs) {
        this.blobs = blobs;
    }

    /// Waits until the first `frames` frames queued since the writer opened are durable.
    ///
    /// @throws LedgerIoException if the writer stops first
    void awaitFrames(long frames) {
        durabilityLock.lock();
        try {
            while (written < frames) {
                checkHealthy();
                durabilityChanged.awaitUninterruptibly();
            }
        } finally {
            durabilityLock.unlock();
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
        flush(Long.MAX_VALUE);
    }

    /// Writes the queued groups until the queue is empty, or until the commit at `until` is durable, for a commit that
    /// waits for its group on its own thread rather than for a thread of the pool to wake, then for its own to be woken.
    ///
    /// One flush writes at a time: another returns at once, and the one that writes looks at the queue again once it
    /// stops, so the frames of a commit whose flush returned are never left behind. A flush for a commit hands what is
    /// queued after its group to the pool.
    private void flush(long until) {
        while (running.compareAndSet(false, true)) {
            try {
                flushGroups(until);
            } finally {
                running.set(false);
            }
            boolean empty;
            queueLock.lock();
            try {
                empty = queue.isEmpty();
                if (empty) {
                    scheduled.set(false);
                }
            } finally {
                queueLock.unlock();
            }
            if (empty || failure != null) {
                return;
            }
            if (until != Long.MAX_VALUE) {
                executor.execute(this::flush);
                return;
            }
        }
    }

    /// {@return whether `frames` seal into the output and the current segment, so the group is written with one call
    /// that a write cut off by an interrupt repeats, with no rollover to prepare a segment on the caller's thread}
    private boolean fitsOneWrite(List<Queued> frames) {
        if (channel == null) {
            return false;
        }
        var bytes = 0L;
        for (var frame : frames) {
            bytes += FRAME_HEADER + frame.body().size() + (keys.encrypted() ? TAG_BYTES : Integer.BYTES);
            if (bytes > OUTPUT - ALIGNMENT) {
                return false;
            }
        }
        return position + bytes + ALIGNMENT <= segmentSize;
    }

    /// Writes the queued groups until the queue is empty or the commit at `until` is durable, holding [#running].
    private void flushGroups(long until) {
        try {
            while (durable < until) {
                if (commitWindowNanos > 0) {
                    LockSupport.parkNanos(commitWindowNanos);
                }
                ArrayList<Queued> group;
                long sequence;
                long frames;
                boolean units;
                queueLock.lock();
                try {
                    if (queue.isEmpty() || (until != Long.MAX_VALUE && !fitsOneWrite(queue))) {
                        return;
                    }
                    group = queue;
                    sequence = lastQueued;
                    frames = appended;
                    units = queuedUnits;
                    queuedUnits = false;
                    queue = flushing;
                } finally {
                    queueLock.unlock();
                }
                var bodies = write(group, units);
                group.clear();
                flushing = group;
                queueLock.lock();
                try {
                    pendingBytes -= bodies;
                    queueDrained.signalAll();
                } finally {
                    queueLock.unlock();
                }
                publish(sequence, frames);
            }
        } catch (IOException exception) {
            stop(exception);
        } catch (RuntimeException | Error throwable) {
            stop(new IOException("The log writer failed", throwable));
        } finally {
            // A commit's thread gets its interrupt back; a thread of the pool, which nothing should interrupt, not
            if (interruptedWrite) {
                interruptedWrite = false;
                if (until != Long.MAX_VALUE) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /// Opens the segment again after an interrupt of the thread writing it closed its channel, with the interrupt
    /// cleared until the flush ends, so a commit that writes its own group survives an interrupt.
    private void reopen() throws IOException {
        interruptedWrite |= Thread.interrupted();
        channel = FileChannel.open(segmentPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    /// Seals and writes `group`, then forces it, and the file of large values if `units`.
    ///
    /// The commits of the group wrote their units before they queued their frames, so one force of that file, run
    /// beside the segment's, makes them all durable. A rollover waits for it before it forces the segment: the first
    /// frame of a group in the next segment then means, as it always does, that everything before it is durable.
    ///
    /// @return the bytes of the frame bodies written
    private long write(List<Queued> group, boolean units) throws IOException {
        if (channel == null) {
            rollover();
        }
        var unitForce = units ? forceUnits() : null;
        var start = position;
        var groupStart = true;
        var bodies = 0L;
        for (var frame : group) {
            var bodyLength = frame.body().size();
            bodies += bodyLength;
            var sealedLength = bodyLength + (keys.encrypted() ? TAG_BYTES : Integer.BYTES);
            if (position + used + FRAME_HEADER + sealedLength > segmentSize && position + used > SEGMENT_HEADER) {
                drain();
                if (unitForce != null) {
                    await(unitForce);
                    unitForce = null;
                }
                force();
                forces++;
                closeSegment();
                rollover();
                start = position;
                groupStart = true;
            }
            seal(frame, sealedLength, groupStart);
            groupStart = false;
        }
        // Zeros to the next block, where the next group starts: a reused file holds older bytes there
        var end = position + used;
        var padding = (int) ((end + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT - end);
        room(padding);
        Arrays.fill(output, used, used + padding, (byte) 0);
        used += padding;
        drain();
        force();
        forces++;
        if (unitForce != null) {
            await(unitForce);
        }
        this.groupStart = start;
        groupEnd = end;
        if (next == null) {
            prepareNext();
        }
        return bodies;
    }

    /// Seals `frame` into the output, writing the output out whenever it fills.
    ///
    /// The body goes to the cipher a slice at a time: the frame is the one GCM message a single call would make, and
    /// the output stays [#OUTPUT] bytes whatever the frame's size.
    private void seal(Queued frame, int sealedLength, boolean groupStart) throws IOException {
        room(FRAME_HEADER);
        var at = used;
        var frameOffset = position + used;
        var header = ByteBuffer.wrap(output, at, FRAME_HEADER);
        header.putInt(MAGIC).putInt(sealedLength).putLong(frame.sequence()).put(frame.type()).put(groupStart ? GROUP_START : 0)
                .putShort((short) 0).putLong(segmentTag);
        crc.reset();
        crc.update(output, at, FRAME_HEADER - Integer.BYTES);
        header.putInt((int) crc.getValue());
        used += FRAME_HEADER;
        var body = frame.body().array();
        var length = frame.body().size();
        if (cipher != null) {
            try {
                cipher.init(Cipher.ENCRYPT_MODE, segmentKey, new GCMParameterSpec(TAG_BYTES * 8, nonce(frameOffset)));
                // The cipher keeps the additional data, so the header may leave the output before the body is sealed
                cipher.updateAAD(output, at, FRAME_HEADER);
                for (var offset = 0; offset < length; ) {
                    var slice = Math.min(SLICE, length - offset);
                    room(slice + TAG_BYTES);
                    used += cipher.update(body, offset, slice, output, used);
                    offset += slice;
                }
                room(2 * TAG_BYTES);
                used += cipher.doFinal(output, used);
            } catch (GeneralSecurityException exception) {
                throw new IOException("A log frame could not be sealed", exception);
            }
            return;
        }
        // Without encryption the CRC covers the header and the body
        crc.reset();
        crc.update(output, at, FRAME_HEADER);
        for (var offset = 0; offset < length; ) {
            var slice = Math.min(SLICE, length - offset);
            room(slice);
            System.arraycopy(body, offset, output, used, slice);
            crc.update(body, offset, slice);
            used += slice;
            offset += slice;
        }
        room(Integer.BYTES);
        ByteBuffer.wrap(output, used, Integer.BYTES).putInt((int) crc.getValue());
        used += Integer.BYTES;
    }

    /// {@return the force of the file of large values, started on a thread of its own}
    private FutureTask<Void> forceUnits() {
        var file = blobs;
        var task = new FutureTask<Void>(() -> {
            file.force();
            return null;
        });
        Thread.ofVirtual().name("ledger-blob-force").start(task);
        return task;
    }

    private static void await(FutureTask<Void> task) throws IOException {
        try {
            task.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while forcing the file of large values", exception);
        } catch (ExecutionException exception) {
            throw exception.getCause() instanceof IOException failure ? failure
                    : new IOException("The file of large values could not be forced", exception.getCause());
        }
    }

    /// Writes the output out if `bytes` more would not fit in it.
    private void room(int bytes) throws IOException {
        if (used + bytes > output.length) {
            drain();
        }
    }

    /// Writes the bytes of the output.
    ///
    /// An interrupt that closes the channel meanwhile has the bytes written again, the same bytes at the same offsets.
    private void drain() throws IOException {
        var start = position;
        while (true) {
            try {
                var buffer = ByteBuffer.wrap(output, 0, used);
                while (buffer.hasRemaining()) {
                    position += channel.write(buffer, position);
                }
                used = 0;
                return;
            } catch (ClosedByInterruptException _) {
                position = start;
                reopen();
            }
        }
    }

    /// A frame waiting in the queue.
    ///
    /// @param type the frame type
    /// @param sequence the sequence it carries
    /// @param body its body, which the writer owns
    private record Queued(byte type, long sequence, FrameBody body) {
    }

    /// The frames of consecutive commits, which [#appendAll(Batch)] queues together; the publisher of a store reuses
    /// one across its passes.
    static final class Batch {
        private final ArrayList<Queued> frames;
        // The bytes of the bodies, which count toward the queue's limit
        private long bodyBytes;
        // The bytes that count toward appendedBytes: headers, bodies and units
        private long appendedBytes;
        // Whether a commit of the batch wrote a unit of the file of large values
        private boolean units;

        Batch() {
            this.frames = new ArrayList<>();
        }

        /// Adds a frame, which the writer owns once queued, for a commit that wrote `unitBytes` of units.
        void add(byte type, long sequence, FrameBody body, long unitBytes) {
            frames.add(new Queued(type, sequence, body));
            bodyBytes += body.size();
            appendedBytes += 13 + body.size() + unitBytes;
            units |= unitBytes > 0;
        }

        /// Forgets every frame.
        void clear() {
            frames.clear();
            bodyBytes = 0;
            appendedBytes = 0;
            units = false;
        }
    }

    /// {@return the GCM nonce of the frame at `offset` of its segment}
    static byte[] nonce(long offset) {
        return ByteBuffer.allocate(12).putInt(0).putLong(offset).array();
    }

    private void publish(long sequence, long frames) {
        durable = sequence;
        durabilityLock.lock();
        try {
            written = frames;
            durabilityChanged.signalAll();
        } finally {
            durabilityLock.unlock();
        }
        wake(sequence);
    }

    /// Wakes the commits waiting for a sequence at or below `sequence`.
    private void wake(long sequence) {
        for (var iterator = waiters.iterator(); iterator.hasNext(); ) {
            var waiter = iterator.next();
            if (waiter.sequence <= sequence) {
                waiter.woken = true;
                iterator.remove();
                LockSupport.unpark(waiter.thread);
            }
        }
    }

    private void stop(IOException cause) {
        failure = new LedgerIoException("The log could not be written: commits since the last force may be lost", cause);
        durabilityLock.lock();
        try {
            durabilityChanged.signalAll();
        } finally {
            durabilityLock.unlock();
        }
        wake(Long.MAX_VALUE);
        queueLock.lock();
        try {
            queueDrained.signalAll();
        } finally {
            queueLock.unlock();
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
        await(sequence, -1);
    }

    /// Waits until the commit at `sequence` is durable or `timeout` elapses.
    ///
    /// @return whether it is durable
    /// @throws InterruptedException if interrupted while waiting
    /// @throws LedgerIoException if the writer stopped before it was
    boolean awaitDurable(long sequence, Duration timeout) throws InterruptedException {
        return await(sequence, Math.max(0, timeout.toNanos()));
    }

    /// Parks the caller until the commit at `sequence` is durable, or `timeout` nanoseconds pass if it is not negative.
    ///
    /// A force wakes exactly the commits it made durable, each of which only reads [#durable]: no lock is handed from
    /// one waiter to the next, which with hundreds of waiters would cost more than the force.
    private boolean await(long sequence, long timeout) throws InterruptedException {
        if (durable >= sequence) {
            return true;
        }
        checkHealthy();
        waited = true;
        if (timeout < 0 && !Thread.currentThread().isInterrupted()) {
            // Writes the group itself if no flush is writing: a wait with a timeout, which a slow force would outlast,
            // leaves it to the pool
            flush(sequence);
            if (durable >= sequence) {
                return true;
            }
        }
        var waiter = new Waiter(Thread.currentThread(), sequence);
        waiters.add(waiter);
        try {
            var deadline = timeout < 0 ? 0 : System.nanoTime() + timeout;
            while (durable < sequence) {
                checkHealthy();
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (timeout < 0) {
                    LockSupport.park(this);
                } else {
                    var remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    LockSupport.parkNanos(this, remaining);
                }
            }
            return true;
        } finally {
            if (!waiter.woken) {
                waiters.remove(waiter);
            }
        }
    }

    /// A commit waiting for durability.
    private static final class Waiter {
        final Thread thread;
        final long sequence;
        volatile boolean woken;

        Waiter(Thread thread, long sequence) {
            this.thread = thread;
            this.sequence = sequence;
        }
    }

    /// {@return the bytes of a segment}
    long segmentSize() {
        return segmentSize;
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

    /// {@return the offset in the current segment where the frames of the last group start, a block boundary}
    long groupStart() {
        return groupStart;
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
        // A preparation that has not started never runs; one that has is waited for, so no file of the store changes
        // once it is closed
        if (preparation == null || nextClaimed.compareAndSet(false, true)) {
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

    /// Forces the segment to stable storage, emptying the drive's cache on macOS when it can.
    ///
    /// An interrupt that closes the channel meanwhile has the segment forced again through a new one: a force makes the
    /// file durable, whichever channel wrote it.
    private void force() throws IOException {
        while (true) {
            try {
                if (fullSync != null) {
                    fullSync.force(channel, false);
                } else {
                    channel.force(false);
                }
                return;
            } catch (ClosedByInterruptException _) {
                reopen();
            }
        }
    }

    private void closeSegment() throws IOException {
        if (fullSync != null) {
            fullSync.close();
            fullSync = null;
        }
        if (channel != null) {
            channel.close();
        }
    }

    private void closeChannel() {
        // No flush runs: everything queued is durable, or the writer stopped
        try {
            closeSegment();
        } catch (IOException _) {
            // The data is durable already; nothing to recover from a failed close
        }
    }

}

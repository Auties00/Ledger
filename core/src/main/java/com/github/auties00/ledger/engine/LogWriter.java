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
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

/// Appends frames to the log and makes them durable in groups.
///
/// Commits queue their frames in order, holding the store's publishing role. One flush task at a time, on the store's
/// [com.github.auties00.ledger.IoPool], takes everything queued, seals the frames and writes them with one call, so a
/// crash of the process keeps them. It then forces the segment only if something needs the frames on stable storage: a
/// commit waiting for one of them, a wait for a frame, a close, a commit that writes its group itself, frames whose
/// commits wrote to the file of large values, a rollover, or [#LAZY_BYTES] bytes not forced. Otherwise the frames wait
/// for one of those, for at most the delay the store was opened with, counted from the first of them: a flush that
/// stays busy forces them itself once the delay is over, and with no flush running a timer has a thread of the pool do
/// it, as soon as the pool has one free. A force wakes the commits waiting for any of them: commits that arrive while a
/// force runs make the next group, so concurrent commits share forces.
///
/// A force first pads the frames with zeros to a multiple of [#ALIGNMENT], where the next frame starts and takes the
/// flag of the first frame after a force, so the pages of acknowledged frames are never written again and such a frame
/// always starts a block. Frames written without a force are followed at once by the next ones, without the flag, and
/// recovery reads them as one group: a group is everything written between two forces, however many passes wrote it. A
/// loss of power may tear any of it, none of which anyone was told is durable.
///
/// A flush tells apart what it wrote from what a force made durable. A commit that comes to wait for a frame already
/// written adds itself to the commits waiting, then reads [#writtenSequence]; a flush publishes that, then looks for
/// the commits waiting. One of the two sees the other, so no commit waits out the delay.
///
/// Segments come from [LogFiles] prepared: once a force makes the first frames of a segment durable, or a part of it
/// ([#PREPARE_DIVISOR]) is written with no force, the next one is prepared on a thread of its own, so a rollover only
/// forces the full segment and switches to the next, and no file is created or renamed on the way to a force. The first
/// one is prepared the same way while the store opens, and the first frames wait for it: forcing a new file and its
/// directory took most of an open on Windows and macOS. A prepared file may hold the frames of an earlier use, which
/// every frame header tells apart by the tag of its segment's salt.
///
/// A close keeps the segment prepared after the current one, cut to its first two blocks, and the next open writes
/// into it, as it does into any segment recovery finds prepared and never written: the first durable commit after an
/// open then waits for one round trip to the device or one force, where preparing a segment cost two more before it.
/// The close marks the segment ([#KEPT_MARK]) as one whose name a force made durable, so a clean open forces nothing
/// for it; an open that finds a segment no close marked forces its name before a commit depends on it.
///
/// A new file is written with zeros first only while the commits wait for the forces and the log fills slower than
/// [#ZEROED_RATE], or has not filled a segment since the store opened but commits have waited for [#ZEROS_AFTER] groups:
/// the zeros halve the time of a force on Linux, but double the bytes the device takes, which slowed an ingest of a
/// history, whose commits do not wait, and 256 committers writing 100 MB a second, by a fifth. On Windows, where they
/// spare a force nothing, only while the log writes groups through as well ([LogFiles#zeroFills(boolean)]).
///
/// Where [WriteThrough] can, a group a commit waits for that one write holds and that lands below the bytes its segment
/// already has is written through to stable storage instead, durable once the write returns, and not forced: one round
/// trip to the device where a write and a force take two. The first such group opens the segment a second time for it,
/// so a segment with none costs nothing. A group no commit waits for stays in the cache, costing the device nothing,
/// and the write through of the next group a commit does wait for takes it along while the output holds it. A group of
/// several writes, one that rolls over, or one that would grow the file is written into the cache and forced as before,
/// so an ingest keeps the bandwidth of the cache, and a file system that refuses the writes through moves the writer to
/// forces.
///
/// A store just opened has such bytes only in the first block of its first segment, which its first group takes when
/// one block holds it. The next segment is prepared without zeros, and written with them on a thread of its own once
/// commits have waited for [#ZEROS_AFTER] groups: on Windows a force beside the zeros takes several times as long,
/// which the few commits of a store that opens, writes and closes would pay and never gain from. The log moves to it between two groups as soon as it is ready, leaving the rest of the segment it was writing
/// unused; until then each group is forced. So a run of commits reaches the path within the time of one preparation,
/// whatever the size of a segment.
///
/// A segment is a file of [#SEGMENT_HEADER] bytes of header followed by frames. Each use of it, the writing of one
/// open, draws a random tag, and the key of the use derives from the segment's number, a random salt of its own and
/// that tag: the offset of a frame is a unique nonce under its key, even where an open writes into a segment that an
/// earlier one began to write and crashed before any of it was durable, whose bytes a copy-on-write file system may
/// keep, and even if a segment number were ever used twice. The first group of a use is everything it writes before
/// its first force. A use writes past it only once that force made it durable, after which no open writes into the
/// segment again, so at most one use of a segment has frames past its first group; of the first group of an earlier
/// use, a loss of power leaves frames with no flag, the flag being on its first frame, which the later use writes over:
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
///                 16  frame type, flags (1: first frame after a completed force, at a multiple of the alignment)
///                 20  tag of the segment: the first 8 bytes of its salt
///                 28  tag of the use: 8 random bytes the writer drew when it began writing the segment
///                 36  CRC-32C of bytes 0 to 35
/// sealed body     AES-256-GCM of the body under the key of the use, the frame's offset as nonce and the header as
///                 associated data; without encryption, the body and the CRC-32C of header and body
/// ```
///
/// The commit frames of a group whose sequences follow on and whose bodies hold at most [#RUN_MAX_BODY] bytes are
/// written as runs, frames of type [FrameBody#COMMITS] holding up to [#RUN_BYTES] bytes of their bodies and carrying the
/// sequence of the first: a GCM message costs a key schedule, a check that its key and nonce differ from the last
/// ones, and the hashing of its header whatever its length, which for the frame of a commit of one small write was most
/// of the time a flush took, and a message below 512 bytes never reaches the vectorized GCM. A frame of at most
/// [#RUN_MAX_BODY] bytes, a run or not, is copied into the output whole, then sealed in place in one call. A run of
/// one commit is written as the frame of that commit. Runs never cross a group or a segment, so a group starts, ends
/// and rolls over at the frames it would one frame per commit; a group torn by a crash drops its runs whole, commits
/// of a group never acknowledged durable.
///
/// A failed write or force stops the writer for good: [#awaitDurable(long)] and every later [#append] throw. A failed
/// write through only moves the writer to forces.
final class LogWriter {
    static final int SEGMENT_HEADER = 4_096;
    static final int ALIGNMENT = 4_096;
    static final int FRAME_HEADER = 40;
    static final int MAGIC = 0x4C474631;
    static final byte GROUP_START = 1;
    static final long SEGMENT_MAGIC = 0x4C475345474D4E54L;
    /// The version of the segments a writer starts: from 3 a segment may hold frames of type [FrameBody#COMMITS], so a
    /// build that reads only earlier versions refuses it by its version and not at the first such frame.
    static final int SEGMENT_VERSION = 3;
    /// The oldest version recovery reads, whose frame types the current version all has.
    static final int OLDEST_SEGMENT_VERSION = 2;
    static final int TAG_BYTES = 16;
    static final int SALT_OFFSET = 40;
    static final int SALT_BYTES = 16;
    static final int HEADER_CRC_OFFSET = 56;
    /// Where a frame header holds the tag of its segment.
    static final int SEGMENT_TAG_OFFSET = 20;
    /// Where a frame header holds the tag of the use of the segment that wrote it.
    static final int USE_TAG_OFFSET = 28;
    /// The length of a segment a close keeps for the next open: its header and a first frame block of zeros.
    static final long KEPT_LENGTH = SEGMENT_HEADER + ALIGNMENT;
    /// The offset of the byte a close sets in the segment it keeps once a force made the segment's name durable, the
    /// last of the first frame block. The write follows the force, so a segment that holds the byte needs no force of
    /// its name at the next open, and one that lost it to a loss of power only takes that force again. The first group
    /// of the segment overwrites it, with frames or the padding after them, and no frame or group starts there.
    static final long KEPT_MARK = KEPT_LENGTH - 1;
    /// What the byte at [#KEPT_MARK] holds in a segment a close kept.
    static final byte KEPT_MARK_VALUE = 1;
    private static final SecureRandom TAGS = new SecureRandom();
    /// The largest size of the buffer frames are sealed into and written from.
    private static final int OUTPUT = 1 << 20;
    /// The size the buffer starts at, and goes back to once groups no longer need more. A buffer of [#OUTPUT] bytes
    /// from the start cost every store a megabyte, and as much again of the heap region it took whole, which a store
    /// of small commits never used.
    private static final int MIN_OUTPUT = 16 << 10;
    /// The groups in a row, each fitting in half of [#MIN_OUTPUT], after which a larger buffer goes back to it.
    private static final int SHRINK_AFTER = 256;
    /// The bytes of a frame body sealed in one call, a multiple of the AES block.
    private static final int SLICE = 64 << 10;
    /// The largest commit body a run takes, and the largest frame body sealed in place; a larger one is a frame of its
    /// own, whose header and tag weigh nothing beside it.
    static final int RUN_MAX_BODY = 16 << 10;
    /// The most bytes of entries a run holds, which the output takes whole before they are sealed in one call: past
    /// it, a GCM setup costs a few hundredths of the time of the bytes it seals.
    static final int RUN_BYTES = 64 << 10;
    /// The bytes of frame bodies the queue holds before commits wait for the device.
    private static final long QUEUE_LIMIT = 64L << 20;
    /// The bytes of frames written and not forced at which a flush forces them whatever the delay: what the output holds
    /// at most, past which no write through can take them along. A load of groups this large has a force of each saved
    /// nothing by waiting: left for the rollover, the 16 MiB of a segment took a history of 1,000 messages a batch a tenth
    /// longer on Windows.
    private static final long LAZY_BYTES = 1L << 20;
    /// The part of a segment, as a divisor of its size, written with no force at which the next one starts to be
    /// prepared. Not at the first frames, whose force may be about to run: a preparation forces a file and its directory
    /// beside it, which on ext4 commit the same journal. A log filling at full speed, which forces at its rollovers only,
    /// still has most of the time the segment takes to fill.
    private static final int PREPARE_DIVISOR = 4;
    /// The heap a write takes in memory beside its bytes until a checkpoint takes it, as [#appendedHeap()] estimates
    /// it: its chain, its version, its slot of the hash index and its key's copy, about 110 bytes, and a share of its
    /// batch's array.
    static final int WRITE_HEAP = 128;
    /// The heap a commit takes beside its writes, as [#appendedHeap()] estimates it: the read-only view of the array
    /// its values are kept on and that view's session, 72 bytes, and the array's header. Without it the estimate was
    /// six tenths of what commits of one small write held, so the half of the heap the checkpoint backlog may take
    /// held four fifths.
    static final int COMMIT_HEAP = 128;
    /// The bytes a second of log below which a new segment is written with zeros first, if the commits wait for it.
    private static final long ZEROED_RATE = 64L << 20;
    /// The groups commits wait for, since the store opened, before a segment is written with zeros with no segment
    /// filled yet: writing a segment with zeros slows the forces beside it, which a store that opens and commits a few
    /// times would pay for writes through it never reaches.
    static final int ZEROS_AFTER = 64;
    private final LogFiles files;
    private final StoreKeys keys;
    private final byte[] storeId;
    private final long segmentSize;
    private final Executor executor;
    private final long commitWindowNanos;
    // How the flush task opens segments to write groups through; owned by it
    private final WriteThrough writeThrough;
    // How long frames may stay written and not forced, or 0 if every group is forced
    private final long lazyNanos;

    // Owned by the flush task; null until the first group opens the first segment
    private FileChannel channel;
    // The current segment opened to write through, or null until its first group that can be
    private WriteThrough.Handle throughFile;
    // Whether groups of the current segment may be written through, once one fits the bytes it has
    private boolean throughWanted;
    // Whether a write succeeded in the way segments are opened in now; until one does, a failure moves one way down
    private boolean wayProven;
    // The bytes the current segment had when it was prepared, below which a write changes neither its size nor the
    // length of its data
    private long valid;
    // The end of the bytes of the current segment a force or a write through made durable: the frame written here takes
    // the flag of the first frame after a force
    private long forcedPosition;
    // Whether the name of the current segment is durable, by its preparation, a close or a force; until then only a force
    // makes a group durable
    private boolean nameDurable;
    // The file of the current segment, which an interrupted write reopens
    private java.nio.file.Path segmentPath;
    // Whether an interrupt closed the channel or cut a wait short during the flush, which then gives the interrupt
    // back to its thread
    private boolean interruptedWrite;
    // Whether the flush running is a commit's, for its own group, which one drain writes whole after the bytes the
    // output already holds
    private boolean leading;
    private long segmentNumber;
    // Where the next drain writes
    private long position;
    // Whether the frames written or in the output are padded, after which only a force or a write through may follow
    private boolean padded;
    // Whether the group being sealed goes through, so the output keeps the bytes it holds of those written before it
    private boolean sealingThrough;
    // The force of the file of large values the group being written started, or null
    private FutureTask<Void> unitForce;
    // Whether the frames written since the last force have the time of the first one recorded
    private boolean delaying;
    private SecretKeySpec segmentKey;
    private long segmentTag;
    // The tag of this writer's use of the segment
    private long useTag;
    // The segment close kept for the next open, or -1
    private volatile long kept = -1;
    // The end of the last group's frames, before its padding, for tests
    private volatile long groupEnd;
    // Where the frames of the last group start, for tests
    private volatile long groupStart;
    // The preparation of the segment after the current one, run on a thread of its own, or by whichever of the flush
    // task and a close needs it first
    private FutureTask<LogFiles.Prepared> next;
    // Whether zeros were asked for the segment of next, by its preparation or after it; owned by the flush task
    private boolean nextZeroed;
    // When the current segment opened, and how long the one before it took to fill, or -1; owned by the flush task
    private long segmentOpened;
    private long lastFill = -1;
    // Whether a commit waited for a force since the segment before the current one opened
    private volatile boolean waited;
    private boolean waitedBefore;
    // The last sequence of the group that began to be written last
    private volatile long startedSequence;
    // Whether a commit came to wait for a frame no group had begun to write, or for one that a group had, since the last
    // group began to be written
    private volatile boolean waitedEarly;
    private volatile boolean waitedLate;
    // Whether a commit had come to wait when the last group began to be written, and whether one waited for the group
    // before it; owned by the flush task
    private boolean waitedLastEarly;
    private boolean waitedBeforeLast;
    // The groups written since the store opened that a commit waited for; owned by the flush task
    private long waitedGroups;
    private final Cipher cipher;
    private final CRC32C crc;
    // The buffer frames are sealed into and written from, a slice at a time whatever their size; it grows up to OUTPUT
    // while a group needs more, so a group that fits is written with one call
    private byte[] output;
    // The bytes of output in use: those a drain wrote, then those not written yet
    private int used;
    // The bytes at the start of the output a drain wrote and no force made durable, up to position
    private int retained;
    // The groups in a row written since one needed more than half of MIN_OUTPUT
    private int smallGroups;
    // The emptied queue, which the next swap reuses
    private ArrayList<Queued> flushing;
    // Written by the flush task only
    private volatile long forces;
    // The groups written through, of the forces; written by the flush task only
    private volatile long writtenThrough;
    // The segment the flush task writes, for a checkpoint to read
    private volatile long currentSegment;

    private final ReentrantLock queueLock;
    // Signalled when a group is written, for the commits waiting on a full queue
    private final Condition queueDrained;
    private ArrayList<Queued> queue;
    // The bytes of the frame bodies queued and not written yet
    private long pendingBytes;
    private volatile long lastQueued;
    // Frames queued since the writer opened; a catalog frame carries a sequence already durable, so close() waits on
    // frames rather than on sequences
    private long appended;
    // The bytes of the frames queued since the writer opened, and of the units of BlobFile their commits wrote, written
    // under the queue lock
    private volatile long appendedBytes;
    // The heap the commits queued since the writer opened hold until a checkpoint takes them, as estimated
    private volatile long appendedHeap;
    // Whether a queued frame's commit wrote a unit of BlobFile, which the next group forces; guarded by the queue lock
    private boolean queuedUnits;
    // A frame that waits to be queued before the next one, or until a thread needs it durable; guarded by the queue lock
    private Queued deferred;
    // The number the deferred frame took among the frames queued, once queued; guarded by the queue lock
    private long deferredFrame;
    // Whether close began, after which nothing is queued; guarded by the queue lock
    private boolean closed;
    // Whether close began, so a segment prepared now is not written with zeros it would cut off
    private volatile boolean closing;
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
    // The frames a force made durable since the writer opened; written under the durability lock
    private volatile long durableFrames;
    // The last sequence and the number of the last frame written to the file, forced or not; written holding running
    private volatile long writtenSequence;
    private volatile long writtenFrames;
    // The last sequence a force started on covers; written holding running
    private volatile long forcingSequence;
    // Whether a thread wants what is written forced; set under the queue lock, so a flush that stops either sees it or
    // leaves scheduled for that thread to set
    private volatile boolean forceRequested;
    // The most frames a thread waits to see durable; written under the queue lock
    private volatile long wantedFrames;
    // Whether a delayed force is on its way; at most one is
    private final AtomicBoolean timerArmed;
    // When the first frame not forced was written, as System.nanoTime() tells
    private volatile long unforcedSince;
    private volatile LedgerIoException failure;

    /// Prepares a segment to write frames to.
    ///
    /// @param files the files of the log
    /// @param keys the store's keys
    /// @param storeId the store's id
    /// @param firstSegment the number of the segment to write first: the one to prepare, above every existing one, or
    ///                     the number of `empty`
    /// @param durable the sequence the log already holds durably
    /// @param segmentSize the size past which a new segment starts
    /// @param executor runs the flush task on platform threads
    /// @param commitWindow how long a flush waits for more commits before it starts
    /// @param durabilityDelay how long frames no commit waits for may stay written and not forced, or zero to force
    ///                        every group as soon as it is written
    /// @param empty the segment numbered `firstSegment`, which recovery found prepared and never written, to write
    ///              first; or `null`, to prepare one
    /// @param writeThrough how the segments are opened to write groups through, which the writer owns from now on
    /// @throws IOException if the segment cannot be prepared
    LogWriter(LogFiles files, StoreKeys keys, byte[] storeId, long firstSegment, long durable, long segmentSize,
              Executor executor, Duration commitWindow, Duration durabilityDelay, LogFiles.Prepared empty, WriteThrough writeThrough) throws IOException {
        this.files = files;
        this.keys = keys;
        this.storeId = storeId;
        this.segmentSize = segmentSize;
        this.executor = executor;
        this.commitWindowNanos = commitWindow.toNanos();
        this.writeThrough = writeThrough;
        this.lazyNanos = durabilityDelay.toNanos();
        try {
            this.cipher = keys.encrypted() ? Cipher.getInstance("AES/GCM/NoPadding") : null;
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("AES-GCM is unavailable", exception);
        }
        this.crc = new CRC32C();
        this.output = new byte[MIN_OUTPUT];
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
        this.writtenSequence = durable;
        this.startedSequence = durable;
        this.forcingSequence = durable;
        this.timerArmed = new AtomicBoolean();
        this.lastQueued = durable;
        // The first group switches to it, as a rollover does
        this.segmentNumber = firstSegment - 1;
        this.currentSegment = firstSegment;
        if (empty != null) {
            var ready = new FutureTask<>(() -> empty);
            ready.run();
            this.next = ready;
        } else {
            prepareNext();
        }
    }

    static String segmentName(long number) {
        return LogFiles.segmentName(number);
    }

    /// Starts writing `segment`.
    private void open(LogFiles.Prepared segment) throws IOException {
        var now = System.nanoTime();
        var first = channel == null;
        if (!first) {
            lastFill = now - segmentOpened;
        }
        segmentOpened = now;
        waitedBefore = waited;
        waited = false;
        this.channel = FileChannel.open(segment.path(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.segmentPath = segment.path();
        this.segmentNumber = segment.number();
        this.currentSegment = segment.number();
        this.position = SEGMENT_HEADER;
        // Its preparation forced the header
        this.forcedPosition = SEGMENT_HEADER;
        // A name no force made durable has a force make it so before a group written through depends on it
        this.nameDurable = segment.nameDurable();
        this.valid = segment.valid();
        this.groupEnd = SEGMENT_HEADER;
        this.segmentTag = ByteBuffer.wrap(segment.salt()).getLong();
        this.useTag = TAGS.nextLong();
        this.segmentKey = keys.encrypted() ? keys.segmentKey(storeId, segment.number(), segment.salt(), useTag) : null;
        this.next = null;
        this.throughFile = null;
        // A segment of its header and first block holds at most one group through, which is worth a handle only as the
        // first of a store just opened, which may commit once
        this.throughWanted = valid > KEPT_LENGTH || (first && segment.nameDurable());
    }

    /// {@return the current segment opened to write through, or `null` if [#writeThrough] has no way left}
    ///
    /// A way that refuses the open moves the writer to the next one down; one that refuses the write, as some file
    /// systems refuse the alignment of `O_DIRECT` at the write and not at the open, does so at the group's own
    /// ([#writeThrough()]).
    private WriteThrough.Handle openThrough() {
        while (writeThrough.enabled()) {
            try {
                return writeThrough.open(segmentPath);
            } catch (IOException | RuntimeException | LinkageError _) {
                writeThrough.downgrade();
                wayProven = false;
            }
        }
        return null;
    }

    private static void closeQuietly(WriteThrough.Handle handle) {
        if (handle == null) {
            return;
        }
        try {
            handle.close();
        } catch (IOException _) {
            // Nothing was written through it that a group depends on
        }
    }

    /// Prepares the segment after the current one on a platform thread of its own.
    ///
    /// Not on the store's pool: creating and forcing a file costs tens of milliseconds on Windows, and a flush queued
    /// behind it would make every commit waiting on that flush wait as long.
    private void prepareNext() {
        var number = segmentNumber + 1;
        var zeros = wantsZeros();
        // On the thread of the preparation: the answer of a device may take a round trip to it
        var preparation = new FutureTask<>(() -> files.prepare(number, segmentSize,
                zeros && !closing && files.zeroFills(writeThrough.enabled()), writeThrough.kind() != null, () -> closing));
        next = preparation;
        nextZeroed = zeros;
        Thread.ofPlatform().daemon().name("ledger-prepare").start(preparation);
    }

    /// {@return whether the next segment is written with zeros: commits waited for the forces of a segment that filled
    /// slower than [#ZEROED_RATE], or for [#ZEROS_AFTER] groups with no segment filled yet}
    private boolean wantsZeros() {
        if (!(waited || waitedBefore)) {
            return false;
        }
        return lastFill < 0 ? waitedGroups >= ZEROS_AFTER : lastFill > segmentSize * 1_000_000_000L / ZEROED_RATE;
    }

    /// Writes the segment prepared after the current one with zeros, on a platform thread of its own, once they are
    /// wanted and it was prepared without them.
    ///
    /// The zeros only give the file its blocks, so a preparation they fail to complete still serves as it was.
    private void fillNext() {
        var preparation = next;
        if (nextZeroed || preparation == null || preparation.state() != Future.State.SUCCESS || !wantsZeros()) {
            return;
        }
        nextZeroed = true;
        var prepared = preparation.resultNow();
        if (prepared.valid() >= segmentSize / 2) {
            return;
        }
        var fill = new FutureTask<>(() -> {
            if (closing || !files.zeroFills(writeThrough.enabled())) {
                return prepared;
            }
            try {
                return files.fill(prepared, segmentSize, () -> closing);
            } catch (IOException _) {
                return prepared;
            }
        });
        next = fill;
        Thread.ofPlatform().daemon().name("ledger-prepare").start(fill);
    }

    /// {@return whether, between two groups, the log moves to the prepared segment after the current one: commits wait
    /// for durability, the current segment has no bytes left to write them through and the next has them}
    ///
    /// Everything written to the current segment is durable at that point, so no force goes with the move. Only a next
    /// segment of at least half the full size counts, so a short spare left by an earlier move is never moved to.
    private boolean rollsEarly() {
        if (position != forcedPosition || (throughWanted && position + ALIGNMENT <= valid)
                || !(waited || waitedBefore) || writeThrough.kind() == null) {
            return false;
        }
        var preparation = next;
        return preparation != null && preparation.state() == Future.State.SUCCESS
                && preparation.resultNow().valid() >= segmentSize / 2;
    }

    /// Moves to the prepared segment after the current one, whose time to fill says nothing of the rate of the log.
    private void rollEarly() throws IOException {
        var fill = lastFill;
        closeSegment();
        rollover();
        lastFill = fill;
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
    /// more than [#QUEUE_LIMIT] bytes not written yet, it waits for the device, which holds back every commit behind
    /// it: memory stays bounded when commits come faster than the device takes them. A frame larger than the limit
    /// waits for the queue to empty.
    ///
    /// @throws LedgerIoException if the writer has stopped
    void append(byte type, long sequence, FrameBody body) {
        append(type, sequence, body, 0, 0);
    }

    /// Queues a frame as [#append(byte, long, FrameBody)] does, for a commit of `writes` writes that wrote `unitBytes`
    /// of units of the file of large values.
    ///
    /// The group that holds the frame forces that file beside the segment. The units count toward the growth that
    /// starts a checkpoint, as if their values were in the frame: memory holds them until a checkpoint takes them. The
    /// writes count toward [#appendedHeap()].
    ///
    /// @return the frames queued since the writer opened, the new one included
    /// @throws LedgerIoException if the writer has stopped
    long append(byte type, long sequence, FrameBody body, long unitBytes, int writes) {
        checkHealthy();
        var length = body.size();
        long frames;
        queueLock.lock();
        try {
            while (pendingBytes > 0 && pendingBytes + length > QUEUE_LIMIT && failure == null) {
                queueDrained.awaitUninterruptibly();
            }
            checkHealthy();
            queueDeferred();
            queue.add(new Queued(type, sequence, body));
            pendingBytes += length;
            lastQueued = sequence;
            frames = ++appended;
            appendedBytes += 13 + length + unitBytes;
            appendedHeap += heapOf(length, unitBytes, writes);
            queuedUnits |= unitBytes > 0;
        } finally {
            queueLock.unlock();
        }
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
        return frames;
    }

    /// Queues the frames of `batch`, in its order, as [#append(byte, long, FrameBody, long, int)] queues each, and empties
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
            queueDeferred();
            queue.addAll(batch.frames);
            pendingBytes += batch.bodyBytes;
            lastQueued = batch.frames.getLast().sequence();
            appended += batch.frames.size();
            appendedBytes += batch.appendedBytes;
            appendedHeap += batch.appendedHeap;
            queuedUnits |= batch.units;
        } finally {
            queueLock.unlock();
            batch.clear();
        }
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
    }

    /// Holds a frame of `body` back until the next frame is queued, which follows it into the same group, or until
    /// [#queueDeferred()]: a frame that matters only to the commits after it, so a close with none drops it.
    ///
    /// @param sequence the sequence it carries, which every frame queued after it carries or exceeds
    void defer(byte type, long sequence, FrameBody body) {
        queueLock.lock();
        try {
            deferred = new Queued(type, sequence, body);
        } finally {
            queueLock.unlock();
        }
    }

    /// Queues the frame [#defer] holds back, unless a frame queued since took it along, and starts a flush.
    ///
    /// @return the number of the frame among those queued since the writer opened, for [#awaitFrames(long)]
    /// @throws IllegalStateException if the writer is closed, which dropped the frame
    long flushDeferred() {
        long frames;
        queueLock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("The store is closed");
            }
            queueDeferred();
            frames = deferredFrame;
        } finally {
            queueLock.unlock();
        }
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
        return frames;
    }

    /// Queues the deferred frame, if any; holding the queue lock.
    private void queueDeferred() {
        var frame = deferred;
        if (frame == null) {
            return;
        }
        deferred = null;
        queue.add(frame);
        pendingBytes += frame.body().size();
        lastQueued = frame.sequence();
        deferredFrame = ++appended;
        appendedBytes += 13 + frame.body().size();
        appendedHeap += 13 + frame.body().size();
    }

    /// Makes `blobs` the file of large values the commits write units to, which the groups force.
    void useBlobs(BlobFile blobs) {
        this.blobs = blobs;
    }

    /// Waits until the first `frames` frames queued since the writer opened are durable.
    ///
    /// @throws LedgerIoException if the writer stops first
    void awaitFrames(long frames) {
        if (durableFrames < frames) {
            wantFrames(frames);
        }
        durabilityLock.lock();
        try {
            while (durableFrames < frames) {
                checkHealthy();
                durabilityChanged.awaitUninterruptibly();
            }
        } finally {
            durabilityLock.unlock();
        }
    }

    /// Has flushes force what they write until the first `frames` frames queued are durable.
    private void wantFrames(long frames) {
        queueLock.lock();
        try {
            wantedFrames = Math.max(wantedFrames, frames);
        } finally {
            queueLock.unlock();
        }
        schedule();
    }

    /// Has a flush on the pool force what is written: the thread asking may be interrupted, a thread of the pool is not.
    private void requestForce() {
        markForceRequested();
        schedule();
    }

    private void markForceRequested() {
        queueLock.lock();
        try {
            forceRequested = true;
        } finally {
            queueLock.unlock();
        }
    }

    /// Starts a flush on the pool unless one is due to look at the queue and the requests again.
    private void schedule() {
        if (scheduled.compareAndSet(false, true)) {
            try {
                executor.execute(this::flush);
            } catch (RejectedExecutionException exception) {
                stop(new IOException("The log's pool refused a flush: it was closed with the store open", exception));
            }
        }
    }

    /// Records that the frames written have started to wait for a force, and arms one for the end of the delay, unless
    /// one is on its way, which finds the time they have left when it runs.
    private void delay() {
        if (!delaying) {
            delaying = true;
            unforcedSince = System.nanoTime();
            armTimer(lazyNanos);
        }
    }

    private void armTimer(long nanos) {
        if (timerArmed.compareAndSet(false, true)) {
            CompletableFuture.delayedExecutor(nanos, TimeUnit.NANOSECONDS, executor).execute(this::delayedForce);
        }
    }

    /// Forces the frames that waited out the delay, on a thread of the pool, unless every frame written is durable or
    /// the writer has stopped: past those two, a closed writer has no channel to force.
    ///
    /// For a flush that has stopped: one still running holds this task back, on a pool of one thread, and looks at the
    /// delay itself ([#forceNeeded(long)]).
    private void delayedForce() {
        // Before it reads what is written: a flush that wrote frames and found the timer armed leaves them to this force
        timerArmed.set(false);
        if (writtenFrames <= durableFrames || failure != null) {
            return;
        }
        var left = unforcedSince + lazyNanos - System.nanoTime();
        if (left > 0) {
            armTimer(left);
            return;
        }
        markForceRequested();
        flush();
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

    /// Writes the queued groups, and forces what something needs forced, until the queue is empty, or until the commit
    /// at `until` is durable, for a commit that waits for its group on its own thread rather than for a thread of the
    /// pool to wake, then for its own to be woken.
    ///
    /// One flush writes at a time: another returns at once, and the one that writes looks at the queue and the requests
    /// for a force again once it stops, so the frames of a commit whose flush returned are never left behind, and no
    /// request is. A flush for a commit hands what is queued after its group to the pool.
    private void flush(long until) {
        while (running.compareAndSet(false, true)) {
            try {
                flushGroups(until);
            } finally {
                running.set(false);
            }
            boolean idle;
            queueLock.lock();
            try {
                idle = queue.isEmpty() && !forceRequested && wantedFrames <= durableFrames;
                if (idle) {
                    scheduled.set(false);
                }
            } finally {
                queueLock.unlock();
            }
            if (idle || failure != null) {
                return;
            }
            if (until != Long.MAX_VALUE) {
                try {
                    executor.execute(this::flush);
                } catch (RejectedExecutionException exception) {
                    stop(new IOException("The log's pool refused a flush: it was closed with the store open", exception));
                }
                return;
            }
        }
    }

    /// {@return whether the frame of the commit at `sequence` is queued, no later commit's is, and no other commit
    /// waits for its durability}
    ///
    /// Only such a commit writes its group itself. With other commits under way, the pool's flush goes on to their
    /// group at once, where a commit that wrote its own would hand that group to the pool and wait for a thread of it
    /// to wake; and the wake of the pool's thread lets more commits join a group than a commit forcing its own at once
    /// would, which at eight committers made fewer forces than leading did.
    ///
    /// Only without a commit window, which the pool's flush waits out for the commits that join the group. A virtual
    /// thread leads too: its force lends the carrier's place to another, as the JDK does for a force, and only the write
    /// of the group into the file's cache holds the carrier; a commit of one virtual thread at a time made a fifth more
    /// commits a second where it did. On Windows the write of a group through to stable storage holds the carrier too,
    /// one at a time as only one flush runs, while the other carriers go on.
    private boolean alone(long sequence) {
        return commitWindowNanos == 0 && waiters.isEmpty() && lastQueued == sequence && durable < sequence;
    }

    /// {@return whether the output holds everything written and not forced, with room for its padding, so a force an
    /// interrupt cuts off can write it all again}
    private boolean canRewriteUnforced() {
        return position - retained == forcedPosition && retained + ALIGNMENT <= OUTPUT;
    }

    /// {@return whether `frames` seal into the output and the current segment, so the group is written with one call
    /// that a write cut off by an interrupt repeats, with no rollover to prepare a segment on the caller's thread, and
    /// a force cut off too can write them again along with what was written before them}
    private boolean fitsOneWrite(List<Queued> frames) {
        if (channel == null || !canRewriteUnforced()) {
            return false;
        }
        var bytes = sealedBytes(frames);
        return bytes >= 0 && position + bytes + ALIGNMENT <= segmentSize;
    }

    /// {@return a bound on the bytes `frames` seal into, each counted as a frame of its own, more than the run that may
    /// hold it takes; or -1 if the output cannot hold that many, the bytes it retains and a block of padding}
    private long sealedBytes(List<Queued> frames) {
        var limit = OUTPUT - ALIGNMENT - retained;
        if (limit < 0) {
            return -1;
        }
        var bytes = 0L;
        for (var frame : frames) {
            bytes += FRAME_HEADER + frame.body().size() + sealOverhead();
            if (bytes > limit) {
                return -1;
            }
        }
        return bytes;
    }

    /// {@return whether the output is written through once `bytes` more are sealed into it, or never if -1: it holds
    /// everything since the last force, so one write starts where all before it is durable, and that write ends, padded,
    /// within the bytes the segment had when it was prepared; the first that does opens the segment for it, if a way is
    /// left}
    ///
    /// Within those bytes a write changes neither the size of the file nor the length of its data, so it needs no
    /// metadata written with it: the one write with forced unit access that makes a write through cheaper than a
    /// write and a force.
    private boolean writesThrough(long bytes) {
        if (!throughWanted || !nameDurable || bytes < 0 || position - retained != forcedPosition
                || used + bytes > OUTPUT - ALIGNMENT) {
            return false;
        }
        var last = writeEnd() + bytes;
        var end = (last + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
        if (end > valid || last + ALIGNMENT > segmentSize) {
            return false;
        }
        if (throughFile == null) {
            throughFile = openThrough();
            throughWanted = throughFile != null;
        }
        return throughWanted;
    }

    /// Writes the queued groups, and forces what something needs forced, until the queue is empty and no force is due
    /// or the commit at `until` is durable, holding [#running].
    private void flushGroups(long until) {
        leading = until != Long.MAX_VALUE;
        try {
            // A stopped writer writes nothing more: a group after one whose force failed would pass for durable while
            // the failed one, maybe partly on disk, stands before it
            while (durable < until && failure == null) {
                if (commitWindowNanos > 0) {
                    LockSupport.parkNanos(commitWindowNanos);
                }
                ArrayList<Queued> group = null;
                var sequence = 0L;
                var frames = 0L;
                var units = false;
                queueLock.lock();
                try {
                    if (!queue.isEmpty()) {
                        if (leading && (lastQueued > until || !fitsOneWrite(queue))) {
                            return;
                        }
                        group = queue;
                        sequence = lastQueued;
                        frames = appended;
                        units = queuedUnits;
                        queuedUnits = false;
                        queue = flushing;
                    }
                } finally {
                    queueLock.unlock();
                }
                if (group != null) {
                    var bodies = write(group, sequence, frames, units);
                    group.clear();
                    flushing = group;
                    queueLock.lock();
                    try {
                        pendingBytes -= bodies;
                        queueDrained.signalAll();
                    } finally {
                        queueLock.unlock();
                    }
                    writtenFrames = frames;
                    writtenSequence = sequence;
                }
                if (position == forcedPosition && writtenFrames > durableFrames) {
                    // The group went through
                    madeDurable(writtenSequence, writtenFrames);
                } else if (forceDue()) {
                    forceWritten();
                } else if (group != null) {
                    delay();
                } else {
                    if (position == forcedPosition) {
                        // A request made before this is met: it asked for what was written then
                        forceRequested = false;
                    }
                    return;
                }
            }
        } catch (IOException exception) {
            stop(exception);
        } catch (RuntimeException | Error throwable) {
            stop(new IOException("The log writer failed", throwable));
        } finally {
            leading = false;
            // A commit's thread gets its interrupt back; a thread of the pool, which nothing should interrupt, not
            if (interruptedWrite) {
                interruptedWrite = false;
                if (until != Long.MAX_VALUE) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /// Opens the segment again after an interrupt of the thread writing it closed its channel, or its handle to write
    /// through, with the interrupt cleared until the flush ends, so a commit that writes its own group survives an
    /// interrupt. Only what the interrupt closed is opened again; a handle to write through that cannot be leaves the
    /// segment's groups to forces.
    private void reopen() throws IOException {
        interruptedWrite |= Thread.interrupted();
        if (!channel.isOpen()) {
            channel = FileChannel.open(segmentPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
        if (throughFile != null && !throughFile.isOpen()) {
            try {
                throughFile = writeThrough.open(segmentPath);
            } catch (IOException | RuntimeException | LinkageError _) {
                throughFile = null;
                throughWanted = false;
            }
        }
    }

    /// Seals and writes `group`, whose last sequence is `sequence` and whose last frame is the `frames`-th queued, and
    /// starts the force of the file of large values if `units`. If a commit needs the group durable it is padded, and
    /// written through if [#writesThrough(List)] says so, along with what was written before it and no force made durable;
    /// otherwise it follows those bytes in the cache.
    ///
    /// The commits of the group wrote their units before they queued their frames, so one force of that file, run
    /// beside the segment's write and force, makes them all durable before the group is. A rollover waits for it
    /// before it forces the segment: the first frame of a group in the next segment then means, as it always does,
    /// that everything before it is durable. The frames of the full segment are durable from that force on, whether or
    /// not anything forces those after them.
    ///
    /// @return the bytes of the frame bodies written
    private long write(List<Queued> group, long sequence, long frames, boolean units) throws IOException {
        if (channel == null) {
            rollover();
        } else if (rollsEarly()) {
            rollEarly();
        }
        // The output holds nothing an interrupted force would write again
        if (smallGroups >= SHRINK_AFTER && output.length > MIN_OUTPUT && used == 0) {
            output = new byte[MIN_OUTPUT];
        }
        startedSequence = sequence;
        var waitsNow = waitedEarly;
        var waitedLast = waitedLastEarly || waitedLate;
        waitedEarly = false;
        waitedLate = false;
        if (waitsNow || waitedLast) {
            waitedGroups++;
        }
        unitForce = units ? forceUnits() : null;
        // A thread that waits for each of its commits often loses the race to a flush that is already awake, whose group
        // would pay a write into the cache before its write through: two groups in a row that commits waited for say the
        // next one will be too, where a commit that does not wait between two that do, as a deferred removal before a
        // durable write, says nothing
        var forcing = leading || waitsNow || (waitedLast && waitedBeforeLast) || forceNeeded(sequence);
        waitedBeforeLast = waitedLast;
        waitedLastEarly = waitsNow;
        // Before a frame is sealed, while the output holds no byte of the group
        var through = forcing && writesThrough(sealedBytes(group));
        sealingThrough = through;
        var start = position;
        var groupStart = position == forcedPosition;
        var bodies = 0L;
        var sealedSequence = writtenSequence;
        var sealedFrames = frames - group.size();
        for (var frame : group) {
            bodies += frame.body().size();
        }
        var count = group.size();
        var rolled = false;
        for (var index = 0; index < count; ) {
            var frame = group.get(index);
            var bodyLength = frame.body().size();
            var sealedLength = bodyLength + sealOverhead();
            // The test of the frame alone: a run takes the commits after its first only while it fits in the segment
            if (writeEnd() + FRAME_HEADER + sealedLength > segmentSize && writeEnd() > SEGMENT_HEADER) {
                drain();
                if (unitForce != null) {
                    await(unitForce);
                    unitForce = null;
                }
                force();
                forces++;
                publish(sealedSequence, sealedFrames);
                closeSegment();
                rollover();
                start = position;
                groupStart = true;
                rolled = true;
            }
            if (bodyLength > RUN_MAX_BODY) {
                seal(frame, sealedLength, groupStart);
                index++;
                sealedFrames++;
            } else {
                var end = runEnd(group, index);
                sealRun(group, index, end, groupStart);
                sealedFrames += end - index;
                index = end;
            }
            sealedSequence = group.get(index - 1).sequence();
            groupStart = false;
            // Not beside a force that is about to run, which prepares it once it ends
            if (next == null && writeEnd() - SEGMENT_HEADER >= segmentSize / PREPARE_DIVISOR && !forcing && !forceNeeded(sequence)) {
                prepareNext();
            }
        }
        var end = writeEnd();
        // In one write with the frames when the force is known already, a commit that came to wait since then costing a
        // write of its own
        if (forcing || forceNeeded(sequence)) {
            forcingSequence = sequence;
            // A commit that came to wait while the group was sealed; a group that rolled over stays forced
            through = through || (!rolled && writesThrough(0));
            sealingThrough = through;
            pad();
        }
        smallGroups = writeEnd() - start <= MIN_OUTPUT / 2 ? smallGroups + 1 : 0;
        if (through) {
            writeThrough();
        } else {
            drain();
        }
        sealingThrough = false;
        this.groupStart = start;
        groupEnd = end;
        return bodies;
    }

    /// {@return where the bytes in the output not written yet end}
    private long writeEnd() {
        return position + used - retained;
    }

    /// Adds zeros up to the next block to the output, where the frame after a force starts: a reused file holds older
    /// bytes there. Only a force or a write through may follow them.
    private void pad() throws IOException {
        var end = writeEnd();
        var padding = (int) ((end + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT - end);
        room(padding);
        Arrays.fill(output, used, used + padding, (byte) 0);
        used += padding;
        padded = true;
    }

    /// {@return whether something needs the frames up to `sequence`, or the ones written before them, forced now}
    ///
    /// The frames of commits that wrote units are, so that a commit whose units the file lacks is one no one was told
    /// of. Frames past the delay are, whatever else this flush has to write: commits that keep coming keep it running,
    /// and on a pool of one thread the delayed force waits for it to stop.
    private boolean forceNeeded(long sequence) {
        return lazyNanos == 0
                || closing
                || forceRequested
                || unitForce != null
                || writeEnd() - forcedPosition >= LAZY_BYTES
                || wantedFrames > durableFrames
                || waitedFor(sequence)
                || (delaying && System.nanoTime() - unforcedSince >= lazyNanos);
    }

    /// {@return whether a commit at or below `sequence` not durable yet waits}
    private boolean waitedFor(long sequence) {
        for (var waiter : waiters) {
            if (waiter.sequence <= sequence && waiter.sequence > durable) {
                return true;
            }
        }
        return false;
    }

    /// {@return whether this flush forces what is written now}
    ///
    /// Read after the flush publishes what it wrote, so a commit that came to wait while it wrote is found, or finds
    /// the frames written itself and requests the force.
    private boolean forceDue() {
        if (padded) {
            return true;
        }
        if (position == forcedPosition) {
            return false;
        }
        return leading ? canRewriteUnforced() : forceNeeded(writtenSequence);
    }

    /// Pads and forces what is written, the file of large values with it if the group started that force, and publishes
    /// it as durable. For a commit that waits, what the output still holds of it goes through rather than being forced,
    /// where it can.
    private void forceWritten() throws IOException {
        var sequence = writtenSequence;
        var frames = writtenFrames;
        forcingSequence = sequence;
        forceRequested = false;
        if (padded) {
            force();
        } else {
            sealingThrough = (leading || !waiters.isEmpty()) && writesThrough(0);
            pad();
            if (sealingThrough) {
                sealingThrough = false;
                writeThrough();
            } else {
                drain();
                force();
            }
        }
        madeDurable(sequence, frames);
    }

    /// Finishes a force or a write through of everything written: waits for the force of the file of large values the
    /// group started, has the next segment prepared and publishes the frames as durable.
    private void madeDurable(long sequence, long frames) throws IOException {
        forces++;
        if (unitForce != null) {
            await(unitForce);
            unitForce = null;
        }
        if (next == null) {
            prepareNext();
        } else {
            fillNext();
        }
        publish(sequence, frames);
    }

    /// {@return the bytes sealing adds to a frame body: the GCM tag, or the CRC-32C without encryption}
    private int sealOverhead() {
        return keys.encrypted() ? TAG_BYTES : Integer.BYTES;
    }

    /// {@return the end, exclusive, of the frames of `group` from `first` on that one frame holds: the commit frames
    /// whose sequences follow on from the one at `first`, each of at most [#RUN_MAX_BODY] bytes, while their entries
    /// hold at most [#RUN_BYTES] bytes and the frame fits in the segment; only the frame at `first` if none joins it}
    private int runEnd(List<Queued> group, int first) {
        var frame = group.get(first);
        if (frame.type() != FrameBody.COMMIT) {
            return first + 1;
        }
        var limit = Math.min(RUN_BYTES, segmentSize - writeEnd() - FRAME_HEADER - sealOverhead());
        var entries = (long) FrameBody.runEntryBytes(frame.body().size());
        var sequence = frame.sequence();
        var end = first + 1;
        for (var count = group.size(); end < count; end++) {
            var next = group.get(end);
            var length = next.body().size();
            if (next.type() != FrameBody.COMMIT || next.sequence() != sequence + 1 || length > RUN_MAX_BODY) {
                break;
            }
            var entry = FrameBody.runEntryBytes(length);
            if (entries + entry > limit) {
                break;
            }
            entries += entry;
            sequence++;
        }
        return end;
    }

    /// Seals the frames of `group` from `first` to `end`, exclusive, as one frame: a run of commits, or the frame at
    /// `first` alone.
    ///
    /// The frame takes its place in the output whole before its body is sealed there, in place: the JDK's GCM
    /// encrypts an array onto itself at the same offset without a copy, where an output offset past the input's makes
    /// it copy the whole array first.
    private void sealRun(List<Queued> group, int first, int end, boolean groupStart) throws IOException {
        var frame = group.get(first);
        var single = end - first == 1;
        var plainLength = 0;
        if (single) {
            plainLength = frame.body().size();
        } else {
            for (var index = first; index < end; index++) {
                plainLength += FrameBody.runEntryBytes(group.get(index).body().size());
            }
        }
        var sealedLength = plainLength + sealOverhead();
        room(FRAME_HEADER + sealedLength);
        var at = used;
        var frameOffset = writeEnd();
        writeHeader(at, sealedLength, frame.sequence(), single ? frame.type() : FrameBody.COMMITS, groupStart);
        var bodyAt = at + FRAME_HEADER;
        if (single) {
            System.arraycopy(frame.body().array(), 0, output, bodyAt, plainLength);
        } else {
            var offset = bodyAt;
            for (var index = first; index < end; index++) {
                offset = group.get(index).body().writeRunEntry(output, offset);
            }
        }
        if (cipher != null) {
            try {
                cipher.init(Cipher.ENCRYPT_MODE, segmentKey, new GCMParameterSpec(TAG_BYTES * 8, nonce(frameOffset)));
                cipher.updateAAD(output, at, FRAME_HEADER);
                cipher.doFinal(output, bodyAt, plainLength, output, bodyAt);
            } catch (GeneralSecurityException exception) {
                throw new IOException("A log frame could not be sealed", exception);
            }
        } else {
            crc.reset();
            crc.update(output, at, FRAME_HEADER + plainLength);
            Bytes.i32(output, bodyAt + plainLength, (int) crc.getValue());
        }
        used = bodyAt + sealedLength;
    }

    /// Writes the header of a frame at `at` of the output.
    private void writeHeader(int at, int sealedLength, long sequence, byte type, boolean groupStart) {
        Bytes.i32(output, at, MAGIC);
        Bytes.i32(output, at + 4, sealedLength);
        Bytes.i64(output, at + 8, sequence);
        output[at + 16] = type;
        output[at + 17] = groupStart ? GROUP_START : 0;
        Bytes.u16(output, at + 18, 0);
        Bytes.i64(output, at + SEGMENT_TAG_OFFSET, segmentTag);
        Bytes.i64(output, at + USE_TAG_OFFSET, useTag);
        crc.reset();
        crc.update(output, at, FRAME_HEADER - Integer.BYTES);
        Bytes.i32(output, at + FRAME_HEADER - Integer.BYTES, (int) crc.getValue());
    }

    /// Seals `frame` into the output, writing the output out whenever it fills.
    ///
    /// The body goes to the cipher a slice at a time: the frame is the one GCM message a single call would make, and
    /// the output stays at most [#OUTPUT] bytes whatever the frame's size.
    private void seal(Queued frame, int sealedLength, boolean groupStart) throws IOException {
        room(FRAME_HEADER);
        var at = used;
        var frameOffset = writeEnd();
        writeHeader(at, sealedLength, frame.sequence(), frame.type(), groupStart);
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

    ///
    /// An interrupt of a commit that writes its own group waits on, and is given back to the commit when the flush ends.
    private void await(FutureTask<Void> task) throws IOException {
        while (true) {
            try {
                task.get();
                return;
            } catch (InterruptedException exception) {
                if (!leading) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while forcing the file of large values", exception);
                }
                interruptedWrite = true;
            } catch (ExecutionException exception) {
                throw exception.getCause() instanceof IOException failure ? failure
                        : new IOException("The file of large values could not be forced", exception.getCause());
            }
        }
    }

    /// Makes room for `bytes` more in the output: gives up the bytes written already unless a commit forcing its own
    /// group may need them, then grows it while it is smaller than [#OUTPUT], else writes it out.
    ///
    /// The bytes written are worth no more memory than the output has: only a commit forcing its own group or a group
    /// going through needs them, and without them the group is forced, by the pool if a commit was to do it.
    private void room(int bytes) throws IOException {
        if (used + bytes <= output.length) {
            return;
        }
        if (retained > 0 && !leading && !sealingThrough) {
            System.arraycopy(output, retained, output, 0, used - retained);
            used -= retained;
            retained = 0;
            if (used + bytes <= output.length) {
                return;
            }
        }
        if (output.length < OUTPUT) {
            var grown = Math.min(OUTPUT, Math.max(2 * output.length, used + bytes));
            // The bytes not written yet stay where they are
            output = Arrays.copyOf(output, grown);
            if (used + bytes <= grown) {
                return;
            }
        }
        drain();
        used = 0;
        retained = 0;
    }

    /// Writes the bytes of the output not written yet, which it keeps until a force or [#room(int)] gives them up.
    ///
    /// An interrupt that closes the channel meanwhile has the bytes written again, the same bytes at the same offsets.
    private void drain() throws IOException {
        var start = position;
        while (true) {
            try {
                var buffer = ByteBuffer.wrap(output, retained, used - retained);
                while (buffer.hasRemaining()) {
                    position += channel.write(buffer, position);
                }
                retained = used;
                return;
            } catch (ClosedByInterruptException _) {
                position = start;
                reopen();
            }
        }
    }

    /// Writes the whole output, a group padded to a block after whatever was written since the last force, through to
    /// stable storage: durable once this returns, as a force would make them.
    ///
    /// The bytes written before go with it, over the same bytes in the cache, because a write through makes durable only
    /// what it writes. A cache holding those pages dirty has the kernel write them first, so such a write costs what a
    /// force does, but it still does not wait behind the pages of a checkpoint in the drive's cache.
    ///
    /// An interrupt that closes the handle meanwhile has the bytes written again through a new one, the same bytes at
    /// the same offsets, as [#drain()] does. A write through that fails otherwise, as one a file system refuses the
    /// alignment of does, moves the writer to forces: the group is written again into the cache and forced, which
    /// makes each of its pages dirty again whatever the failed write left on the device, so a force that succeeds
    /// makes the group durable, and one that fails stops the writer as it always does. Until a write has succeeded in
    /// a way, a failure moves the writer one way down instead, and the next way writes the same bytes at the same
    /// offsets.
    private void writeThrough() throws IOException {
        var start = forcedPosition;
        while (throughFile != null) {
            try {
                throughFile.write(output, used, output.length, start);
                position = start + used;
                used = 0;
                retained = 0;
                padded = false;
                delaying = false;
                forcedPosition = position;
                writtenThrough++;
                wayProven = true;
                return;
            } catch (ClosedByInterruptException _) {
                reopen();
            } catch (IOException _) {
                if (wayProven) {
                    break;
                }
                writeThrough.downgrade();
                closeQuietly(throughFile);
                throughFile = openThrough();
            }
        }
        writeThrough.disable();
        closeQuietly(throughFile);
        throughFile = null;
        throughWanted = false;
        position = start;
        retained = 0;
        drain();
        force();
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
        // The heap that counts toward appendedHeap
        private long appendedHeap;
        // Whether a commit of the batch wrote a unit of the file of large values
        private boolean units;

        Batch() {
            this.frames = new ArrayList<>();
        }

        /// Adds a frame, which the writer owns once queued, for a commit of `writes` writes that wrote `unitBytes` of
        /// units.
        void add(byte type, long sequence, FrameBody body, long unitBytes, int writes) {
            frames.add(new Queued(type, sequence, body));
            bodyBytes += body.size();
            appendedBytes += 13 + body.size() + unitBytes;
            appendedHeap += heapOf(body.size(), unitBytes, writes);
            units |= unitBytes > 0;
        }

        /// Forgets every frame.
        void clear() {
            frames.clear();
            bodyBytes = 0;
            appendedBytes = 0;
            appendedHeap = 0;
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
            durableFrames = frames;
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
    /// Asking does not hurry it: with no thread waiting it may stay false for the delay the store was opened with.
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
    /// one waiter to the next, which with hundreds of waiters would cost more than the force. A commit written already
    /// and left unforced that cannot force it itself requests the force of a flush on the pool, with or without a
    /// timeout, which the delay might outlast.
    private boolean await(long sequence, long timeout) throws InterruptedException {
        if (durable >= sequence) {
            return true;
        }
        checkHealthy();
        waited = true;
        if (sequence > startedSequence) {
            waitedEarly = true;
        } else {
            waitedLate = true;
        }
        if (timeout < 0 && !Thread.currentThread().isInterrupted() && alone(sequence)) {
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
            // After it is in the list: a flush that published these frames before it was finds them here, and one that
            // publishes them after finds it. A force started after they were written makes them durable and wakes it
            if (writtenSequence >= sequence && forcingSequence < sequence && durable < sequence) {
                requestForce();
            }
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

    /// {@return the heap the commits queued since the writer opened hold until a checkpoint takes them, estimated: the
    /// bytes of their frames and units, [#WRITE_HEAP] a write and [#COMMIT_HEAP] a commit}
    long appendedHeap() {
        return appendedHeap;
    }

    /// {@return the heap a commit of `writes` writes, whose frame body is `bodyBytes` and whose units are `unitBytes`,
    /// holds until a checkpoint takes it, as [#appendedHeap()] estimates it}
    static long heapOf(long bodyBytes, long unitBytes, int writes) {
        return 13 + bodyBytes + unitBytes + (long) writes * WRITE_HEAP + (writes > 0 ? COMMIT_HEAP : 0);
    }

    /// {@return the offset in the current segment where the frames of the last pass of a flush end, before any padding}
    long groupEnd() {
        return groupEnd;
    }

    /// {@return the offset in the current segment where the frames of the last pass of a flush start}
    long groupStart() {
        return groupStart;
    }

    /// {@return the forces made so far, a group written through counting as one}
    long forces() {
        return forces;
    }

    /// {@return the groups written through so far, for tests}
    long writtenThrough() {
        return writtenThrough;
    }

    /// {@return the way later segments are opened to write through, or `null` if their groups are forced, for tests}
    WriteThrough.Kind writeThroughKind() {
        return writeThrough.kind();
    }

    /// {@return the last sequence written to the file, forced or not}
    long writtenSequence() {
        return writtenSequence;
    }

    /// Makes everything queued durable, then closes the segment.
    ///
    /// @throws LedgerIoException if the queued frames cannot be made durable
    void close() {
        closing = true;
        long frames;
        queueLock.lock();
        try {
            // No frame followed it, so no commit needs it
            deferred = null;
            closed = true;
            frames = appended;
        } finally {
            queueLock.unlock();
        }
        try {
            if (durableFrames < frames) {
                wantFrames(frames);
            }
            durabilityLock.lock();
            try {
                while (durableFrames < frames) {
                    checkHealthy();
                    durabilityChanged.awaitUninterruptibly();
                }
            } finally {
                durabilityLock.unlock();
            }
        } finally {
            // The flush task has nothing left to write, or has stopped
            closeChannel();
            keepNext();
        }
    }

    /// {@return the first segment a later open may write: the one [#close()] kept for it, or the one after the current}
    long nextOpenSegment() {
        var segment = kept;
        return segment >= 0 ? segment : currentSegment + 1;
    }

    /// Keeps the segment prepared after the current one, which nothing wrote, for the next open, cut to
    /// [#KEPT_LENGTH] bytes: a spare's frames of an earlier use, or a new file's zeros, are not left on the disk of a
    /// closed store, nor in the cache of a copy of it.
    ///
    /// A preparation its thread has not started runs here, as a rollover runs it; one that has is waited for, by an
    /// interrupted thread too, so no file of the store changes once it is closed.
    private void keepNext() {
        var preparation = next;
        if (preparation == null) {
            return;
        }
        // A file operation would close its channel on an interrupted thread
        var interrupted = Thread.interrupted();
        preparation.run();
        try {
            while (true) {
                try {
                    var prepared = preparation.get();
                    // Not forced: recovery reads the segment the same at either length, and without the mark forces
                    // its name once more
                    try (var channel = FileChannel.open(prepared.path(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                        if (channel.size() > KEPT_LENGTH) {
                            channel.truncate(KEPT_LENGTH);
                        }
                        if (prepared.nameDurable()) {
                            mark(channel);
                        }
                    }
                    kept = prepared.number();
                    return;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } catch (ExecutionException | IOException _) {
            // An unused segment left behind holds no frame; the next open writes into it or into a new one
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Sets the byte at [#KEPT_MARK] in the segment open in `channel`, unless an earlier close did.
    private static void mark(FileChannel channel) throws IOException {
        var mark = ByteBuffer.allocate(1);
        if (channel.read(mark, KEPT_MARK) == 1 && mark.get(0) == KEPT_MARK_VALUE) {
            return;
        }
        mark.clear().put(0, KEPT_MARK_VALUE);
        while (mark.hasRemaining()) {
            channel.write(mark, KEPT_MARK);
        }
    }

    /// Forces the segment to stable storage; on macOS the JDK empties the drive's cache with `fcntl(F_FULLFSYNC)`.
    ///
    /// A write through replaces the force for the bytes it writes. On Linux `O_DSYNC` is the guarantee of `fdatasync`,
    /// which flushes the device's cache or writes with forced unit access, as the force does. On Windows a write
    /// through a handle opened with `FILE_FLAG_NO_BUFFERING` and `FILE_FLAG_WRITE_THROUGH` only requests a
    /// write-through of the drive's cache, where this force asks the device to empty it, so [WriteThrough.Mode#AUTO]
    /// replaces the force only on a device that reports that the request holds. [WriteThrough.Mode#DIRECT] and
    /// [WriteThrough.Mode#DSYNC] ask for less there, as they say. On macOS, whose `O_DSYNC` leaves the bytes in the
    /// drive's cache, the groups of a store on the default file system are always forced.
    ///
    /// An interrupt of a commit writing its own group that closes the channel meanwhile has everything since the last
    /// force written again through a new one, then forced: the interrupt may hide a failure of the force, after which
    /// the file system may report the next force a success with the bytes not on disk, so the pages are made dirty again
    /// first. A flush of the pool, whose bytes the output may not hold, fails as any failed force does.
    private void force() throws IOException {
        while (true) {
            try {
                channel.force(false);
                forcedPosition = position;
                nameDurable = true;
                used = 0;
                retained = 0;
                padded = false;
                delaying = false;
                return;
            } catch (ClosedByInterruptException exception) {
                if (!leading) {
                    throw exception;
                }
                reopen();
                rewriteUnforced();
            }
        }
    }

    /// Writes everything since the last force again, where it was written: a commit forcing its own group holds it all
    /// in the output ([#canRewriteUnforced()]).
    private void rewriteUnforced() throws IOException {
        while (true) {
            try {
                var buffer = ByteBuffer.wrap(output, 0, retained);
                var at = forcedPosition;
                while (buffer.hasRemaining()) {
                    at += channel.write(buffer, at);
                }
                return;
            } catch (ClosedByInterruptException _) {
                reopen();
            }
        }
    }

    /// Closes the channel of the current segment and its handle to write through.
    private void closeSegment() throws IOException {
        var handle = throughFile;
        throughFile = null;
        try {
            if (handle != null) {
                handle.close();
            }
        } finally {
            if (channel != null) {
                channel.close();
            }
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

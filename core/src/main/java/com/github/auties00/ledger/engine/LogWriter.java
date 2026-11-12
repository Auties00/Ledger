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
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32C;

/// Appends frames to the log and makes them durable in groups.
///
/// Commits queue their frames in order, holding the store's publishing role. One flush task at a time, on the store's
/// [com.github.auties00.ledger.IoPool], takes everything queued, seals the frames, writes them with one call, forces
/// the segment once and wakes the commits waiting for any of them: commits that arrive while a force runs make the
/// next group, so concurrent commits share forces. Each group is padded with zeros to a multiple of [#ALIGNMENT], where
/// the next one starts, so the pages of acknowledged frames are never written again.
///
/// The commits a force wakes come back with their next ones only after the flush took the next group, so on their own
/// they would always make the group after next: the committers would split into two halves that take turns, each
/// commit waiting for two forces. After a force that woke two or more waiting commits, or one while another still
/// waits, the flush therefore waits for them to wait again, their next frames queued, before it takes the next group,
/// for at most three quarters of a force and [#MAX_COHORT_WAIT]: one force then serves both halves. It waits only while
/// the woken commits and those queued behind them are at most [#COHORT_PER_PROCESSOR] a processor, as more take longer
/// to come back than the force they save; a wait that runs out turns the next ones off for 2, then 4, up to 64 groups,
/// and a close, a failure, a full queue or a task of another store waiting for the pool ends one at once.
///
/// A flush of the pool whose store shares the pool writes one group at a time while a task of another store waits for
/// a thread: it submits itself again behind that task, so the stores on a pool take turns a group each. A store whose
/// queue never drains would otherwise hold a thread for as long as its commits keep coming.
///
/// Segments come from [LogFiles] prepared: once a segment takes its first group, the next one is prepared on a thread
/// of its own, so a rollover only forces the full segment and switches to the next, and no file is created or renamed
/// on the way to a force. The first one is prepared the same way while the store opens, and the first group waits for
/// it: forcing a new file and its directory took most of an open on Windows and macOS. A prepared file may hold the
/// frames of an earlier use, which every frame header tells apart by the tag of its segment's salt.
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
/// Where [WriteThrough] can, a group that one write holds and that lands below the bytes its segment already has is
/// written through to stable storage instead, durable once the write returns, and not forced: one round trip to the
/// device where a write and a force take two. The first such group opens the segment a second time for it, so a segment
/// with none costs nothing. A group of several writes, one that rolls over, or one that would grow the file is written
/// into the cache and forced as before, so an ingest keeps the bandwidth of the cache, and a file system that refuses
/// the writes through moves the writer to forces.
///
/// A store just opened has such bytes only in the first block of its first segment, which its first group takes when
/// one block holds it. The next segment is prepared without zeros, and written with them on a thread of its own once
/// commits have waited for [#ZEROS_AFTER] groups: on Windows a force beside the zeros takes several times as long,
/// which the few commits of a store that opens, writes and closes would pay and never gain from. The log moves to it between two groups as soon as it is ready, leaving the rest of the segment it was writing
/// unused; until then each group is forced. So a run of commits reaches the path within the time of one preparation,
/// whatever the size of a segment.
///
/// Two groups written through may be on the device at once ([Overlap]): while the flush's group is there, a second
/// lane, a platform thread with a handle and an output of its own, takes the frames queued since and writes them
/// through as the next group, so the commits one write woke come back while the other is on the device, and a device
/// that serves two writes together serves both halves of the committers in one round trip. A group goes beside the one
/// before it only if both are written through into the same segment, past the first group of its use, neither holds
/// units of the file of large values nor is a commit's own, commits wait for it, and the one before is the only group
/// not durable: the group after waits for that one. Groups are published in the order they were laid out, so a commit
/// is acknowledged only once every group up to its own is durable. The first frame of a group written beside another
/// carries [#OVERLAPS] and the distance back to it, which recovery reads to drop the group with that one when a crash
/// tears it ([LogRecovery]). [Overlap#AUTO] measures the rate of commits with groups beside one another against one at
/// a time, and keeps the faster.
///
/// A segment is a file of [#SEGMENT_HEADER] bytes of header followed by frames. Each use of it, the writing of one
/// open, draws a random tag, and the key of the use derives from the segment's number, a random salt of its own and
/// that tag: the offset of a frame is a unique nonce under its key, even where an open writes into a segment that an
/// earlier one began to write and crashed before any of it was durable, whose bytes a copy-on-write file system may
/// keep, and even if a segment number were ever used twice. A use writes past its first group only once a force made
/// that group durable, after which no open writes into the segment again, so at most one use of a segment has frames
/// past its first group:
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
///                 16  frame type
///                 17  flags: 1 for the first frame of a group; 2 as well for one of a group written beside the last
///                 18  with flag 2, the distance back to the start of the group before, in blocks of 4 KiB
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
/// write through only moves the writer to forces; one of a group beside another first waits for the other's write,
/// then writes its group again through the cache and forces it, and groups go one at a time from then on.
final class LogWriter {
    static final int SEGMENT_HEADER = 4_096;
    static final int ALIGNMENT = 4_096;
    static final int FRAME_HEADER = 40;
    static final int MAGIC = 0x4C474631;
    static final byte GROUP_START = 1;
    /// The flag of the first frame of a group written while the group before it was not durable yet, whose frame
    /// header then holds, in bytes 18 and 19, the distance back to that group's start in blocks of [#ALIGNMENT].
    static final byte OVERLAPS = 2;
    static final long SEGMENT_MAGIC = 0x4C475345474D4E54L;
    /// The version of the segments a writer starts: from 3 a segment may hold frames of type [FrameBody#COMMITS], and
    /// from 4 groups with [#OVERLAPS], so a build that reads only earlier versions refuses it by its version and not at
    /// the first such frame or at a crash it would read as damage.
    static final int SEGMENT_VERSION = 4;
    /// The oldest version whose groups may carry [#OVERLAPS].
    static final int OVERLAPS_VERSION = 4;
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
    /// The longest a flush waits for the commits the last force woke to come back, in nanoseconds: a force of a small
    /// group took a tenth to a fifth of it on the devices measured.
    private static final long MAX_COHORT_WAIT = 1_000_000;
    /// How long a flush waiting for commits to come back spins before it yields its processor between looks, in
    /// nanoseconds: the threads it waits for may need that processor, and a timed park, which lasts at least a
    /// millisecond on Windows, would outlast the wait.
    private static final long COHORT_SPIN = 4_000;
    /// The woken and queued commits a processor past which a flush no longer waits for the woken ones: the wait pays
    /// only while they come back in less than a force, and each takes a few microseconds of a processor on its way, so
    /// 64 of them on 4 processors take about as long as the force they would save.
    private static final int COHORT_PER_PROCESSOR = 4;
    /// The waits in a row that ran out past which waits are turned off no longer: after the n-th, the flush writes 2
    /// to the power of n groups without one.
    private static final int MAX_COHORT_MISSES = 6;
    /// How long the second lane waits for work before its thread ends, in nanoseconds.
    private static final long LANE_IDLE = 1_000_000_000;
    // The states of a lane
    private static final int IDLE = 0;
    private static final int WRITING = 1;
    private static final int WRITTEN = 2;
    private static final int FAILED = 3;
    private final LogFiles files;
    private final StoreKeys keys;
    private final byte[] storeId;
    private final long segmentSize;
    private final Executor executor;
    // Whether a task waits for a thread of the pool, which a flush of the pool gives up its thread to, or null if the
    // store owns its pool
    private final BooleanSupplier poolQueued;
    private final long commitWindowNanos;
    // How the flush task opens segments to write groups through; owned by it
    private final WriteThrough writeThrough;
    // The woken and queued commits past which a flush no longer waits for the woken ones to come back
    private final int maxCohort;
    // Guards the layout of groups and the state of the lanes: the flush that holds running holds it while it takes,
    // seals and, unless its group may have another beside it, writes a group; the second lane while it takes and seals
    // one; either while it publishes
    private final ReentrantLock laneLock;
    // Signalled under the lane lock when a lane changes state
    private final Condition laneChanged;
    // The group of the flush that holds running, and the one the second lane writes beside it
    private final Lane first;
    private final Lane second;
    // The groups laid out since the writer opened, which order the lanes' groups; under the lane lock
    private long tickets;
    // Whether a group may go beside another now, as the epochs of AUTO decide; written under the lane lock
    private volatile boolean overlapOn;
    // Whether a failure turned groups beside one another off for good; under the lane lock
    private boolean overlapBroken;
    // The thread of the second lane, or null while there is none; written under the lane lock
    private volatile Thread laneThread;
    // Whether a close asked the thread of the second lane to end
    private volatile boolean laneStop;
    // Whether the second lane waits for frames to write beside the group of the first, which an append wakes it for
    private volatile boolean laneWanted;
    // The output the second lane seals into, and its groups in a row that needed at most half of MIN_OUTPUT; under the
    // lane lock
    private byte[] laneOutput;
    private int laneSmallGroups;
    // The current segment opened to write through for the second lane, or null until it writes into it
    private WriteThrough.Handle laneHandle;
    // The emptied queue the second lane's next take reuses
    private ArrayList<Queued> laneSpare;
    // The commits woken and not yet taken back into a group, which a lane about to take a group waits for; written
    // under the lane lock
    private volatile int cohortLeft;
    // The frames queued when the last group was taken
    private volatile long takenFrames;
    // Whether the last published group was written through, so the next may go beside another
    private volatile boolean lastThrough;
    // The blocks back from the group being sealed to the start of the one before it, which its first frame names, or 0
    private int overlapBack;
    // What decides, under AUTO, whether a group may go beside another; null in the other modes. Under the lane lock
    private final OverlapGate gate;
    // The groups written beside another, for tests
    private volatile long overlappedGroups;

    // The state of the segment below, up to the output, is the flush task's and the second lane's, under the lane lock;
    // null until the first group opens the first segment
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
    // The end of the bytes of the current segment a force or a write through made durable, the groups before it
    // published
    private long forcedPosition;
    // The file of the current segment, which an interrupted write reopens
    private java.nio.file.Path segmentPath;
    // Whether an interrupt closed the channel or cut a wait short during the flush, which then gives the interrupt
    // back to its thread
    private boolean interruptedWrite;
    // Whether the flush running is a commit's, for its own group, which one drain writes whole
    private boolean leading;
    // Where the last drain wrote, and how many bytes, which the output still holds until the next group is sealed
    private long lastDrainStart;
    private int lastDrainLength;
    private long segmentNumber;
    private long position;
    private SecretKeySpec segmentKey;
    private long segmentTag;
    // The tag of this writer's use of the segment
    private long useTag;
    // The segment close kept for the next open, or -1
    private volatile long kept = -1;
    // The end of the last published group's frames, before its padding, for tests
    private volatile long groupEnd;
    // Where the frames of the last published group start, for tests
    private volatile long groupStart;
    // The preparation of the segment after the current one, run on a thread of its own, or by whichever of the flush
    // task and a close needs it first
    private FutureTask<LogFiles.Prepared> next;
    // Whether zeros were asked for the segment of next, by its preparation or after it; under the lane lock
    private boolean nextZeroed;
    // When the current segment opened, and how long the one before it took to fill, or -1; under the lane lock
    private long segmentOpened;
    private long lastFill = -1;
    // Whether a commit waited for a force since the segment before the current one opened
    private volatile boolean waited;
    private boolean waitedBefore;
    // Whether a commit waited since the last group began to be written
    private volatile boolean waitedGroup;
    // The groups written since the store opened that a commit waited for; under the lane lock
    private long waitedGroups;
    private final Cipher cipher;
    private final CRC32C crc;
    // The buffer frames are sealed into and written from, a slice at a time whatever their size; it grows up to OUTPUT
    // while a group needs more, so a group that fits is written with one call
    private byte[] output;
    // The bytes of output not written yet
    private int used;
    // The groups in a row written since one needed more than half of MIN_OUTPUT
    private int smallGroups;
    // How long the write and force of a group took, or its write through, a moving average in nanoseconds of the groups
    // after the first that no other group was written beside, each at most four times the average before it, or 0
    // before the second group; written under the lane lock
    private volatile long forceNanos;
    // The commits the last published group woke, its leader included; read by commits deciding whether to lead
    private volatile int lastGroupWaiters;
    // The commits the last published group woke and those queued behind it
    private int cohortSpan;
    // The commits the last published group woke that the next flush waits for, or 0; the registrations counted before
    // they were woken, and when they were
    private int cohort;
    private long cohortBase;
    private long cohortStart;
    // The waits for a cohort in a row that ran out, and the groups still to publish without one
    private int cohortMisses;
    private int skippedGroups;
    // Whether commits count their registrations as waiters, for a flush that waits for its cohort
    private volatile boolean cohortArmed;
    private final Padded.Count registered;
    // The waits for a cohort that ended with all of it back and that ran out, for tests
    private volatile long cohortsGathered;
    private volatile long cohortsMissed;
    // The emptied queue, which the next swap reuses
    private ArrayList<Queued> flushing;
    // Written under the lane lock
    private volatile long forces;
    // The groups written through, of the forces; written under the lane lock
    private volatile long writtenThrough;
    // The segment the flush task writes, for a checkpoint to read
    private volatile long currentSegment;

    private final ReentrantLock queueLock;
    // Signalled when a group is durable, for the commits waiting on a full queue
    private final Condition queueDrained;
    private ArrayList<Queued> queue;
    // The bytes of the frame bodies queued and not durable yet
    private long pendingBytes;
    // The commits waiting for room in the queue; written under the queue lock
    private volatile int queueWaiters;
    private volatile long lastQueued;
    // Frames queued since the writer opened, written under the queue lock; a catalog frame carries a sequence already
    // durable, so close() waits on frames rather than on sequences
    private volatile long appended;
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
    private long written;
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
    /// @param poolQueued whether a task waits for a thread of the pool, or `null` if the store owns its pool: a flush
    ///                   waiting for the commits a force woke stops waiting once one does, and a flush of the pool
    ///                   gives up its thread after the group it wrote
    /// @param commitWindow how long a flush waits for more commits before it starts
    /// @param empty the segment numbered `firstSegment`, which recovery found prepared and never written, to write
    ///              first; or `null`, to prepare one
    /// @param writeThrough how the segments are opened to write groups through, which the writer owns from now on
    /// @param overlap whether a group written through may go to the device while the one before it is still there
    /// @throws IOException if the segment cannot be prepared
    LogWriter(LogFiles files, StoreKeys keys, byte[] storeId, long firstSegment, long durable, long segmentSize,
              Executor executor, BooleanSupplier poolQueued, Duration commitWindow, LogFiles.Prepared empty,
              WriteThrough writeThrough, Overlap overlap) throws IOException {
        this.files = files;
        this.keys = keys;
        this.storeId = storeId;
        this.segmentSize = segmentSize;
        this.executor = executor;
        this.poolQueued = poolQueued;
        this.commitWindowNanos = commitWindow.toNanos();
        this.writeThrough = writeThrough;
        this.maxCohort = COHORT_PER_PROCESSOR * Runtime.getRuntime().availableProcessors();
        this.laneLock = new ReentrantLock();
        this.laneChanged = laneLock.newCondition();
        this.first = new Lane();
        this.second = new Lane();
        this.laneSpare = new ArrayList<>();
        this.overlapOn = overlap == Overlap.ON;
        this.gate = overlap == Overlap.AUTO ? new OverlapGate(System::nanoTime, durable) : null;
        this.registered = new Padded.Count();
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
        // Its preparation forced the header. A name no force made durable has the first group forced, which makes it so
        this.forcedPosition = segment.nameDurable() ? SEGMENT_HEADER : -1;
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
        if ((throughWanted && position + ALIGNMENT <= valid) || !(waited || waitedBefore) || writeThrough.kind() == null) {
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
    /// more than [#QUEUE_LIMIT] bytes not durable yet, it waits for the device, which holds back every commit behind
    /// it: memory stays bounded when commits come faster than the device takes them. A frame larger than the limit
    /// waits for the queue to empty.
    ///
    /// @throws LedgerIoException if the writer has stopped
    void append(byte type, long sequence, FrameBody body) {
        append(type, sequence, body, 0, 0, false);
    }

    /// Queues a frame as [#append(byte, long, FrameBody)] does, for a commit of `writes` writes that wrote `unitBytes`
    /// of units of the file of large values.
    ///
    /// The group that holds the frame forces that file beside the segment. The units count toward the growth that
    /// starts a checkpoint, as if their values were in the frame: memory holds them until a checkpoint takes them. The
    /// writes count toward [#appendedHeap()].
    ///
    /// @param sharesChunk whether the commit's values are on one chunk of the [ValueSlab], as [#heapOf] takes it
    /// @return the frames queued since the writer opened, the new one included
    /// @throws LedgerIoException if the writer has stopped
    long append(byte type, long sequence, FrameBody body, long unitBytes, int writes, boolean sharesChunk) {
        checkHealthy();
        var length = body.size();
        long frames;
        queueLock.lock();
        try {
            while (pendingBytes > 0 && pendingBytes + length > QUEUE_LIMIT && failure == null) {
                awaitRoom();
            }
            checkHealthy();
            queueDeferred();
            queue.add(new Queued(type, sequence, body));
            pendingBytes += length;
            lastQueued = sequence;
            frames = ++appended;
            appendedBytes += 13 + length + unitBytes;
            appendedHeap += heapOf(length, unitBytes, writes, sharesChunk);
            queuedUnits |= unitBytes > 0;
        } finally {
            queueLock.unlock();
        }
        wakeLane();
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
        return frames;
    }

    /// Queues the frames of `batch`, in its order, as [#append(byte, long, FrameBody, long, int, boolean)] queues each, and
    /// empties it: one hold of the queue lock for a whole pass of the store's publisher.
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
                awaitRoom();
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
        wakeLane();
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
    }

    /// Waits, holding the queue lock, for a group to leave the queue, counted in [#queueWaiters] meanwhile.
    private void awaitRoom() {
        queueWaiters++;
        try {
            queueDrained.awaitUninterruptibly();
        } finally {
            queueWaiters--;
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
        wakeLane();
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
    /// stops, so the frames of a commit whose flush returned are never left behind. The second lane only takes what is
    /// queued while a group of the flush is on the device, and never stops a flush from being scheduled. A flush for a
    /// commit hands what is queued after its group to the pool, as a flush of the pool does when a task of another
    /// store waits for its thread.
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
            if (until != Long.MAX_VALUE || poolWanted()) {
                try {
                    executor.execute(this::flush);
                } catch (RejectedExecutionException exception) {
                    stop(new IOException("The log's pool refused a flush: it was closed with the store open", exception));
                }
                return;
            }
        }
    }

    /// {@return whether the frame of the commit at `sequence` is queued, no later commit's is, no other commit waits
    /// for its durability, and the last group made at most one waiting commit durable}
    ///
    /// Only such a commit writes its group itself. With other commits under way, the pool's flush goes on to their
    /// group at once, where a commit that wrote its own would hand that group to the pool and wait for a thread of it
    /// to wake; and the wake of the pool's thread lets more commits join a group than a commit forcing its own at once
    /// would, which at eight committers made fewer forces than leading did. Right after a group that woke several
    /// commits, the first to come back finds no other waiting, as the rest are on their way: leading, it would write a
    /// group of one while they queue behind it, where the pool's flush waits for them all.
    ///
    /// Only without a commit window, which the pool's flush waits out for the commits that join the group. A virtual
    /// thread leads too: its force lends the carrier's place to another, as the JDK does for a force, and only the write
    /// of the group into the file's cache holds the carrier; a commit of one virtual thread at a time made a fifth more
    /// commits a second where it did. On Windows the write of a group through to stable storage holds the carrier too,
    /// one at a time as only one flush runs, while the other carriers go on.
    private boolean alone(long sequence) {
        return commitWindowNanos == 0 && lastGroupWaiters <= 1 && waiters.isEmpty() && lastQueued == sequence
                && durable < sequence;
    }

    /// {@return whether `frames` seal into the output and the current segment, so the group is written with one call
    /// that a write cut off by an interrupt repeats, with no rollover to prepare a segment on the caller's thread}
    private boolean fitsOneWrite(List<Queued> frames) {
        if (channel == null) {
            return false;
        }
        var bytes = sealedBytes(frames);
        return bytes >= 0 && position + bytes + ALIGNMENT <= segmentSize;
    }

    /// {@return a bound on the bytes `frames` seal into, each counted as a frame of its own, more than the run that may
    /// hold it takes; or -1 if the output cannot hold that many and a block of padding}
    private long sealedBytes(List<Queued> frames) {
        var bytes = 0L;
        for (var frame : frames) {
            bytes += FRAME_HEADER + frame.body().size() + (keys.encrypted() ? TAG_BYTES : Integer.BYTES);
            if (bytes > OUTPUT - ALIGNMENT) {
                return -1;
            }
        }
        return bytes;
    }

    /// {@return whether `group` is written through: it seals into one write, which starts where everything before it
    /// is durable and ends, padded, within the bytes the segment had when it was prepared; the first that does opens the
    /// segment for it, if a way is left}
    ///
    /// Within those bytes a write changes neither the size of the file nor the length of its data, so it needs no
    /// metadata written with it: the one write with forced unit access that makes a write through cheaper than a
    /// write and a force.
    private boolean writesThrough(List<Queued> group) {
        if (!throughWanted || position != forcedPosition) {
            return false;
        }
        var bytes = sealedBytes(group);
        if (bytes < 0) {
            return false;
        }
        var end = (position + bytes + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
        if (end > valid || position + bytes + ALIGNMENT > segmentSize) {
            return false;
        }
        if (throughFile == null) {
            throughFile = openThrough();
            throughWanted = throughFile != null;
        }
        return throughWanted;
    }

    /// {@return whether a task waits for a thread of the pool this store shares}
    private boolean poolWanted() {
        return poolQueued != null && poolQueued.getAsBoolean();
    }

    /// Writes the queued groups until the queue is empty, the commit at `until` is durable or, on the pool, a task
    /// waits for this thread, holding [#running].
    ///
    /// A group that may have another beside it goes to the device with the lane lock let go, so the second lane can
    /// take and write the next group meanwhile ([#runLane()]); any other is written with the lock held, as nothing else
    /// is in flight.
    private void flushGroups(long until) {
        leading = until != Long.MAX_VALUE;
        try {
            // A stopped writer writes nothing more: a group after one whose force failed would pass for durable while
            // the failed one, maybe partly on disk, stands before it
            while (durable < until && failure == null) {
                if (commitWindowNanos > 0) {
                    LockSupport.parkNanos(commitWindowNanos);
                } else if (cohort > 0 && !leading) {
                    // With the queue empty too: had it returned, the next flush would take the first commits back as a
                    // group of their own
                    awaitCohort();
                } else if (!leading && overlapOn && lastThrough) {
                    gather(second);
                }
                byte[] bytes;
                WriteThrough.Handle handle;
                laneLock.lock();
                try {
                    ArrayList<Queued> group = null;
                    long sequence = 0;
                    long frames = 0;
                    var units = false;
                    var beside = false;
                    while (group == null) {
                        if (failure != null) {
                            return;
                        }
                        // The output still holds a group that is not durable, which a failed write writes again
                        if (first.state != IDLE) {
                            laneChanged.awaitUninterruptibly();
                            continue;
                        }
                        beside = second.state != IDLE;
                        var wait = false;
                        queueLock.lock();
                        try {
                            if (queue.isEmpty() || (leading && (lastQueued > until || !fitsOneWrite(queue)))) {
                                return;
                            }
                            if (beside && !admitsBeside(second, queue, queuedUnits, leading)) {
                                wait = true;
                            } else {
                                group = queue;
                                sequence = lastQueued;
                                frames = appended;
                                units = queuedUnits;
                                queuedUnits = false;
                                queue = flushing;
                                takenFrames = frames;
                            }
                        } finally {
                            queueLock.unlock();
                        }
                        if (wait) {
                            laneChanged.awaitUninterruptibly();
                        }
                    }
                    cohortLeft = Math.max(0, cohortLeft - group.size());
                    var open = write(group, units, sequence, frames, beside);
                    group.clear();
                    flushing = group;
                    if (!open) {
                        if (!leading && poolWanted()) {
                            return;
                        }
                        continue;
                    }
                    bytes = output;
                    handle = throughFile;
                    if (second.state == IDLE) {
                        // Before the look at the queue below, so a frame queued after it wakes the lane itself
                        startLane();
                        laneWanted = true;
                    }
                } finally {
                    laneLock.unlock();
                }
                if (appended != takenFrames) {
                    wakeLane();
                }
                writeOut(first, handle, bytes);
                laneLock.lock();
                try {
                    complete(first);
                } finally {
                    laneLock.unlock();
                }
                if (!leading && poolWanted()) {
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

    /// Waits, holding [#running], until the commits the last group woke wait again, their next frames queued, so the
    /// next group takes them beside the frames queued during the last force; or until three quarters of a force, at
    /// most [#MAX_COHORT_WAIT], have passed since the last group was published.
    ///
    /// It spins, then yields between looks. A close, a failure, a commit waiting for room in the queue, or a task of
    /// another store waiting for the pool ends it at once. A wait that runs out while a frame waits in the queue turns
    /// the next ones off for twice as many groups as the last that ran out did, from 2 to 64; one that ends with all of
    /// them back resets that.
    private void awaitCohort() {
        var target = cohort;
        cohort = 0;
        try {
            var budget = Math.min(forceNanos - forceNanos / 4, MAX_COHORT_WAIT);
            var looked = false;
            while (registered.get() - cohortBase < target) {
                var waited = System.nanoTime() - cohortStart;
                if (waited >= budget) {
                    // One that ran out before this flush started, after a commit's own group, or with no frame queued
                    // since the group, as when the committers stop, held back no commit
                    if (looked && lastQueued > durable) {
                        cohortMisses = Math.min(cohortMisses + 1, MAX_COHORT_MISSES);
                        skippedGroups = 1 << cohortMisses;
                        cohortsMissed++;
                    }
                    return;
                }
                if (failure != null || closing || queueWaiters > 0 || poolWanted()) {
                    return;
                }
                looked = true;
                if (waited < COHORT_SPIN) {
                    Thread.onSpinWait();
                } else {
                    Thread.yield();
                }
            }
            cohortMisses = 0;
            cohortsGathered++;
        } finally {
            cohortArmed = false;
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

    /// Seals `group` into the output and writes it, then forces it, and the file of large values if `units`; or writes
    /// it through, if [#writesThrough(List)], and forces only that file; or, if another group may go beside it, leaves
    /// its write through to the caller, with the lane lock let go. Holding the lane lock, with [#first] idle.
    ///
    /// The commits of the group wrote their units before they queued their frames, so one force of that file, run
    /// beside the segment's write and force, makes them all durable before the group is. A rollover waits for it
    /// before it forces the segment: the first frame of a group in the next segment then means, as it always does,
    /// that everything before it is durable.
    ///
    /// A group written here is published before this returns. One left to the caller is laid out in [#first], in the
    /// state [#WRITING], with the output holding its bytes and [#position] past them: the caller writes it with
    /// [#writeOut] and settles it with [#complete].
    ///
    /// @param sequence the commit up to which the group makes the log durable
    /// @param frames the frames queued up to its last one
    /// @param beside whether [#second] holds a group not durable yet, which [#admitsBeside] admitted this one beside
    /// @return whether the group's write is left to the caller
    private boolean write(List<Queued> group, boolean units, long sequence, long frames, boolean beside) throws IOException {
        if (!beside) {
            if (channel == null) {
                rollover();
            } else if (rollsEarly()) {
                rollEarly();
            }
        }
        // Between groups: the last drain's bytes, which an interrupted force writes again, are durable by now
        if (smallGroups >= SHRINK_AFTER && output.length > MIN_OUTPUT) {
            output = new byte[MIN_OUTPUT];
        }
        if (waitedGroup) {
            waitedGroup = false;
            waitedGroups++;
        }
        // Before a frame is sealed, while the output holds no byte of the group
        var through = beside || writesThrough(group);
        var open = beside || (through && overlapOn && !overlapBroken && !leading && !units && wayProven && !closing
                && position > SEGMENT_HEADER && throughFile != null);
        var unitForce = units ? forceUnits() : null;
        var start = position;
        var groupStart = true;
        var bodies = 0L;
        for (var frame : group) {
            bodies += frame.body().size();
        }
        overlapBack = beside ? (int) ((start - second.start) / ALIGNMENT) : 0;
        var count = group.size();
        for (var index = 0; index < count; ) {
            var frame = group.get(index);
            var bodyLength = frame.body().size();
            var sealedLength = bodyLength + sealOverhead();
            // The test of the frame alone: a run takes the commits after its first only while it fits in the segment
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
            if (bodyLength > RUN_MAX_BODY) {
                seal(frame, sealedLength, groupStart);
                index++;
            } else {
                var end = runEnd(group, index);
                sealRun(group, index, end, groupStart);
                index = end;
            }
            groupStart = false;
        }
        overlapBack = 0;
        // Zeros to the next block, where the next group starts: a reused file holds older bytes there
        var end = position + used;
        var padding = (int) ((end + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT - end);
        room(padding);
        Arrays.fill(output, used, used + padding, (byte) 0);
        used += padding;
        smallGroups = position + used - start <= MIN_OUTPUT / 2 ? smallGroups + 1 : 0;
        var lane = first;
        lane.ticket = ++tickets;
        lane.start = start;
        lane.frameEnd = end;
        lane.sequence = sequence;
        lane.frames = frames;
        lane.bodies = bodies;
        lane.leading = leading;
        lane.overlapped = beside;
        lane.through = through;
        lane.failure = null;
        lane.interrupted = false;
        if (open) {
            lane.length = used;
            lane.overlappable = true;
            lane.open = true;
            lane.state = WRITING;
            if (beside) {
                second.overlapped = true;
                overlappedGroups++;
            }
            position = start + used;
            used = 0;
            return true;
        }
        lane.length = (int) (position + used - start);
        lane.overlappable = false;
        lane.open = false;
        var forceStart = System.nanoTime();
        if (through) {
            writeThrough();
        } else {
            drain();
            force();
        }
        var took = Math.max(1, System.nanoTime() - forceStart);
        // The first group pays for the first force of its segment, and an average one stalled force moves would hold
        // the wait for woken commits at its bound for dozens of groups: the first counts for nothing, and a later one
        // for four times the average at most
        if (forces > 0) {
            averageForce(took);
        }
        forces++;
        if (unitForce != null) {
            await(unitForce);
        }
        if (next == null) {
            prepareNext();
        } else {
            fillNext();
        }
        lane.state = WRITTEN;
        publishReady();
        return false;
    }

    /// Takes `took` nanoseconds into [#forceNanos], at most four times the average before it.
    private void averageForce(long took) {
        var average = forceNanos;
        forceNanos = average == 0 ? took : average + (Math.min(took, 4 * average) - average) / 8;
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
        var limit = Math.min(RUN_BYTES, segmentSize - position - used - FRAME_HEADER - sealOverhead());
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
        var frameOffset = position + at;
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

    /// Writes the header of a frame at `at` of the output; the first of a group written beside the one before it
    /// carries [#OVERLAPS] and [#overlapBack].
    private void writeHeader(int at, int sealedLength, long sequence, byte type, boolean groupStart) {
        Bytes.i32(output, at, MAGIC);
        Bytes.i32(output, at + 4, sealedLength);
        Bytes.i64(output, at + 8, sequence);
        output[at + 16] = type;
        var back = groupStart ? overlapBack : 0;
        output[at + 17] = (byte) ((groupStart ? GROUP_START : 0) | (back > 0 ? OVERLAPS : 0));
        Bytes.u16(output, at + 18, back);
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
        var frameOffset = position + used;
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

    /// Makes room for `bytes` more in the output: grows it while it is smaller than [#OUTPUT], else writes it out.
    private void room(int bytes) throws IOException {
        if (used + bytes > output.length) {
            if (output.length < OUTPUT) {
                var grown = Math.min(OUTPUT, Math.max(2 * output.length, used + bytes));
                // The bytes not written yet stay where they are
                output = Arrays.copyOf(output, grown);
                if (used + bytes <= grown) {
                    return;
                }
            }
            drain();
        }
    }

    /// Writes the bytes of the output.
    ///
    /// An interrupt that closes the channel meanwhile has the bytes written again, the same bytes at the same offsets.
    private void drain() throws IOException {
        var start = position;
        lastDrainStart = start;
        lastDrainLength = used;
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

    /// Writes the bytes of the output, a group padded to a block, through to stable storage: durable once this
    /// returns, as a force would make them.
    ///
    /// An interrupt that closes the handle meanwhile has the bytes written again through a new one, the same bytes at
    /// the same offsets, as [#drain()] does. A write through that fails otherwise, as one a file system refuses the
    /// alignment of does, moves the writer to forces: the group is written again into the cache and forced, which
    /// makes each of its pages dirty again whatever the failed write left on the device, so a force that succeeds
    /// makes the group durable, and one that fails stops the writer as it always does. Until a write has succeeded in
    /// a way, a failure moves the writer one way down instead, and the next way writes the same bytes at the same
    /// offsets.
    private void writeThrough() throws IOException {
        var start = position;
        lastDrainStart = start;
        lastDrainLength = used;
        while (throughFile != null) {
            try {
                throughFile.write(output, used, output.length, start);
                position = start + used;
                used = 0;
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
        /// units, whose values are on one chunk of the [ValueSlab] if `sharesChunk`.
        void add(byte type, long sequence, FrameBody body, long unitBytes, int writes, boolean sharesChunk) {
            frames.add(new Queued(type, sequence, body));
            bodyBytes += body.size();
            appendedBytes += 13 + body.size() + unitBytes;
            appendedHeap += heapOf(body.size(), unitBytes, writes, sharesChunk);
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

    /// Makes the commits up to `sequence`, and the first `frames` frames, durable, wakes the commits waiting for them
    /// and, if the queue is `roomy` and the flush that holds [#running] publishes, has its next group wait for the
    /// woken commits to come back.
    ///
    /// One woken commit is waited for only while another still waits: two committers out of step wake one each, and
    /// never merge otherwise, while a lone committer leaves no other waiting and is never held back. After a group
    /// written through while groups go beside one another, the lanes wait for the woken commits themselves
    /// ([#gather(Lane)]); after a forced one, whose successor is forced alone too, the flush does.
    ///
    /// @param leader whether the group is a commit's own, whose thread comes back too
    /// @param main whether the flush that holds [#running] publishes, which alone owns the wait for woken commits
    /// @param through whether the group was written through
    /// @return the commits it woke, the leader included
    private int publish(long sequence, long frames, boolean roomy, boolean leader, boolean main, boolean through) {
        var arm = main && (!overlapOn || !through) && roomy && commitWindowNanos == 0 && skippedGroups == 0
                && cohortSpan <= maxCohort && forceNanos > 0 && !closing;
        if (main && skippedGroups > 0) {
            skippedGroups--;
        }
        if (arm) {
            // Before durable, which every woken commit reads before its next one registers
            cohortBase = registered.get();
            cohortArmed = true;
        }
        durable = sequence;
        durabilityLock.lock();
        try {
            written = frames;
            durabilityChanged.signalAll();
        } finally {
            durabilityLock.unlock();
        }
        // A commit that wrote its own group comes back too
        var woken = wake(sequence) + (leader ? 1 : 0);
        var span = woken + (int) Math.clamp(lastQueued - sequence, 0, Integer.MAX_VALUE);
        lastGroupWaiters = woken;
        cohortSpan = span;
        if (arm && span <= maxCohort && (woken >= 2 || (woken == 1 && !waiters.isEmpty()))) {
            cohort = woken;
            cohortStart = System.nanoTime();
        } else if (arm) {
            cohortArmed = false;
        }
        return woken;
    }

    /// Wakes the commits waiting for a sequence at or below `sequence`.
    ///
    /// @return the commits it woke
    private int wake(long sequence) {
        var woken = 0;
        for (var iterator = waiters.iterator(); iterator.hasNext(); ) {
            var waiter = iterator.next();
            if (waiter.sequence <= sequence) {
                waiter.woken = true;
                iterator.remove();
                LockSupport.unpark(waiter.thread);
                woken++;
            }
        }
        return woken;
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
        // A flush waiting for a lane to go idle looks at the failure
        if (laneLock.isHeldByCurrentThread()) {
            laneChanged.signalAll();
        } else {
            laneLock.lock();
            try {
                laneChanged.signalAll();
            } finally {
                laneLock.unlock();
            }
        }
    }

    /// {@return whether `frames` may go to the device as one group while the group of `other`, the last laid out, is
    /// still there: groups beside one another are on, both are written through into the same segment, neither is a
    /// commit's own nor holds units of the file of large values, the group fits one write within the bytes the segment
    /// had when it was prepared, and commits wait for durability}
    ///
    /// The group of `other` is the only one not published, so everything before it is durable: a crash that tears it
    /// leaves this one the only later group, which recovery tells by the distance back to it its first frame names, and
    /// the group after this one waits for `other`'s. Holding the lane lock and the queue lock.
    ///
    /// @param leader whether a commit's flush would write the group, which it writes alone
    private boolean admitsBeside(Lane other, List<Queued> frames, boolean units, boolean leader) {
        if (leader || units || !overlapOn || overlapBroken || closing || failure != null || !other.overlappable
                || waiters.isEmpty() || (other == second ? throughFile : laneHandle) == null) {
            return false;
        }
        var bytes = sealedBytes(frames);
        if (bytes < 0) {
            return false;
        }
        var end = (position + bytes + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
        return end <= valid && position + bytes + ALIGNMENT <= segmentSize && (position - other.start) / ALIGNMENT <= 0xFFFF;
    }

    /// Waits, outside the lane lock, for the commits woken and not taken back into a group to queue their next frames:
    /// all of them while `other` holds a group, so each lane writes the commits its own group woke, or half of them
    /// while it does not, so the other half makes the group the second lane writes beside this one.
    ///
    /// It spins, then yields, for at most three quarters of a force and [#MAX_COHORT_WAIT], as [#awaitCohort()] does.
    /// A close, a failure, a commit waiting for room in the queue, groups going back to one lane, or, for the main
    /// flush, a task of another store waiting for the pool, ends it at once; for the second lane, so does the end of
    /// the write of [#first], which leaves nothing to go beside.
    private void gather(Lane other) {
        var left = cohortLeft;
        var average = forceNanos;
        var budget = Math.min(average - average / 4, MAX_COHORT_WAIT);
        if (left <= 0 || budget <= 0) {
            return;
        }
        var target = other.state != IDLE ? left : (left + 1) / 2;
        var began = System.nanoTime();
        while (appended - takenFrames < target) {
            var waited = System.nanoTime() - began;
            if (waited >= budget || failure != null || closing || queueWaiters > 0 || !overlapOn
                    || (other == first ? first.state != WRITING : poolWanted())) {
                return;
            }
            if (waited < COHORT_SPIN) {
                Thread.onSpinWait();
            } else {
                Thread.yield();
            }
        }
    }

    /// Writes the group laid out in `lane` through `handle` from `bytes`, outside the lane lock, and records how long
    /// it took or why it failed, for [#complete(Lane)].
    ///
    /// An interrupt of the thread, which nothing should interrupt, closes the handle: the failure records it, and the
    /// interrupt is cleared, so the group can be written again through the cache.
    private void writeOut(Lane lane, WriteThrough.Handle handle, byte[] bytes) {
        var began = System.nanoTime();
        try {
            handle.write(bytes, lane.length, bytes.length, lane.start);
            lane.took = Math.max(1, System.nanoTime() - began);
        } catch (ClosedByInterruptException exception) {
            var _ = Thread.interrupted();
            lane.interrupted = true;
            lane.failure = exception;
        } catch (IOException exception) {
            lane.failure = exception;
        } catch (RuntimeException | LinkageError throwable) {
            lane.failure = new IOException("A log group could not be written through", throwable);
        }
    }

    /// Settles the write of the group laid out in `lane`: publishes it once every group laid out before it is
    /// published, or, if the write failed, has it written again ([#settle(Lane)]). Holding the lane lock.
    private void complete(Lane lane) {
        if (lane.failure == null) {
            lane.state = WRITTEN;
            if (failure == null) {
                // A write beside another says nothing of the time of one alone, which the wait for woken commits takes
                if (!lane.overlapped && forces > 0) {
                    averageForce(lane.took);
                }
                forces++;
                writtenThrough++;
                if (next == null) {
                    prepareNext();
                } else {
                    fillNext();
                }
                publishReady();
            }
        } else {
            lane.state = FAILED;
            laneChanged.signalAll();
            settle(lane);
        }
        laneChanged.signalAll();
    }

    /// Writes the group of `lane`, whose write through failed, again into the cache and forces it, once the other
    /// lane's write is no longer in flight, then publishes what is durable; a group of the other lane that failed as
    /// well is written again with it.
    ///
    /// As [#writeThrough()] does for a group alone, the write makes each page of the group dirty again whatever the
    /// failed write left on the device, so a force that succeeds makes it durable, and one that fails stops the writer.
    /// No write through is in flight beside them, so no write of the cache shares a page with one. A failure other than
    /// an interrupt moves the writer to forces and turns groups beside one another off for good; an interrupt only
    /// closes the lane's handle, which the lane's next group opens again. Holding the lane lock.
    private void settle(Lane lane) {
        var other = lane == first ? second : first;
        while (other.state == WRITING) {
            laneChanged.awaitUninterruptibly();
        }
        // The other lane settled both, or the writer stopped meanwhile
        if (lane.state != FAILED || failure != null) {
            return;
        }
        try {
            var firstFailed = first.state == FAILED;
            var secondFailed = second.state == FAILED;
            if ((firstFailed && !first.interrupted) || (secondFailed && !second.interrupted)) {
                writeThrough.disable();
                overlapBroken = true;
                overlapOn = false;
                throughWanted = false;
                closeQuietly(throughFile);
                throughFile = null;
                closeQuietly(laneHandle);
                laneHandle = null;
            } else {
                if (firstFailed) {
                    closeQuietly(throughFile);
                    throughFile = null;
                }
                if (secondFailed) {
                    closeQuietly(laneHandle);
                    laneHandle = null;
                }
            }
            if (firstFailed) {
                rewrite(output, first);
            }
            if (secondFailed) {
                rewrite(laneOutput, second);
            }
            channel.force(false);
            forces++;
            if (firstFailed) {
                first.failure = null;
                first.state = WRITTEN;
            }
            if (secondFailed) {
                second.failure = null;
                second.state = WRITTEN;
            }
        } catch (IOException exception) {
            stop(exception);
            return;
        } catch (RuntimeException | Error throwable) {
            stop(new IOException("The log writer failed", throwable));
            return;
        }
        publishReady();
    }

    /// Writes the bytes of the group of `lane`, held by `bytes`, into the cache where they belong.
    private void rewrite(byte[] bytes, Lane lane) throws IOException {
        var buffer = ByteBuffer.wrap(bytes, 0, lane.length);
        var at = lane.start;
        while (buffer.hasRemaining()) {
            at += channel.write(buffer, at);
        }
    }

    /// Publishes the groups at the front of the lanes that are durable, in the order they were laid out: a group whose
    /// write ended before the write of the group laid out ahead of it waits for that one. Holding the lane lock; a
    /// stopped writer publishes nothing more.
    private void publishReady() {
        while (failure == null) {
            var lane = front();
            if (lane == null || lane.state != WRITTEN) {
                return;
            }
            publishLane(lane);
        }
    }

    /// {@return the lane whose group was laid out first of those not published, or `null` if both are idle}
    private Lane front() {
        if (first.state == IDLE) {
            return second.state == IDLE ? null : second;
        }
        return second.state == IDLE || first.ticket < second.ticket ? first : second;
    }

    /// Makes the group of `lane`, durable and the first not published, the last published one, and the lane idle.
    /// Holding the lane lock.
    private void publishLane(Lane lane) {
        // A group written with the lane lock held moved it itself
        if (lane.open) {
            forcedPosition = lane.start + lane.length;
        }
        groupStart = lane.start;
        groupEnd = lane.frameEnd;
        lastThrough = lane.through;
        boolean roomy;
        queueLock.lock();
        try {
            pendingBytes -= lane.bodies;
            roomy = pendingBytes < QUEUE_LIMIT / 2;
            queueDrained.signalAll();
        } finally {
            queueLock.unlock();
        }
        lane.state = IDLE;
        var main = Thread.currentThread() != laneThread;
        var woken = publish(lane.sequence, lane.frames, roomy, lane.leading, main, lane.through);
        cohortLeft = Math.min(maxCohort, cohortLeft + woken);
        account(lane, woken);
        laneChanged.signalAll();
    }

    /// Counts a published group toward the epoch of [Overlap#AUTO], whose end decides whether groups go beside one
    /// another in the next one ([OverlapGate]). Holding the lane lock.
    private void account(Lane lane, int woken) {
        if (gate != null && !overlapBroken) {
            overlapOn = gate.published(lane.sequence, lane.through, woken);
        }
    }

    /// Starts the thread of the second lane, unless it runs or the writer closes. Holding the lane lock.
    ///
    /// A platform thread of its own: a thread of the pool, which a store that owns its pool has one of, would wait
    /// behind the flush whose write it should go beside, and a virtual thread's write through on Windows holds its
    /// carrier.
    private void startLane() {
        if (laneThread != null || laneStop) {
            return;
        }
        var thread = Thread.ofPlatform().daemon().name("ledger-log-lane").unstarted(this::laneLoop);
        laneThread = thread;
        thread.start();
    }

    /// Wakes the thread of the second lane if it waits for frames to write beside the group of [#first].
    private void wakeLane() {
        if (laneWanted) {
            var thread = laneThread;
            if (thread != null) {
                LockSupport.unpark(thread);
            }
        }
    }

    /// Runs the second lane until a close stops it, or until it has written nothing for [#LANE_IDLE], when it gives
    /// its output, its buffer and its handle back and its thread ends; the next group that may have another beside it
    /// starts one again.
    private void laneLoop() {
        var idleSince = System.nanoTime();
        while (!laneStop) {
            boolean wrote;
            try {
                wrote = runLane();
            } catch (RuntimeException | Error throwable) {
                stop(new IOException("The log writer failed", throwable));
                wrote = false;
            }
            var now = System.nanoTime();
            if (wrote) {
                idleSince = now;
                continue;
            }
            if (now - idleSince >= LANE_IDLE) {
                laneLock.lock();
                try {
                    if (second.state == IDLE) {
                        laneThread = null;
                        laneWanted = false;
                        laneOutput = null;
                        closeQuietly(laneHandle);
                        laneHandle = null;
                        writeThrough.release(1);
                        return;
                    }
                } finally {
                    laneLock.unlock();
                }
            }
            LockSupport.parkNanos(this, LANE_IDLE);
        }
    }

    /// Takes the queued frames as a group and writes it through beside the group [#first] writes, on the thread of the
    /// second lane, if [#admitsBeside] admits them, after waiting for the commits woken to queue theirs.
    ///
    /// @return whether it wrote a group
    private boolean runLane() {
        laneWanted = false;
        if (overlapOn) {
            gather(first);
        }
        byte[] bytes;
        WriteThrough.Handle handle;
        laneLock.lock();
        try {
            if (!overlapOn || overlapBroken || failure != null || closing || second.state != IDLE || first.state != WRITING
                    || !first.overlappable) {
                return false;
            }
            if (laneHandle == null) {
                try {
                    laneHandle = writeThrough.open(segmentPath, 1);
                } catch (IOException | RuntimeException | LinkageError _) {
                    // The writes of the first lane go on as they are; only a second handle is refused
                    overlapBroken = true;
                    overlapOn = false;
                    return false;
                }
            }
            ArrayList<Queued> group;
            long sequence;
            long frames;
            queueLock.lock();
            try {
                if (queue.isEmpty() || !admitsBeside(first, queue, queuedUnits, false)) {
                    // Frames that cannot go beside it now never can once more join them, while an empty queue may yet
                    laneWanted = queue.isEmpty();
                    return false;
                }
                group = queue;
                sequence = lastQueued;
                frames = appended;
                queue = laneSpare;
                takenFrames = frames;
            } finally {
                queueLock.unlock();
            }
            cohortLeft = Math.max(0, cohortLeft - group.size());
            try {
                layoutLane(group, sequence, frames);
            } catch (IOException exception) {
                stop(exception);
                return false;
            } finally {
                group.clear();
                laneSpare = group;
            }
            bytes = laneOutput;
            handle = laneHandle;
        } finally {
            laneLock.unlock();
        }
        writeOut(second, handle, bytes);
        laneLock.lock();
        try {
            complete(second);
        } finally {
            laneLock.unlock();
        }
        return true;
    }

    /// Seals `group` into the output of the second lane, at the block after the group of [#first], its first frame
    /// naming the distance back to that group, and lays it out in [#second], in the state [#WRITING]. Holding the lane
    /// lock.
    ///
    /// @param sequence the commit up to which the group makes the log durable
    /// @param frames the frames queued up to its last one
    private void layoutLane(List<Queued> group, long sequence, long frames) throws IOException {
        if (laneOutput == null || (laneSmallGroups >= SHRINK_AFTER && laneOutput.length > MIN_OUTPUT)) {
            laneOutput = new byte[MIN_OUTPUT];
        }
        if (waitedGroup) {
            waitedGroup = false;
            waitedGroups++;
        }
        // The output of the first lane holds its group, which the sealing below must leave alone
        var firstOutput = output;
        var firstUsed = used;
        output = laneOutput;
        used = 0;
        var start = position;
        overlapBack = (int) ((start - first.start) / ALIGNMENT);
        try {
            var bodies = 0L;
            for (var frame : group) {
                bodies += frame.body().size();
            }
            var groupStart = true;
            for (int index = 0, count = group.size(); index < count; ) {
                var frame = group.get(index);
                var bodyLength = frame.body().size();
                if (bodyLength > RUN_MAX_BODY) {
                    seal(frame, bodyLength + sealOverhead(), groupStart);
                    index++;
                } else {
                    var end = runEnd(group, index);
                    sealRun(group, index, end, groupStart);
                    index = end;
                }
                groupStart = false;
            }
            var end = position + used;
            var padding = (int) ((end + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT - end);
            room(padding);
            Arrays.fill(output, used, used + padding, (byte) 0);
            used += padding;
            laneOutput = output;
            laneSmallGroups = used <= MIN_OUTPUT / 2 ? laneSmallGroups + 1 : 0;
            var lane = second;
            lane.ticket = ++tickets;
            lane.start = start;
            lane.length = used;
            lane.frameEnd = end;
            lane.sequence = sequence;
            lane.frames = frames;
            lane.bodies = bodies;
            lane.leading = false;
            lane.through = true;
            lane.overlappable = true;
            lane.open = true;
            lane.overlapped = true;
            lane.failure = null;
            lane.interrupted = false;
            lane.state = WRITING;
            first.overlapped = true;
            overlappedGroups++;
            position = start + used;
        } finally {
            overlapBack = 0;
            output = firstOutput;
            used = firstUsed;
        }
    }

    /// Ends the thread of the second lane and waits for it, so it writes nothing once this returns.
    private void stopLane() {
        laneStop = true;
        var thread = laneThread;
        if (thread == null) {
            return;
        }
        LockSupport.unpark(thread);
        var interrupted = false;
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException _) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Whether a group written through may go to the device while the group before it is still there, a second lane
    /// writing it from a thread and a handle of its own.
    ///
    /// Two writes in flight at once pay where the device serves them together, and where the commits a group wakes
    /// come back while the other lane's group is on the device; where it takes one at a time, a group split in two
    /// takes two of its round trips for the commits one took.
    enum Overlap {
        /// As measured: groups go beside one another in epochs whose rate of commits beat the epoch on one lane before
        /// them, and back to one lane otherwise ([OverlapGate]). The default.
        AUTO,
        /// One group at a time.
        OFF,
        /// Beside one another wherever they may.
        ON;

        /// {@return the mode the system property `ledger.log.overlap` names, [#AUTO] if it names none}
        ///
        /// @throws IllegalArgumentException if it names no mode
        static Overlap fromProperty() {
            var value = System.getProperty("ledger.log.overlap");
            if (value == null || value.isBlank()) {
                return AUTO;
            }
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("ledger.log.overlap must be auto, off or on: " + value, exception);
            }
        }
    }

    /// A group laid out and not yet published, with what its publication needs; a lane holds one at a time.
    private static final class Lane {
        // IDLE, WRITING, WRITTEN or FAILED; written under the lane lock
        volatile int state;
        // The order it was laid out in, among the groups of both lanes
        long ticket;
        // Where its bytes start in the segment, a block boundary, and how many, its padding included; where its frames
        // end
        long start;
        int length;
        long frameEnd;
        // The commit up to which, and the frames queued up to which, it makes the log durable, and the bytes of their
        // bodies
        long sequence;
        long frames;
        long bodies;
        // Whether it is a commit's own group, whose thread comes back too
        boolean leading;
        // Whether it was written through
        boolean through;
        // Whether a group may go beside it: written through, with no units, not a commit's own nor its use's first
        boolean overlappable;
        // Whether it went to the device with the lane lock let go, so its publication moves forcedPosition
        boolean open;
        // Whether another group was on the device beside it
        boolean overlapped;
        // How long its write took, in nanoseconds
        long took;
        // Why its write failed, and whether an interrupt closed its handle, or null
        IOException failure;
        boolean interrupted;
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
        waitedGroup = true;
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
        if (cohortArmed) {
            registered.increment();
        }
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

    /// {@return the heap the commits queued since the writer opened hold until a checkpoint takes them, estimated: the
    /// bytes of their frames and units, [#WRITE_HEAP] a write and [#COMMIT_HEAP] a commit}
    long appendedHeap() {
        return appendedHeap;
    }

    /// {@return the heap a commit of `writes` writes, whose frame body is `bodyBytes` and whose units are `unitBytes`,
    /// holds until a checkpoint takes it, as [#appendedHeap()] estimates it}
    ///
    /// @param sharesChunk whether the commit's values are on one chunk of the [ValueSlab], whose view the slab shares
    ///                    among the commits on the chunk and whose whole the slab's own estimate counts: such a commit
    ///                    allocates none of what [#COMMIT_HEAP] counts
    static long heapOf(long bodyBytes, long unitBytes, int writes, boolean sharesChunk) {
        return 13 + bodyBytes + unitBytes + (long) writes * WRITE_HEAP + (writes > 0 && !sharesChunk ? COMMIT_HEAP : 0);
    }

    /// {@return the offset in the current segment where the frames of the last published group end, before their
    /// padding}
    long groupEnd() {
        return groupEnd;
    }

    /// {@return the offset in the current segment where the frames of the last published group start, a block boundary}
    long groupStart() {
        return groupStart;
    }

    /// {@return the forces made so far, a group written through counting as one}
    long forces() {
        return forces;
    }

    /// {@return whether the work on the segment after the current one, if any was started, is over, for tests}
    boolean nextSettled() {
        var preparation = next;
        return preparation == null || preparation.isDone();
    }

    /// {@return the groups written through so far, for tests}
    long writtenThrough() {
        return writtenThrough;
    }

    /// {@return the groups written while the group before them was not durable yet, for tests}
    long overlappedGroups() {
        return overlappedGroups;
    }

    /// {@return whether groups go beside one another now, for tests}
    boolean overlapping() {
        return overlapOn;
    }

    /// {@return whether the group of the second lane, if `second`, or of the flush, is durable and waits for the group
    /// laid out before it to be published, for tests}
    boolean laneWritten(boolean second) {
        return (second ? this.second : first).state == WRITTEN;
    }

    /// {@return whether the second lane has a thread, for tests}
    boolean laneRunning() {
        return laneThread != null;
    }

    /// {@return the commits waiting for durability, for tests}
    int waitingForTests() {
        return waiters.size();
    }

    /// {@return the way later segments are opened to write through, or `null` if their groups are forced, for tests}
    WriteThrough.Kind writeThroughKind() {
        return writeThrough.kind();
    }

    /// {@return the waits for the commits a force woke that ended with all of them back, for tests}
    long cohortsGathered() {
        return cohortsGathered;
    }

    /// {@return the waits for the commits a force woke that ran out, for tests}
    long cohortsMissed() {
        return cohortsMissed;
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
    /// An interrupt of a commit writing its own group that closes the channel meanwhile has the group written again
    /// through a new one, then forced: the interrupt may hide a failure of the force, after which the file system may
    /// report the next force a success with the group not on disk, so the pages are made dirty again first. A group of
    /// the pool's flush, which one write may not hold, fails as any failed force does.
    private void force() throws IOException {
        while (true) {
            try {
                channel.force(false);
                forcedPosition = position;
                return;
            } catch (ClosedByInterruptException exception) {
                if (!leading) {
                    throw exception;
                }
                reopen();
                rewriteLastDrain();
            }
        }
    }

    /// Writes the bytes of the last drain again, where it wrote them; they are still in the output.
    private void rewriteLastDrain() throws IOException {
        while (true) {
            try {
                var buffer = ByteBuffer.wrap(output, 0, lastDrainLength);
                var at = lastDrainStart;
                while (buffer.hasRemaining()) {
                    at += channel.write(buffer, at);
                }
                return;
            } catch (ClosedByInterruptException _) {
                reopen();
            }
        }
    }

    /// Closes the channel of the current segment and its handles to write through, with no write in flight.
    private void closeSegment() throws IOException {
        var handle = throughFile;
        throughFile = null;
        closeQuietly(laneHandle);
        laneHandle = null;
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
        // No flush takes a group any more: everything queued is durable, or the writer stopped, which may leave a write
        // of a lane to return
        stopLane();
        laneLock.lock();
        try {
            while (first.state == WRITING || second.state == WRITING) {
                laneChanged.awaitUninterruptibly();
            }
            closeSegment();
        } catch (IOException _) {
            // The data is durable already; nothing to recover from a failed close
        } finally {
            laneLock.unlock();
        }
    }

}

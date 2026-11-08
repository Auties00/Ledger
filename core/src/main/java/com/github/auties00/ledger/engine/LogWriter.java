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
import java.util.concurrent.Callable;
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
/// A force of the pool's flush that has frames to seal behind it runs on a forcing thread of the writer's own while the
/// flush seals ahead what follows it: at a rollover, the rest of the group into the segment after it; at the end of a
/// group holding at least [#AHEAD_FROM] bytes of frames queued behind it, or commits waiting for room in the queue, the
/// next group, taken off the queue as it comes. What is sealed ahead is held in arrays of its own until the force
/// returns, and the bytes of a group reach the file only once every earlier group's force has returned: recovery reads
/// the first frame of a later group, or of a later segment, as a force having made everything before it durable. An
/// ingest seals while the device forces, so the seal adds nothing to the forces' time. A commit that writes its own
/// group, a group written through, and a force with nothing queued behind it stay on the flush's thread: a hand-off
/// costs two thread wakes, more than the seal it would hide.
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
///                 16  frame type, flags (1: first frame after a force)
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
    /// The bytes of the bodies of frames queued and not sealed yet past which commits wait for the device: a sealed
    /// frame's body is let go, its bytes are in the output or held ahead.
    private static final long QUEUE_LIMIT = 64L << 20;
    /// The bytes of each array frames are sealed ahead into: below half a G1 region of a heap of any size, and one of
    /// the sizes the heap pool recycles. A frame of at most [#RUN_MAX_BODY] bytes and a run, which never split, fit.
    private static final int AHEAD_ARRAY = 256 << 10;
    /// The bytes an array sealed ahead into may leave unfilled at its end: more than a frame header, which never
    /// splits, and than what a slice or a tag that does not fit leaves.
    private static final int AHEAD_SLACK = 64;
    /// The bytes of frames queued behind a group past which its force runs on the forcing thread while the flush seals
    /// them: sealing 256 KiB takes about 200 microseconds, a hand-off of the force about a tenth of that.
    static final long AHEAD_FROM = 256 << 10;
    /// How long the forcing thread waits for its next force before it ends, so an idle store keeps no thread for it.
    private static final long FORCER_IDLE = TimeUnit.SECONDS.toNanos(1);
    /// What a force on the forcing thread returns when it completes.
    private static final Object FORCED = new Object();
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
    // The pool the arrays sealed ahead into come from and go back to once a flush ends
    private final HeapPool heap;
    // The bytes queued behind a group from which its force runs on the forcing thread
    private final long aheadFrom;
    // The most bytes held ahead: a segment's frames, or a thirty-second of the heap if less
    private final long aheadLimit;

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
    // The end of the bytes of the current segment a force or a write through made durable
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
    // Whether a commit waited since the last group began to be written
    private volatile boolean waitedGroup;
    // The groups written since the store opened that a commit waited for; owned by the flush task
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
    // The emptied queue, which the next swap reuses
    private ArrayList<Queued> flushing;
    // The emptied queue a take of the next group while a force runs reuses
    private ArrayList<Queued> aheadTaken;
    // The bytes of the bodies of the frames sealed since the queue's count last let them go; owned by the flush task
    private long sealedBodies;
    // Whether seal() holds what it seals in `held` rather than writing it: while a force runs on the forcing thread,
    // the flush seals ahead, into arrays that reach the file once the force returns
    private boolean holding;
    // The arrays sealed ahead, in order, from heldStart to heldEnd of their segment; heldLengths holds the bytes of each
    private final ArrayList<byte[]> held;
    private int[] heldLengths;
    private long heldStart;
    private long heldEnd;
    // Free arrays of AHEAD_ARRAY bytes to seal ahead into, which a flush gives back to the pool when it ends
    private final ArrayList<byte[]> spare;
    // What sealing ahead set aside until it ends: the output, where it is written, and the key and tags of its segment
    private byte[] asideOutput;
    private long asidePosition;
    private SecretKeySpec asideKey;
    private long asideSegmentTag;
    private long asideUseTag;
    // The next group of a flush of the pool, taken off the queue and sealed in part while the force of the group before
    // it ran, and the instance the group being written uses; owned by the flush task
    private NextGroup nextGroup;
    private NextGroup writtenGroup;
    // The flush task's thread while it waits, sealing ahead, for a frame to be queued or a segment to be prepared
    private volatile Thread aheadWaiter;
    // The frames sealed ahead so far, for tests; written by the flush task only
    private volatile long sealedAhead;
    // The thread that forces a segment while the flush seals ahead, started with the first such force and ending once
    // idle for FORCER_IDLE or at close, or null; the channel it forces next, or null; whether close ended it
    private final ReentrantLock forcerLock;
    private final Condition forceRequested;
    private Thread forcer;
    private FileChannel forceTarget;
    private boolean forcerStopped;
    // What the last force on the forcing thread returned, FORCED or what it threw, or null while it runs; the thread
    // it wakes then
    private volatile Object forceOutcome;
    private volatile Thread forceWaiter;
    // Written by the flush task only
    private volatile long forces;
    // The groups written through, of the forces; written by the flush task only
    private volatile long writtenThrough;
    // The segment the flush task writes, for a checkpoint to read
    private volatile long currentSegment;

    private final ReentrantLock queueLock;
    // Signalled when a group is durable, for the commits waiting on a full queue
    private final Condition queueDrained;
    private ArrayList<Queued> queue;
    // The bytes of the frame bodies queued and not sealed yet
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
    /// @param commitWindow how long a flush waits for more commits before it starts
    /// @param empty the segment numbered `firstSegment`, which recovery found prepared and never written, to write
    ///              first; or `null`, to prepare one
    /// @param writeThrough how the segments are opened to write groups through, which the writer owns from now on
    /// @param heap the pool of the arrays frames are sealed ahead into
    /// @param aheadFrom the bytes of frames queued behind a group from which its force runs on the forcing thread while
    ///                  the flush seals them, [#AHEAD_FROM] but in tests
    /// @throws IOException if the segment cannot be prepared
    LogWriter(LogFiles files, StoreKeys keys, byte[] storeId, long firstSegment, long durable, long segmentSize,
              Executor executor, Duration commitWindow, LogFiles.Prepared empty, WriteThrough writeThrough, HeapPool heap,
              long aheadFrom) throws IOException {
        this.files = files;
        this.keys = keys;
        this.storeId = storeId;
        this.segmentSize = segmentSize;
        this.executor = executor;
        this.commitWindowNanos = commitWindow.toNanos();
        this.writeThrough = writeThrough;
        this.heap = heap;
        this.aheadFrom = aheadFrom;
        this.aheadLimit = Math.min(segmentSize - SEGMENT_HEADER, Runtime.getRuntime().maxMemory() / 32);
        try {
            this.cipher = keys.encrypted() ? Cipher.getInstance("AES/GCM/NoPadding") : null;
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("AES-GCM is unavailable", exception);
        }
        this.crc = new CRC32C();
        this.output = new byte[MIN_OUTPUT];
        this.flushing = new ArrayList<>();
        this.aheadTaken = new ArrayList<>();
        this.held = new ArrayList<>();
        this.heldLengths = new int[8];
        this.spare = new ArrayList<>();
        this.nextGroup = new NextGroup();
        this.writtenGroup = new NextGroup();
        this.forcerLock = new ReentrantLock();
        this.forceRequested = forcerLock.newCondition();
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

    /// Starts writing `segment`, under a use of a tag of its own.
    private void open(LogFiles.Prepared segment) throws IOException {
        var useTag = TAGS.nextLong();
        open(segment, useTag, keyOf(segment, useTag));
    }

    /// Starts writing `segment` under the use of `useTag`, whose key is `key`, drawn while a force ran so frames could be
    /// sealed ahead into it.
    private void open(LogFiles.Prepared segment, long useTag, SecretKeySpec key) throws IOException {
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
        this.segmentTag = tagOf(segment);
        this.useTag = useTag;
        this.segmentKey = key;
        this.next = null;
        this.throughFile = null;
        // A segment of its header and first block holds at most one group through, which is worth a handle only as the
        // first of a store just opened, which may commit once
        this.throughWanted = valid > KEPT_LENGTH || (first && segment.nameDurable());
    }

    /// {@return the tag of `segment` its frame headers carry: the first 8 bytes of its salt}
    private static long tagOf(LogFiles.Prepared segment) {
        return ByteBuffer.wrap(segment.salt()).getLong();
    }

    /// {@return the key of the use of `segment` of `useTag`, or `null` if the frames are not encrypted}
    private SecretKeySpec keyOf(LogFiles.Prepared segment, long useTag) {
        return keys.encrypted() ? keys.segmentKey(storeId, segment.number(), segment.salt(), useTag) : null;
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
    /// behind it would make every commit waiting on that flush wait as long. Its end wakes a flush that seals ahead
    /// while it waits for the segment.
    private void prepareNext() {
        var number = segmentNumber + 1;
        var zeros = wantsZeros();
        // On the thread of the preparation: the answer of a device may take a round trip to it
        var preparation = preparation(() -> files.prepare(number, segmentSize,
                zeros && !closing && files.zeroFills(writeThrough.enabled()), writeThrough.kind() != null, () -> closing));
        next = preparation;
        nextZeroed = zeros;
        Thread.ofPlatform().daemon().name("ledger-prepare").start(preparation);
    }

    /// {@return a task of the segment after the current one, whose end wakes a flush that seals ahead while it waits}
    private FutureTask<LogFiles.Prepared> preparation(Callable<LogFiles.Prepared> work) {
        return new FutureTask<>(work) {
            @Override
            protected void done() {
                wakeAhead();
            }
        };
    }

    /// Prepares the segment after the current one now, rather than once the group is written, if the bytes held ahead
    /// and the frames of `group` from `from` go past the end of the current one: the rollover that ends it then finds
    /// it prepared, and seals ahead into it while the full segment's force runs.
    private void prepareIfBeyond(List<Queued> group, int from) {
        if (next != null) {
            return;
        }
        var room = segmentSize - position - (held.isEmpty() ? 0 : heldEnd - heldStart);
        for (var index = from; index < group.size(); index++) {
            room -= FRAME_HEADER + group.get(index).body().size() + sealOverhead();
            if (room < 0) {
                prepareNext();
                return;
            }
        }
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
        var fill = preparation(() -> {
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
    /// more than [#QUEUE_LIMIT] bytes not sealed yet, it waits for the device, which holds back every commit behind
    /// it: memory stays bounded when commits come faster than the device takes them. A frame larger than the limit
    /// waits until every frame queued before it is sealed.
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
        wakeAhead();
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
        wakeAhead();
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
        wakeAhead();
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::flush);
        }
        return frames;
    }

    /// Wakes the flush task if it waits, sealing ahead, for a frame to be queued or a segment to be prepared.
    private void wakeAhead() {
        var waiter = aheadWaiter;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }
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

    /// Writes the queued groups until the queue is empty or the commit at `until` is durable, holding [#running].
    ///
    /// A flush of the pool returns only once the next group it took off the queue during a force is written and forced
    /// too: its frames left the queue, and their commits would wait for a flush that never comes.
    private void flushGroups(long until) {
        leading = until != Long.MAX_VALUE;
        try {
            // A stopped writer writes nothing more: a group after one whose force failed would pass for durable while
            // the failed one, maybe partly on disk, stands before it
            while (durable < until && failure == null) {
                if (commitWindowNanos > 0) {
                    LockSupport.parkNanos(commitWindowNanos);
                }
                var carried = nextGroup.frames.isEmpty() ? null : nextGroup;
                ArrayList<Queued> taken = null;
                long sequence;
                long frames;
                boolean units;
                queueLock.lock();
                try {
                    if (carried == null && (queue.isEmpty() || (until != Long.MAX_VALUE && (lastQueued > until || !fitsOneWrite(queue))))) {
                        return;
                    }
                    units = carried != null && carried.units;
                    if (queue.isEmpty()) {
                        sequence = carried.sequence;
                        frames = carried.frameCount;
                    } else {
                        taken = queue;
                        sequence = lastQueued;
                        frames = appended;
                        units |= queuedUnits;
                        queuedUnits = false;
                        queue = flushing;
                    }
                } finally {
                    queueLock.unlock();
                }
                if (carried == null) {
                    write(taken, 0, units, null);
                    taken.clear();
                    flushing = taken;
                } else {
                    // What was queued since joins the group; the group after it is taken into the other instance
                    nextGroup = writtenGroup;
                    writtenGroup = carried;
                    if (taken != null) {
                        carried.frames.addAll(taken);
                        taken.clear();
                        flushing = taken;
                    }
                    write(carried.frames, carried.sealed, units, carried);
                    carried.clear();
                }
                if (sealedBodies > 0) {
                    // The bodies of the next group sealed ahead during the force
                    release();
                }
                publish(sequence, frames);
            }
        } catch (IOException exception) {
            stop(exception);
        } catch (RuntimeException | Error throwable) {
            stop(new IOException("The log writer failed", throwable));
        } finally {
            leading = false;
            if (failure != null) {
                abandonAhead();
            }
            while (!spare.isEmpty()) {
                heap.release(spare.removeLast());
            }
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

    /// Seals and writes the frames of `group` from `from`, then forces them, and the file of large values if `units`; or
    /// writes them through, if [#writesThrough(List)], and forces only that file.
    ///
    /// The frames before `from` were sealed ahead while the force of the group before ran, as `carried` holds them:
    /// they are written first, at the header of the next segment if `carried` says the group starts it, and the frames
    /// sealed are let go from `group`.
    ///
    /// The commits of the group wrote their units before they queued their frames, so one force of that file, run
    /// beside the segment's write and force, makes them all durable before the group is. A rollover waits for it
    /// before it forces the segment: the first frame of a group in the next segment then means, as it always does,
    /// that everything before it is durable.
    private void write(ArrayList<Queued> group, int from, boolean units, NextGroup carried) throws IOException {
        if (carried != null && carried.segment != null) {
            // The force of the group before made the current segment durable whole: the group starts the next one with
            // no force of its own
            closeSegment();
            open(carried.segment, carried.useTag, carried.key);
            prepareIfBeyond(group, from);
        } else if (channel == null) {
            rollover();
        } else if (from == 0 && rollsEarly()) {
            // Not with frames sealed ahead, which follow the group before in the current segment under its key
            rollEarly();
        }
        // Between groups: the last drain's bytes, which an interrupted force writes again, are durable by now
        if (smallGroups >= SHRINK_AFTER && output.length > MIN_OUTPUT) {
            output = new byte[MIN_OUTPUT];
        }
        if (waitedGroup) {
            waitedGroup = false;
            waitedGroups++;
        }
        // Before a frame is sealed, while the output holds no byte of the group; frames sealed ahead take a write of
        // their own, so a group that has some is forced
        var through = from == 0 && writesThrough(group);
        var unitForce = units ? forceUnits() : null;
        var start = position;
        writeHeld();
        var groupStart = from == 0;
        var count = group.size();
        for (var index = from; index < count; ) {
            var bodyLength = group.get(index).body().size();
            var sealedLength = bodyLength + sealOverhead();
            // The test of the frame alone: a run takes the commits after its first only while it fits in the segment
            if (position + used + FRAME_HEADER + sealedLength > segmentSize && position + used > SEGMENT_HEADER) {
                drain();
                if (unitForce != null) {
                    await(unitForce);
                    unitForce = null;
                }
                var sealed = rollover(group, index);
                index += sealed;
                start = SEGMENT_HEADER;
                groupStart = sealed == 0;
                continue;
            }
            var end = frameEnd(group, index);
            sealFrames(group, index, end, groupStart);
            index = end;
            groupStart = false;
        }
        // Zeros to the next block, where the next group starts: a reused file holds older bytes there
        var end = position + used;
        var padding = (int) ((end + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT - end);
        room(padding);
        Arrays.fill(output, used, used + padding, (byte) 0);
        used += padding;
        smallGroups = position + used - start <= MIN_OUTPUT / 2 ? smallGroups + 1 : 0;
        if (through) {
            release();
            writeThrough();
        } else {
            drain();
            if (release() && !leading && startForce()) {
                Object outcome;
                try {
                    sealNextGroup();
                } finally {
                    outcome = joinForce();
                }
                checkForced(outcome);
            } else {
                force();
            }
        }
        forces++;
        if (unitForce != null) {
            await(unitForce);
        }
        this.groupStart = start;
        groupEnd = end;
        if (next == null) {
            prepareNext();
        } else if (nextGroup.segment == null) {
            // Not into a segment the next group sealed ahead for: its frames would land beside the zeros
            fillNext();
        }
    }

    /// Forces the full segment and switches to the next one, prepared, for the frames of `group` from `from`.
    ///
    /// On a flush of the pool the force runs on the forcing thread while those frames are sealed ahead into the next
    /// segment, once its preparation is done: none of them reaches the next segment before the force returns.
    ///
    /// @return the frames of `group` from `from` sealed ahead, which are written into the new segment
    private int rollover(ArrayList<Queued> group, int from) throws IOException {
        // The group's frames sealed so far are let go: the commits waiting for room queue the next group meanwhile
        release();
        var sealed = 0;
        LogFiles.Prepared prepared = null;
        var tag = 0L;
        SecretKeySpec key = null;
        if (!leading && startForce()) {
            Object outcome;
            try {
                prepared = awaitPrepared();
                if (prepared != null) {
                    tag = TAGS.nextLong();
                    key = keyOf(prepared, tag);
                    if (beginAhead(SEGMENT_HEADER, tagOf(prepared), tag, key)) {
                        try {
                            while (from + sealed < group.size() && forceOutcome == null) {
                                var end = sealAhead(group, from + sealed, sealed == 0);
                                if (end < 0) {
                                    break;
                                }
                                sealed = end - from;
                            }
                        } finally {
                            endAhead();
                        }
                    }
                }
            } finally {
                outcome = joinForce();
            }
            checkForced(outcome);
        } else {
            force();
        }
        forces++;
        closeSegment();
        if (prepared != null) {
            open(prepared, tag, key);
        } else {
            // Reports the preparation's failure, or prepares the segment here
            rollover();
        }
        prepareIfBeyond(group, from + sealed);
        writeHeld();
        return sealed;
    }

    /// {@return the segment after the current one once its preparation is done; or `null` if it failed, which
    /// [#rollover()] then reports}
    ///
    /// Waits while the full segment's force runs, through any interrupt: the rollover after the force would wait as long.
    /// The preparation runs on its own thread, which may not have started yet, rather than here as a rollover runs it:
    /// the wait costs nothing beside the force, and the file operations of an interrupted thread fail.
    private LogFiles.Prepared awaitPrepared() {
        if (next == null) {
            prepareNext();
        }
        var preparation = next;
        while (true) {
            try {
                return preparation.get();
            } catch (InterruptedException _) {
                interruptedWrite = true;
            } catch (ExecutionException _) {
                return null;
            }
        }
    }

    /// Seals ahead the next group while the force of the group just written runs on the forcing thread: the frames
    /// queued, taken off the queue as they come, until the force returns, they fill what may be held ahead, or the next
    /// one does not fit in its segment.
    ///
    /// A group whose first frame does not fit in the current segment starts the next one, once its preparation is done:
    /// the force running makes the current segment durable whole, so the group needs no rollover force of its own.
    private void sealNextGroup() throws IOException {
        var group = nextGroup;
        try {
            while (forceOutcome == null) {
                if (group.sealed == group.frames.size()) {
                    var seen = take(group);
                    if (seen >= 0) {
                        awaitAhead(() -> appendedBytes != seen);
                        continue;
                    }
                }
                if (!holding && !beginNextGroup(group, group.frames.get(group.sealed))) {
                    return;
                }
                var end = sealAhead(group.frames, group.sealed, group.sealed == 0);
                if (end < 0) {
                    return;
                }
                group.sealed = end;
            }
        } finally {
            if (holding) {
                endAhead();
            }
        }
    }

    /// Takes everything queued into `group`, whose publication then covers it.
    ///
    /// @return -1 if it took frames, else the bytes queued since the writer opened, which the next frame queued changes
    private long take(NextGroup group) {
        ArrayList<Queued> taken;
        queueLock.lock();
        try {
            if (queue.isEmpty()) {
                return appendedBytes;
            }
            taken = queue;
            group.sequence = lastQueued;
            group.frameCount = appended;
            group.units |= queuedUnits;
            queuedUnits = false;
            queue = aheadTaken;
        } finally {
            queueLock.unlock();
        }
        group.frames.addAll(taken);
        taken.clear();
        aheadTaken = taken;
        return -1;
    }

    /// Begins sealing `group` ahead where its first frame, `frame`, goes: after the group just written, or at the header
    /// of the segment after the current one if it does not fit, once that segment's preparation is done.
    ///
    /// @return whether sealing ahead began; not if the force returned before the preparation was done, the preparation
    ///         failed, or no array could be had
    private boolean beginNextGroup(NextGroup group, Queued frame) {
        if (position + FRAME_HEADER + frame.body().size() + sealOverhead() <= segmentSize || position <= SEGMENT_HEADER) {
            return beginAhead(position, segmentTag, useTag, segmentKey);
        }
        if (next == null) {
            prepareNext();
        }
        var preparation = next;
        awaitAhead(preparation::isDone);
        if (preparation.state() != Future.State.SUCCESS) {
            return false;
        }
        var prepared = preparation.resultNow();
        var tag = TAGS.nextLong();
        var key = keyOf(prepared, tag);
        if (!beginAhead(SEGMENT_HEADER, tagOf(prepared), tag, key)) {
            return false;
        }
        group.segment = prepared;
        group.useTag = tag;
        group.key = key;
        return true;
    }

    /// Parks the flush task, which seals ahead, until the force on the forcing thread returns or `ready` holds; an
    /// interrupt waits on, and [#joinForce()] takes it.
    private void awaitAhead(BooleanSupplier ready) {
        aheadWaiter = Thread.currentThread();
        try {
            while (forceOutcome == null && !ready.getAsBoolean()) {
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    interruptedWrite = true;
                }
            }
        } finally {
            aheadWaiter = null;
        }
    }

    /// Seals ahead the frames of `group` from `index` that one frame holds, unless that frame does not fit whole in its
    /// segment or in what may still be held ahead, or the arrays it needs cannot be had: a frame is sealed ahead whole or
    /// not at all, as a frame sealed once at an offset is never sealed there again.
    ///
    /// @return the end, exclusive, of the frames it sealed, or -1 if it sealed none
    private int sealAhead(ArrayList<Queued> group, int index, boolean groupStart) throws IOException {
        var length = (long) FRAME_HEADER + group.get(index).body().size() + sealOverhead();
        if (position + used + length > segmentSize && position + used > SEGMENT_HEADER) {
            return -1;
        }
        var end = frameEnd(group, index);
        var bytes = (long) FRAME_HEADER + plainBytes(group, index, end) + sealOverhead();
        if (position - heldStart + used + bytes > aheadLimit || !reserve(bytes)) {
            return -1;
        }
        sealFrames(group, index, end, groupStart);
        sealedAhead += end - index;
        return end;
    }

    /// Makes [#seal] hold what it seals, from offset `start` of the segment of the tags and the key given, until
    /// [#endAhead()]; the output, written out whole, and where it goes are set aside meanwhile.
    ///
    /// @return whether it began; not if no array could be had
    private boolean beginAhead(long start, long segmentTag, long useTag, SecretKeySpec key) {
        byte[] array;
        if (!spare.isEmpty()) {
            array = spare.removeLast();
        } else {
            try {
                array = heap.acquire(AHEAD_ARRAY);
            } catch (OutOfMemoryError _) {
                // Sealing ahead is not needed: the frames are sealed once the force returns, as without it
                return false;
            }
        }
        asideOutput = output;
        asidePosition = position;
        asideKey = segmentKey;
        asideSegmentTag = this.segmentTag;
        asideUseTag = this.useTag;
        output = array;
        used = 0;
        position = start;
        this.segmentTag = segmentTag;
        this.useTag = useTag;
        this.segmentKey = key;
        heldStart = start;
        holding = true;
        return true;
    }

    /// Ends what [#beginAhead] began: what was sealed is held until [#writeHeld()], and the output is back.
    private void endAhead() {
        if (used > 0) {
            keep(output, used);
        } else {
            spare.add(output);
        }
        heldEnd = position + used;
        output = asideOutput;
        used = 0;
        position = asidePosition;
        segmentKey = asideKey;
        segmentTag = asideSegmentTag;
        useTag = asideUseTag;
        asideOutput = null;
        asideKey = null;
        holding = false;
    }

    /// Holds the first `length` bytes of `array`, sealed ahead, after the arrays held before it.
    private void keep(byte[] array, int length) {
        if (held.size() == heldLengths.length) {
            heldLengths = Arrays.copyOf(heldLengths, 2 * heldLengths.length);
        }
        heldLengths[held.size()] = length;
        held.add(array);
    }

    /// Holds the array sealed ahead into, which has no room for what comes next, and seals on into a spare one.
    private void hold() {
        keep(output, used);
        position += used;
        // reserve() took the arrays a frame needs before it began
        output = spare.isEmpty() ? heap.acquire(AHEAD_ARRAY) : spare.removeLast();
        used = 0;
    }

    /// Takes from the pool the spare arrays sealing `bytes` more ahead may need, so a frame begun is always finished.
    ///
    /// @return whether it could
    private boolean reserve(long bytes) {
        var room = output.length - used - AHEAD_SLACK;
        var needed = bytes <= room ? 0 : (bytes - Math.max(0, room) + AHEAD_ARRAY - AHEAD_SLACK - 1) / (AHEAD_ARRAY - AHEAD_SLACK);
        try {
            while (spare.size() < needed) {
                spare.add(heap.acquire(AHEAD_ARRAY));
            }
            return true;
        } catch (OutOfMemoryError _) {
            return false;
        }
    }

    /// Writes the arrays sealed ahead, where the output would be written next, and keeps them to seal ahead into again.
    private void writeHeld() throws IOException {
        if (held.isEmpty()) {
            return;
        }
        var at = heldStart;
        for (var index = 0; index < held.size(); index++) {
            var array = held.get(index);
            var length = heldLengths[index];
            while (true) {
                try {
                    var buffer = ByteBuffer.wrap(array, 0, length);
                    var offset = at;
                    while (buffer.hasRemaining()) {
                        offset += channel.write(buffer, offset);
                    }
                    break;
                } catch (ClosedByInterruptException _) {
                    reopen();
                }
            }
            at += length;
        }
        // Only once all are written: after a failed write, abandonAhead() gives back whatever is still held
        spare.addAll(held);
        held.clear();
        position = at;
    }

    /// Drops what was sealed ahead and the next group taken, once the writer stopped: none of it may reach the file.
    private void abandonAhead() {
        for (var array : held) {
            heap.release(array);
        }
        held.clear();
        nextGroup.clear();
        writtenGroup.clear();
    }

    /// Takes the bodies of the frames sealed since it last ran off the bytes the queue counts, and wakes the commits
    /// waiting for room: a sealed frame's body is let go.
    ///
    /// @return whether a force now is worth running on the forcing thread while the flush seals ahead: at least
    ///         [#aheadFrom] bytes are queued for the next group, or commits waited for room, which they queue during
    ///         the force
    private boolean release() {
        var bodies = sealedBodies;
        sealedBodies = 0;
        queueLock.lock();
        try {
            var waiting = queueLock.hasWaiters(queueDrained);
            if (bodies > 0) {
                pendingBytes -= bodies;
                queueDrained.signalAll();
            }
            return waiting || pendingBytes >= aheadFrom;
        } finally {
            queueLock.unlock();
        }
    }

    /// Starts forcing the segment on the forcing thread, starting that thread if none runs.
    ///
    /// @return whether the force started; if not, the caller forces the segment itself
    private boolean startForce() {
        forceOutcome = null;
        forceWaiter = Thread.currentThread();
        forcerLock.lock();
        try {
            if (forcerStopped) {
                return false;
            }
            forceTarget = channel;
            if (forcer == null) {
                try {
                    forcer = Thread.ofPlatform().daemon().name("ledger-log-force").start(this::runForces);
                } catch (OutOfMemoryError _) {
                    // No thread to spare: the flush forces the segment itself
                    forceTarget = null;
                    return false;
                }
            } else {
                forceRequested.signal();
            }
            return true;
        } finally {
            forcerLock.unlock();
        }
    }

    /// Forces the segments the flush hands over, until none comes for [#FORCER_IDLE] or close ends it.
    ///
    /// A platform thread, not a virtual one: a force must start as soon as it is asked for, whatever the application's
    /// virtual threads keep busy.
    private void runForces() {
        while (true) {
            FileChannel target;
            forcerLock.lock();
            try {
                var idle = FORCER_IDLE;
                while ((target = forceTarget) == null) {
                    if (forcerStopped || idle <= 0) {
                        forcer = null;
                        return;
                    }
                    try {
                        idle = forceRequested.awaitNanos(idle);
                    } catch (InterruptedException _) {
                        // Nothing interrupts this thread: a stray interrupt must not end it with a force to come
                    }
                }
                forceTarget = null;
            } finally {
                forcerLock.unlock();
            }
            Object outcome = FORCED;
            try {
                target.force(false);
            } catch (Throwable throwable) {
                outcome = throwable;
            }
            forceOutcome = outcome;
            LockSupport.unpark(forceWaiter);
        }
    }

    /// Waits for the force [#startForce()] started, through any interrupt: nothing closes the segment, or stops the
    /// writer, while it runs.
    ///
    /// An interrupt of the flush meanwhile is taken, as [#reopen()] takes one: the force ran on a thread no interrupt
    /// reached, so it can be trusted, and a later force of the flush on its own thread must not fail for it.
    ///
    /// @return [#FORCED], or what the force threw
    private Object joinForce() {
        Object outcome;
        while ((outcome = forceOutcome) == null) {
            LockSupport.park(this);
            // Taken at once, or the park would return at once while the interrupt stands
            interruptedWrite |= Thread.interrupted();
        }
        interruptedWrite |= Thread.interrupted();
        return outcome;
    }

    /// Throws if the force on the forcing thread failed, `outcome` being what [#joinForce()] returned.
    private void checkForced(Object outcome) throws IOException {
        if (outcome == FORCED) {
            forcedPosition = position;
            return;
        }
        if (outcome instanceof IOException failure) {
            throw failure;
        }
        throw new IOException("The log segment could not be forced", (Throwable) outcome);
    }

    /// Ends the forcing thread, once nothing is written any more.
    private void stopForcer() {
        forcerLock.lock();
        try {
            forcerStopped = true;
            forceRequested.signal();
        } finally {
            forcerLock.unlock();
        }
    }

    /// {@return the bytes sealing adds to a frame body: the GCM tag, or the CRC-32C without encryption}
    private int sealOverhead() {
        return keys.encrypted() ? TAG_BYTES : Integer.BYTES;
    }

    /// {@return the end, exclusive, of the frames of `group` from `first` that one frame holds}
    private int frameEnd(List<Queued> group, int first) {
        return group.get(first).body().size() > RUN_MAX_BODY ? first + 1 : runEnd(group, first);
    }

    /// {@return the bytes the frame of the frames of `group` from `first` to `end`, exclusive, seals: the body of the
    /// frame at `first` alone, or the entries of a run}
    private static int plainBytes(List<Queued> group, int first, int end) {
        if (end - first == 1) {
            return group.get(first).body().size();
        }
        var bytes = 0;
        for (var index = first; index < end; index++) {
            bytes += FrameBody.runEntryBytes(group.get(index).body().size());
        }
        return bytes;
    }

    /// Seals the frames of `group` from `first` to `end`, exclusive, as one frame and lets them go from the list.
    private void sealFrames(ArrayList<Queued> group, int first, int end, boolean groupStart) throws IOException {
        var frame = group.get(first);
        if (frame.body().size() > RUN_MAX_BODY) {
            seal(frame, frame.body().size() + sealOverhead(), groupStart);
        } else {
            sealRun(group, first, end, groupStart);
        }
        for (var index = first; index < end; index++) {
            sealedBodies += group.get(index).body().size();
            group.set(index, null);
        }
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
        var plainLength = plainBytes(group, first, end);
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

    /// Seals `frame` into the output, writing the output out whenever it fills, or, sealing ahead, holding it.
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
                    var slice = fit(Math.min(SLICE, length - offset), TAG_BYTES);
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
            var slice = fit(Math.min(SLICE, length - offset), 0);
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

    /// {@return `wanted` bytes of a body to seal next, or, sealing ahead, fewer that fill what is left of the array
    /// beside `extra` bytes, a multiple of the AES block}
    ///
    /// The arrays sealed ahead into do not grow, and a slice that did not fit would leave a quarter of each empty.
    private int fit(int wanted, int extra) {
        if (!holding) {
            return wanted;
        }
        var left = output.length - used - extra;
        return wanted <= left || left < 16 ? wanted : left & -16;
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

    /// Makes room for `bytes` more in the output: grows it while it is smaller than [#OUTPUT], else writes it out;
    /// sealing ahead, holds it and seals on into a spare array.
    private void room(int bytes) throws IOException {
        if (used + bytes > output.length) {
            if (holding) {
                hold();
                return;
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

    /// The next group of a flush of the pool: the frames it took off the queue while the force of the group before ran,
    /// the first of which it sealed ahead.
    private static final class NextGroup {
        // The frames, in order; the first `sealed` are held ahead, and let go from the list
        final ArrayList<Queued> frames;
        int sealed;
        // The sequence and the frames queued at its last take off the queue, which its publication covers, and whether
        // the commits of its frames wrote units of the file of large values
        long sequence;
        long frameCount;
        boolean units;
        // The prepared segment after the current one, whose header it starts at, with the tag and the key of the use
        // drawn for it; or null, if it follows the group before in the current segment
        LogFiles.Prepared segment;
        long useTag;
        SecretKeySpec key;

        NextGroup() {
            this.frames = new ArrayList<>();
        }

        /// Forgets the group, once written or dropped.
        void clear() {
            frames.clear();
            sealed = 0;
            units = false;
            segment = null;
            key = null;
        }
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
    static long heapOf(long bodyBytes, long unitBytes, int writes) {
        return 13 + bodyBytes + unitBytes + (long) writes * WRITE_HEAP + (writes > 0 ? COMMIT_HEAP : 0);
    }

    /// {@return the offset in the current segment where the frames of the last group end, before their padding}
    long groupEnd() {
        return groupEnd;
    }

    /// {@return the offset in the current segment where the frames of the last group start, a block boundary}
    long groupStart() {
        return groupStart;
    }

    /// {@return the forces made so far, a group written through counting as one}
    long forces() {
        return forces;
    }

    /// {@return the frames sealed ahead so far, while a force ran on the forcing thread}
    long sealedAhead() {
        return sealedAhead;
    }

    /// {@return the groups written through so far, for tests}
    long writtenThrough() {
        return writtenThrough;
    }

    /// {@return the way later segments are opened to write through, or `null` if their groups are forced, for tests}
    WriteThrough.Kind writeThroughKind() {
        return writeThrough.kind();
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
            // The flush task has nothing left to write, or has stopped, its last force joined either way
            closeChannel();
            keepNext();
            stopForcer();
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

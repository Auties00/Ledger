package com.github.auties00.ledger.engine;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.zip.CRC32C;

/// The paged values waiting for a checkpoint that may leave the heap for the log, which holds them durably too.
///
/// A value a batch put to a paged key space stays on its batch's array, or on the array of its own a value of half a
/// chunk or more takes, until a checkpoint writes it, and the array with it: up to six intervals of log, hundreds of
/// megabytes after a history sync. Each commit with such values queues a [Record] of them once it is published, which
/// the log writer locates in the frame it seals. Once the queue holds more than its budget of values whose frames are
/// durable, a virtual thread moves the values of the oldest records off their arrays, until three quarters of the
/// budget is left: a value of at least [#LOG_VALUE_MIN] bytes to its place in the log ([LogValue]), with a tag that
/// authenticates it, a shorter one to an array of its own. A value that takes nearly all of its frame gets no tag: its
/// frame's own AES-GCM tag authenticates it, as a read takes the whole frame ([#framesAlone]), so the thread moves it
/// without reading a byte of it. An array no version needs any more is then garbage. The
/// newest values, those of the budget, stay where they are, so reads of recent writes do not change; a read of a value
/// in the log reads it back with one positioned read.
///
/// A record leaves the queue once its values moved, or once a published checkpoint holds its commit. Values are not
/// moved:
/// - of a batch's shared arrays in a key space until samples of its puts show that it replaces or removes few of
///   its values before a checkpoint ([ValueSlab.Overwrites#spills()]): a record lists versions, which keep their
///   batch's arrays alive until the record leaves the queue, and moving a value no reader can reach any more costs a
///   tag of its bytes for nothing. A version listed and then replaced counts in the held bytes until its record
///   leaves, a share of them the samples keep under an eighth. A value on an array of its own moves without them: the
///   array is the value's alone and lives as long as its version in any case, so the record keeps no other bytes
///   alive. In a key space that replaces such values before a checkpoint, the arrays it replaced stay until their
///   records leave, at most the budget of them, and the thread tags some of them for nothing;
/// - of a group the log wrote through to stable storage, whose pages that write took out of the system's cache, so a
///   read back would go to the device, unless every value the record lists is at least [#THROUGH_VALUE_MIN] bytes;
/// - of a batch on a [ValueSlab]'s chunks, which the slab bounds already, even beside a value on an array of its
///   own, of a transaction, or of one with a unit of [BlobFile]. A value a recovery copied to an array of its own
///   moves, to the log only;
/// - of a commit whose values on shared arrays would take, once moved, more than half the bytes they take now
///   ([Record#settle(long)]): a short value goes to an array of its own, which frees the array it shared only once
///   every other value on it left too, so moving a commit of mostly short values, like an app-state snapshot of 100 to
///   300 byte values, allocates about as much as it frees and makes the next checkpoint read the long ones back from
///   the log. The commit's values on arrays of their own move all the same, each freeing its array for a [LogValue].
///   Unless the commit holds more such values than twice the budget, and at least [#CHUNKS_MIN] bytes, that lie back
///   to back on their arrays: each array then leaves the heap whole for one [LogChunk], which the next checkpoint reads
///   back in one read rather than one per value. A thread of its own tags the blocks of those arrays while the log
///   writes the commit's frame ([Record#tagAhead(SpillQueue)]), and a wait for such a commit to be durable moves its
///   values on the waiting thread before it returns ([#moveOwn(long)]), rather than leaving them to the thread that
///   moves values, which would take them only after the commit returned.
///
/// A recovery moves a value it replays that the log holds before its last budget of bytes at once, on its own thread,
/// tagged where it lies in the frame body it came in
/// ([#moveReplayed(Crypto, Version, LogSegmentView, long, int, byte\[\])]), so no version keeps a body past its commit
/// and no value of the log's older part is copied; it queues only the values of the log's last budget, copied off their
/// bodies ([#keepsReplayed(long)]).
///
/// A segment holding such values stays in place past the checkpoint that makes it obsolete while a reader that may
/// still need them reads a tree older than that checkpoint ([#retirableBefore(long, ReaderTable)]): log disk use then
/// grows where the heap grew before. Once it retires, a reader that finds such a value reads it from its own tree.
///
/// Nothing of this is durable: a recovery replays the commits as ever, and moves the values it replays the same way
/// ([LiveLedger#replay(long, byte\[\], int, int, ReplayValues, LogSegmentView, long, long)]).
final class SpillQueue {
    /// The shortest value that moves to the log: a [LogValue] takes 40 bytes, an array of the value 16 more than the
    /// value; below it, the array costs less than a read back from the file.
    static final int LOG_VALUE_MIN = 256;
    /// The bytes a [LogValue] takes on the heap.
    private static final int LOG_VALUE_BYTES = 40;
    /// The least bytes of the values of a commit on shared arrays that leave the heap an array at a time ([LogChunk])
    /// when they would not pay to move one by one: twice the least budget, so an app-state snapshot of tens of
    /// thousands of entries, which the next checkpoint takes from the heap, stays there.
    static final long CHUNKS_MIN = 8L << 20;
    /// The most nanoseconds a move waits for the thread that tags a record's blocks ahead
    /// ([Record#tagAhead(SpillQueue)]), which takes tens of milliseconds for the runs of a 20 MB commit while the JVM
    /// still interprets its loop.
    static final long TAGS_WAIT = 2_000_000_000L;
    /// The bytes of the header of an array.
    private static final int ARRAY_HEADER = 16;
    /// The shortest values whose record moves to the log although the log wrote its group through: a value that long
    /// comes back with one positioned read even from the device, as long as the write of it took, where a record of
    /// small values would take a random read of the device per value.
    static final int THROUGH_VALUE_MIN = 128 << 10;
    /// The bytes of an AES block.
    static final int BLOCK = 16;
    /// The shortest value a read authenticates by the tag of its frame rather than by a tag of its own: below it, the
    /// set-up of an AES-GCM message per read costs more than the polynomials of the value.
    static final int FRAMED_VALUE_MIN = 16 << 10;
    /// The prime 2^61 - 1, the modulus of the polynomials of the tags.
    private static final long PRIME = (1L << 61) - 1;
    /// The bytes of a word of a tagged value: 7, so a word is below [#PRIME].
    private static final int WORD = 7;
    /// The scratch past which a [Crypto] given back lets go of it: the values of messages take a few kilobytes.
    static final int KEPT_SCRATCH = 16 << 10;
    /// The keys of segment uses a [Crypto] keeps a cipher for: a checkpoint reads values back in key order, which
    /// interleaves the few segments the log holds values in.
    private static final int KEPT_KEYS = 4;
    private static final SecureRandom KEYS = new SecureRandom();

    private final long budget;
    // The held bytes the thread moves values down to
    private final long lowWater;
    // The least bytes of a commit's values on shared arrays that move an array at a time: CHUNKS_MIN, or twice the
    // budget if more
    private long chunksMin;
    private final boolean encrypted;
    // The two points the polynomials of the tags are evaluated at, from 1 to PRIME - 1, drawn for this open at the
    // first value moved or read back and never stored; null before, and without encryption
    private volatile long[] tagKeys;
    private final Object lock;
    // The records not taken, oldest first; guarded by the lock
    private final ArrayDeque<Record> records;
    // The segments values moved to, by number, which no checkpoint has retired; guarded by the lock
    private final ArrayList<LogSegmentView> segments;
    // The bytes of values the records of the queue list; written holding the lock
    private volatile long held;
    // The records whose values move an array at a time that are queued or moving, oldest first; guarded by the lock,
    // which a committer waiting for the thread to move its record waits on
    private final ArrayList<Record> chunkRecords;
    // The size of chunkRecords, which a commit that waited to be durable reads without the lock; written holding it
    private volatile int chunkRecordCount;
    // The chunks a checkpoint read whole, whose bytes it keeps until it ends; guarded by the lock
    private final ArrayList<LogChunk> materialized;
    // Whether the thread that moves values runs
    private final AtomicBoolean running;
    private volatile boolean closed;
    // The latest sequence the log holds durably; before the store has a log, the commits queued are those a recovery
    // replays from it, every one durable
    private volatile LongSupplier durable = () -> Long.MAX_VALUE;
    // Reads the supplier the queue holds at each call: a thread that started moving values during a recovery goes on
    // after the store attaches its log, and a supplier it took at its start would let it move the values of live
    // commits the log does not hold durably yet, and skip the volatile read of the log's durable sequence that orders
    // the writer's locating of a record before the thread reads it
    private final LongSupplier current = () -> durable.getAsLong();
    private final AtomicReferenceArray<Crypto> cryptos;
    // The arrays the reads of framed values take their frames into
    private final HeapPool pool;
    // The values moved to the log and to arrays of their own, and the records drained, for tests
    private final AtomicLong loggedValues = new AtomicLong();
    private final AtomicLong copiedValues = new AtomicLong();
    private final AtomicLong drainedRecords = new AtomicLong();
    // The records whose values on shared arrays stay, as their move would not pay, for tests
    private final AtomicLong unpaidRecords = new AtomicLong();
    // The records whose values move an array at a time, the chunks made of them and the values they hold, for tests
    private final AtomicLong chunkedRecords = new AtomicLong();
    private final AtomicLong loggedChunks = new AtomicLong();
    private final AtomicLong chunkedValues = new AtomicLong();
    // The chunks checkpoints read whole so far, and the bytes of the log the reads of chunks read, for tests
    private final AtomicLong materializations = new AtomicLong();
    private final AtomicLong chunkBytesRead = new AtomicLong();
    // The runs of values whose block tags a thread computed ahead of their move, for tests
    private final AtomicLong taggedAhead = new AtomicLong();
    // Whether the blocks of a record whose values move an array at a time are tagged ahead of the move, and what the
    // thread that tags them runs once it starts, before it tags; for tests
    private volatile boolean tagsAhead = true;
    private volatile Runnable tagHookForTests;
    // The most nanoseconds a move waits for the thread that tags ahead, TAGS_WAIT but in tests
    private volatile long tagsWait = TAGS_WAIT;

    /// Creates the queue of a store.
    ///
    /// @param budget the bytes of values whose frames are durable the queue holds before it moves them
    /// @param encrypted whether the store's log is encrypted, so the values read back are authenticated by a keyed hash
    SpillQueue(long budget, boolean encrypted) {
        this.budget = budget;
        this.lowWater = budget - budget / 4;
        this.chunksMin = Math.max(CHUNKS_MIN, 2 * budget);
        this.encrypted = encrypted;
        this.lock = new Object();
        this.records = new ArrayDeque<>();
        this.segments = new ArrayList<>();
        this.chunkRecords = new ArrayList<>();
        this.materialized = new ArrayList<>();
        this.running = new AtomicBoolean();
        this.cryptos = new AtomicReferenceArray<>(Math.max(4, 2 * Runtime.getRuntime().availableProcessors()));
        this.pool = HeapPool.process();
    }

    /// Makes `bytes` the least bytes of a commit's values on shared arrays that move an array at a time; for tests.
    void chunksMinForTests(long bytes) {
        this.chunksMin = bytes;
    }

    /// Sets whether a thread tags the blocks of a record whose values move an array at a time ahead of their move
    /// ([Record#tagAhead(SpillQueue)]); for tests.
    void tagsAheadForTests(boolean ahead) {
        this.tagsAhead = ahead;
    }

    /// Makes the thread that tags the blocks of a record ahead of their move run `hook` once it starts, before it tags
    /// them, or nothing if `null`; for tests.
    void tagHookForTests(Runnable hook) {
        this.tagHookForTests = hook;
    }

    /// Makes `nanos` the most a move waits for the thread that tags a record's blocks ahead; for tests.
    void tagsWaitForTests(long nanos) {
        this.tagsWait = nanos;
    }

    /// {@return the runs of values whose block tags a thread computed ahead of their move; for tests}
    long taggedAheadForTests() {
        return taggedAhead.get();
    }

    /// {@return the pool the reads of [framed][LogValue#framed()] values take their arrays from: the process's, which
    /// every store with a directory shares}
    HeapPool pool() {
        return pool;
    }

    /// {@return the keys of the tags, drawn at the first call}
    private long[] tagKeys() {
        var keys = tagKeys;
        if (keys == null) {
            synchronized (lock) {
                keys = tagKeys;
                if (keys == null) {
                    keys = new long[]{1 + Math.floorMod(KEYS.nextLong(), PRIME - 1), 1 + Math.floorMod(KEYS.nextLong(), PRIME - 1)};
                    tagKeys = keys;
                }
            }
        }
        return keys;
    }

    /// {@return the budget for a heap of `maxMemory` bytes: a 1,024th, from 4 MiB to 64 MiB}
    ///
    /// The budget counts the bytes of the values, not of the arrays of their batches, which they keep alive with them,
    /// so a full queue holds more heap than its budget.
    static long defaultBudget(long maxMemory) {
        return Math.clamp(maxMemory / 1024, 4L << 20, 64L << 20);
    }

    /// Reads the latest durable sequence from `log` from now on, and moves values whenever the queue is over budget.
    void useLog(LogWriter log) {
        useDurable(log::durableSequence);
    }

    /// Reads the latest durable sequence from `durableSequence` from now on, a thread moving values included, and moves
    /// values whenever the queue is over budget.
    void useDurable(LongSupplier durableSequence) {
        this.durable = durableSequence;
        if (over()) {
            start();
        }
    }

    /// {@return the view of the use of segment `number` in `path` whose key is `key`, `null` without encryption}
    LogSegmentView segment(long number, Path path, SecretKeySpec key) {
        return new LogSegmentView(number, path, key, this);
    }

    /// {@return whether the queue holds more than its budget}
    boolean over() {
        return held > budget;
    }

    /// {@return whether a value of `length` bytes in a frame of an encrypted segment use whose body holds `frameBody`
    /// bytes is read back as its whole frame, which the frame's own AES-GCM tag authenticates}
    ///
    /// The value takes nearly all of the frame: at least [#FRAMED_VALUE_MIN] bytes, and at least eight ninths of the
    /// body, so a read takes at most an eighth more bytes than the value's own; and the frame fits an array the pool
    /// recycles. The tag of such a value is never made: the value moves without being read.
    static boolean framesAlone(LogSegmentView segment, int frameBody, int length) {
        return segment.key() != null && length >= FRAMED_VALUE_MIN && frameBody >= length
                && frameBody - length <= length >>> 3
                && LogWriter.FRAME_HEADER + (long) frameBody + LogWriter.TAG_BYTES <= HeapPool.MAX_POOLED;
    }

    /// {@return an empty record of the commit at `sequence`, with room for `writes` values}
    Record record(long sequence, int writes) {
        return new Record(sequence, Math.max(1, writes));
    }

    /// {@return whether a recovery keeps on the heap, listed in the queue, the values of a frame that `logFrom` bytes
    /// of log at most start from: those of the log's last budget}
    boolean keepsReplayed(long logFrom) {
        return logFrom <= budget;
    }

    /// Records that a recovery moves values of the commit at `sequence` it replays to `segment`, before the first
    /// one moves there ([#moveReplayed(Crypto, Version, LogSegmentView, long, int, byte\[\])]), so a checkpoint that
    /// retires segments sees the commit.
    void noteReplayed(long sequence, LogSegmentView segment) {
        synchronized (lock) {
            note(segment, sequence);
        }
    }

    /// Records that values of the commit at `sequence` move to `segment`, which joins the segments in the order of
    /// their numbers if it is the first; holding the lock.
    ///
    /// The thread that moves values takes the records oldest first, but a committer takes its own record past older
    /// ones ([#moveOwn(long)]), so a segment may be noted after a later one: [#published(long, long, long)] and
    /// [#retirableBefore(long, ReaderTable)] stop at the first segment past their bound, and would never reach an older
    /// one behind it.
    private void note(LogSegmentView segment, long sequence) {
        if (!segment.note(sequence)) {
            return;
        }
        var at = segments.size();
        while (at > 0 && segments.get(at - 1).number() > segment.number()) {
            at--;
        }
        segments.add(at, segment);
    }

    /// Moves the value of `version`, which a recovery replayed from `frame`, at once to its place in the log, on the
    /// caller's thread: `frame` is the opened body of the frame at `frameOffset` of `segment`, which holds it durably,
    /// and `version` is on it, at `bodyIndex`, and at least [#LOG_VALUE_MIN] bytes. The tag is made where the value
    /// lies in `frame`, which the version no longer keeps.
    ///
    /// @param crypto the caller's, until it gives it back ([#takeCrypto()])
    /// @return whether the value moved
    boolean moveReplayed(Crypto crypto, Version version, LogSegmentView segment, long frameOffset, int bodyIndex, byte[] frame) {
        if (!version.spill(crypto, segment, frameOffset, bodyIndex, 0, frame)) {
            return false;
        }
        loggedValues.incrementAndGet();
        return true;
    }

    /// {@return the place in the log of a value a recovery replays into a [PackedRun]: the `length` bytes at `bodyIndex`
    /// of `frame`, the opened body of the frame at `frameOffset` of `segment`, which holds it durably, of the write
    /// stamped `stamp`, tagged where they lie}
    ///
    /// @param crypto the caller's, until it gives it back ([#takeCrypto()])
    LogValue tagReplayed(Crypto crypto, LogSegmentView segment, long frameOffset, int bodyIndex, long stamp, byte[] frame,
                         int length) {
        var logged = crypto.logValue(segment, frameOffset, bodyIndex, stamp, frame, bodyIndex, length);
        loggedValues.incrementAndGet();
        return logged;
    }

    /// Queues `record`, of a published commit after every one queued, and starts moving values if the queue is over
    /// its budget.
    void add(Record record) {
        if (record.count == 0) {
            return;
        }
        var settled = record.settle(chunksMin);
        if (settled == Record.CHUNKS) {
            chunkedRecords.incrementAndGet();
        } else if (settled != Record.ALL) {
            unpaidRecords.incrementAndGet();
            if (settled == Record.NONE) {
                record.release();
                return;
            }
        }
        synchronized (lock) {
            if (closed) {
                return;
            }
            records.addLast(record);
            held = held + record.bytes;
            if (settled == Record.CHUNKS) {
                chunkRecords.add(record);
                chunkRecordCount = chunkRecords.size();
            }
        }
        if (settled == Record.CHUNKS && tagsAhead) {
            record.tagAhead(this);
        }
        if (over()) {
            start();
        }
    }

    /// Starts moving values if the queue is over its budget; called by the log as a group becomes durable, which may
    /// make the oldest record's values movable.
    void durableAdvanced() {
        if (over() && !running.get()) {
            start();
        }
    }

    /// Starts the thread that moves values, unless it runs; a thread the system refuses leaves the values where they
    /// are, until the next start.
    private void start() {
        if (closed || !running.compareAndSet(false, true)) {
            return;
        }
        try {
            Thread.ofVirtual().name("ledger-spill").start(this::run);
        } catch (Throwable _) {
            running.set(false);
        }
    }

    private void run() {
        try {
            drain(current);
        } finally {
            running.set(false);
        }
        // A group that became durable as the thread stopped found it running
        if (movable()) {
            start();
        }
    }

    /// {@return whether the queue is over its budget and its oldest record's frame is durable}
    private boolean movable() {
        var log = durable;
        if (!over()) {
            return false;
        }
        synchronized (lock) {
            var first = records.peekFirst();
            return first != null && first.sequence <= log.getAsLong();
        }
    }

    /// Moves the values of the oldest records whose commits `durableSequence` gives durable, while the queue holds more
    /// than three quarters of its budget, on the caller's thread.
    void drain(LongSupplier durableSequence) {
        drain(durableSequence, lowWater);
    }

    /// Moves the values of the oldest records whose commits `durableSequence` gives durable, while the queue holds more
    /// than `downTo` bytes, on the caller's thread.
    private void drain(LongSupplier durableSequence, long downTo) {
        var crypto = takeCrypto();
        try {
            while (!closed) {
                Record record;
                synchronized (lock) {
                    record = records.peekFirst();
                    if (record == null || held <= downTo || record.sequence > durableSequence.getAsLong()) {
                        return;
                    }
                    records.pollFirst();
                    take(record);
                }
                try {
                    record.spill(this, crypto);
                } finally {
                    if (record.chunked) {
                        moved(record);
                    }
                }
            }
        } finally {
            giveBack(crypto);
        }
    }

    /// Takes `record`, out of the queue, to move its values; holding the lock.
    private void take(Record record) {
        held = held - record.bytes;
        drainedRecords.incrementAndGet();
        if (record.logs()) {
            // Before any value moves there, so a checkpoint that retires segments sees the commit
            note(record.segment, record.sequence);
        }
    }

    /// Records that the values of `record`, which move an array at a time, moved or stay, waking a committer that
    /// waits for them.
    private void moved(Record record) {
        synchronized (lock) {
            if (chunkRecords.remove(record)) {
                chunkRecordCount = chunkRecords.size();
                lock.notifyAll();
            }
        }
    }

    /// Moves the values of the commit at `sequence`, which the log holds durably, on the caller's thread, if they move
    /// an array at a time ([LogChunk]) and are still queued; or waits for the thread that moves values if it took
    /// them. A commit that waits for its frame to be durable calls it once it is: such a commit's values on shared
    /// arrays take more than twice the budget, which a collection right after it would otherwise find on the heap. The
    /// blocks of those arrays are tagged by then, by the thread that tags them while the log writes the frame
    /// ([Record#tagAhead(SpillQueue)]), which the move waits for if it still tags them, or else by the move.
    ///
    /// Any other commit returns at once, after one volatile read. An interrupt while waiting for the thread ends the
    /// wait, the interrupt kept, and the thread moves the values as it would. A failure to move them leaves them on the
    /// heap, which the caller, whose commit is durable, never sees.
    void moveOwn(long sequence) {
        if (chunkRecordCount == 0) {
            return;
        }
        Record record = null;
        synchronized (lock) {
            for (var queued : chunkRecords) {
                if (queued.sequence == sequence) {
                    record = queued;
                    break;
                }
            }
            if (record == null || closed || sequence > durable.getAsLong()) {
                return;
            }
            if (!records.remove(record)) {
                // The thread that moves values took it
                while (!closed && chunkRecords.contains(record)) {
                    try {
                        lock.wait();
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                return;
            }
            take(record);
        }
        Crypto crypto = null;
        try {
            crypto = takeCrypto();
            record.spill(this, crypto);
        } catch (RuntimeException _) {
            // The commit is durable all the same: the values left where they are stay until a checkpoint takes them
        } finally {
            if (crypto != null) {
                giveBack(crypto);
            }
            moved(record);
        }
    }

    /// Counts `bytes` of the log a read of a [LogChunk] read.
    void readChunkBytes(int bytes) {
        chunkBytesRead.addAndGet(bytes);
    }

    /// Keeps `chunk`, which a checkpoint read whole, until the checkpoint ends ([#releaseMaterialized()]).
    void materialized(LogChunk chunk) {
        materializations.incrementAndGet();
        synchronized (lock) {
            materialized.add(chunk);
        }
    }

    /// Lets go of the bytes of the chunks a checkpoint read whole, as it ends, whether it was published or failed.
    void releaseMaterialized() {
        synchronized (lock) {
            for (var chunk : materialized) {
                chunk.dropMaterialized();
            }
            materialized.clear();
        }
    }

    /// Drops the records of the commits up to `cut`, which a checkpoint just published holds, and records that it holds
    /// every commit of the segments before `logStart`, `latest` being the latest sequence as it was published; holding
    /// the store's publishing role.
    void published(long cut, long logStart, long latest) {
        synchronized (lock) {
            Record record;
            while ((record = records.peekFirst()) != null && record.sequence <= cut) {
                records.pollFirst();
                held = held - record.bytes;
                record.release();
                if (record.chunked && chunkRecords.remove(record)) {
                    chunkRecordCount = chunkRecords.size();
                    lock.notifyAll();
                }
            }
            for (var segment : segments) {
                if (segment.number() >= logStart) {
                    break;
                }
                segment.cover(latest);
            }
        }
        releaseMaterialized();
    }

    /// Retires, from the oldest, the segments before `first` that values moved to and that no reader may need any
    /// more, and returns the first segment the log keeps: `first`, or the oldest that a reader may still need.
    ///
    /// A reader may need a segment only if it may read a tree that lacks a commit whose values moved there. Each such
    /// segment holds commits that a published checkpoint holds, its tree published when the latest sequence was
    /// [LogSegmentView#coveredAt()]: a reader registered later reads that tree or a newer one, and a reader at a
    /// sequence before the segment's oldest commit sees none of its versions. So a segment is needed while a reader is
    /// registered between the two, which at the latest sequence of the publication itself may read either tree.
    /// A reader at such a sequence registers before it takes its tree, so one that took the tree before the publication
    /// is in the table when this looks.
    ///
    /// @return the first segment the log keeps
    long retirableBefore(long first, ReaderTable readers) {
        synchronized (lock) {
            var iterator = segments.iterator();
            while (iterator.hasNext()) {
                var segment = iterator.next();
                if (segment.number() >= first) {
                    break;
                }
                if (segment.coveredAt() < 0 || readers.readsWithin(segment.lowest(), segment.coveredAt())) {
                    return segment.number();
                }
                segment.retire();
                iterator.remove();
            }
            return first;
        }
    }

    /// {@return whether a segment values moved to is held past a checkpoint that makes it obsolete}
    boolean holdsBack() {
        synchronized (lock) {
            return !segments.isEmpty() && segments.getFirst().coveredAt() >= 0;
        }
    }

    /// Retires every segment values moved to, as the store closes and no reader reads any more.
    void retireAll() {
        synchronized (lock) {
            for (var segment : segments) {
                segment.retire();
            }
            segments.clear();
        }
    }

    /// Stops moving values and retires every segment, as the store closes.
    void close() {
        closed = true;
        synchronized (lock) {
            for (var record : records) {
                record.release();
            }
            records.clear();
            held = 0;
            chunkRecords.clear();
            chunkRecordCount = 0;
            lock.notifyAll();
        }
        releaseMaterialized();
        retireAll();
    }

    /// {@return the bytes of values the queued records list; for tests}
    long heldForTests() {
        return held;
    }

    /// {@return the values moved to the log so far; for tests}
    long loggedValuesForTests() {
        return loggedValues.get();
    }

    /// {@return the values moved to arrays of their own so far; for tests}
    long copiedValuesForTests() {
        return copiedValues.get();
    }

    /// {@return the room for values of each record the queue holds, oldest first; for tests}
    int[] recordRoomsForTests() {
        synchronized (lock) {
            return records.stream().mapToInt(record -> record.versions.length).toArray();
        }
    }

    /// {@return the records whose values a drain took to move so far; for tests}
    long drainedRecordsForTests() {
        return drainedRecords.get();
    }

    /// {@return the records whose values on shared arrays stay where they are, as moving them would not pay
    /// ([Record#settle(long)]), whether or not the record's values on arrays of their own move; for tests}
    long unpaidRecordsForTests() {
        return unpaidRecords.get();
    }

    /// {@return the records whose values on shared arrays move an array at a time ([Record#CHUNKS]); for tests}
    long chunkedRecordsForTests() {
        return chunkedRecords.get();
    }

    /// {@return the chunks values moved to so far ([LogChunk]); for tests}
    long loggedChunksForTests() {
        return loggedChunks.get();
    }

    /// {@return the values moved to chunks so far; for tests}
    long chunkedValuesForTests() {
        return chunkedValues.get();
    }

    /// {@return the records whose values move an array at a time that are queued or moving; for tests}
    int chunkRecordsForTests() {
        return chunkRecordCount;
    }

    /// {@return the chunks checkpoints read whole so far; for tests}
    long materializationsForTests() {
        return materializations.get();
    }

    /// {@return the bytes of the log the reads of chunks read so far; for tests}
    long chunkBytesReadForTests() {
        return chunkBytesRead.get();
    }

    /// {@return the chunks a checkpoint read whole and still keeps; for tests}
    int materializedForTests() {
        synchronized (lock) {
            return materialized.size();
        }
    }

    /// {@return the segments values moved to that no checkpoint retired, by number; for tests}
    long[] segmentsForTests() {
        synchronized (lock) {
            return segments.stream().mapToLong(LogSegmentView::number).toArray();
        }
    }

    /// Waits until the thread that moves values is not running; for tests.
    void awaitIdleForTests() throws InterruptedException {
        while (running.get()) {
            Thread.sleep(1);
        }
    }

    /// {@return a [Crypto] for the caller alone until [#giveBack(Crypto)]}
    Crypto takeCrypto() {
        for (var i = 0; i < cryptos.length(); i++) {
            var crypto = cryptos.getPlain(i);
            if (crypto != null && cryptos.compareAndSet(i, crypto, null)) {
                return crypto;
            }
        }
        return new Crypto(this);
    }

    /// Gives `crypto` back for another caller, or to the collector if every place is taken.
    void giveBack(Crypto crypto) {
        crypto.trim();
        for (var i = 0; i < cryptos.length(); i++) {
            if (cryptos.getPlain(i) == null && cryptos.compareAndSet(i, null, crypto)) {
                return;
            }
        }
    }

    /// The values of one commit that may move, where its frame lies in the log, and whether they can.
    ///
    /// It lists versions, not their chains: a version a later write replaced moves too, which costs its tag and
    /// spares a walk of the chain per value. The log writer locates it while it seals the frame, before the frame is
    /// durable, which publishes the location to whatever reads the durable sequence after.
    static final class Record {
        private final long sequence;
        private Version[] versions;
        // Where each value starts in the body of its commit
        private int[] offsets;
        private int count;
        /// The verdict of [#settle(long)] that every value of the record may move.
        static final int ALL = 0;
        /// The verdict of [#settle(long)] that only the record's values on arrays of their own may move.
        static final int OWN = 1;
        /// The verdict of [#settle(long)] that no value of the record moves.
        static final int NONE = 2;
        /// The verdict of [#settle(long)] that the record's values on shared arrays that lie back to back on them move
        /// an array at a time ([LogChunk]), and its values on arrays of their own one by one.
        static final int CHUNKS = 3;
        // The states of the tags of the runs' blocks ([#tags])
        private static final int TAGS_NONE = 0;
        private static final int TAGS_RUNNING = 1;
        private static final int TAGS_DONE = 2;
        private static final VarHandle TAGS;

        static {
            try {
                TAGS = MethodHandles.lookup().findVarHandle(Record.class, "tags", int.class);
            } catch (ReflectiveOperationException exception) {
                throw new ExceptionInInitializerError(exception);
            }
        }

        // The bytes of the values the record moves, whether one is at least LOG_VALUE_MIN, and the bytes of the
        // shortest
        private long bytes;
        private boolean large;
        private int shortest = Integer.MAX_VALUE;
        // The bytes of the values on arrays other values may share (a batch's, a frame's body) and the heap they would
        // take once moved; the bytes of the values on arrays of their own, and of the shortest of them
        private long sharedBytes;
        private long sharedMoved;
        private long ownBytes;
        private int ownShortest = Integer.MAX_VALUE;
        // Whether only the values on arrays of their own move one by one ([#settle(long)])
        private boolean ownOnly;
        // Whether the values on shared arrays move an array at a time ([#CHUNKS])
        private boolean chunked;
        // In a record whose values move an array at a time, the runs of its values that lie back to back on one shared
        // array, in the order of the writes: the read-only segment of the array, the first and last index of the run's
        // values among the versions, the values in it, where the first starts on the array and where the last ends;
        // an end of -1 marks a run with a gap, which stays
        private MemorySegment[] runHomes;
        private int[] runFirsts;
        private int[] runLasts;
        private int[] runValues;
        private int[] runStarts;
        private int[] runEnds;
        private int runs;
        // The tags of the blocks of each run that a thread computed while the log wrote the frame ([#tagAhead]): per
        // run the first polynomials, null without encryption, and the low 32 bits of the second or the CRC-32C; null
        // until it published them, and if it failed. Written before tags becomes TAGS_DONE, read after
        private long[][] runHighs;
        private int[][] runLows;
        // TAGS_NONE until the thread that tags ahead starts, or the move claims the tags first; TAGS_RUNNING while that
        // thread computes them; TAGS_DONE once it published them, or the move claimed them
        private volatile int tags;
        // The thread of the move waiting for the thread that tags ahead, which that thread unparks once done
        private volatile Thread tagWaiter;
        // Set by the log writer as it seals the frame: the segment use, the frame's offset, where the commit's body
        // starts in the frame's body, the bytes of the frame's body, 0 if unknown, and whether the group was written
        // through
        private LogSegmentView segment;
        private long frameOffset;
        private int commitStart;
        private int frameBody;
        private boolean through;

        private Record(long sequence, int capacity) {
            this.sequence = sequence;
            this.versions = new Version[capacity];
            this.offsets = new int[capacity];
        }

        /// Lists `version`, whose value of `length` bytes starts at `offset` of the commit's body.
        ///
        /// @param offset where the value starts in the commit's body; or, for a value a split body left to its section,
        ///               `-1` minus where it starts in the section, until [#resolveSection(int)]
        void add(Version version, int offset, int length) {
            if (count == versions.length) {
                versions = Arrays.copyOf(versions, count * 2);
                offsets = Arrays.copyOf(offsets, count * 2);
            }
            versions[count] = version;
            offsets[count++] = offset;
            bytes += length;
            large |= length >= LOG_VALUE_MIN;
            shortest = Math.min(shortest, length);
            if (length >= LOG_VALUE_MIN && version.ownArray() != null) {
                ownBytes += length;
                ownShortest = Math.min(ownShortest, length);
            } else {
                sharedBytes += length;
                sharedMoved += length >= LOG_VALUE_MIN ? LOG_VALUE_BYTES : (ARRAY_HEADER + length + 7) & -8;
            }
        }

        /// Makes the offsets of the values the commit's split body left to its section, which [#add] took as `-1` minus
        /// where each starts in the section, the offsets of those values in the body, whose section starts at
        /// `sectionStart`; the other offsets stay. Called once the body is complete, before the record is queued.
        void resolveSection(int sectionStart) {
            for (var i = 0; i < count; i++) {
                if (offsets[i] < 0) {
                    offsets[i] = sectionStart + (-1 - offsets[i]);
                }
            }
        }

        /// Records where the log wrote the frame of the commit: at `frameOffset` of the use `segment`, the commit's body
        /// at `commitStart` of the frame's body of `frameBody` bytes, written through to stable storage if `through`.
        ///
        /// @param segment the use, or `null` if the store moves no value
        /// @param frameBody the bytes of the frame's body before it was sealed, or 0 if they are not known, so no value
        ///                  is authenticated by the frame's tag ([LogValue#framed()])
        void locate(LogSegmentView segment, long frameOffset, int commitStart, int frameBody, boolean through) {
            this.segment = segment;
            this.frameOffset = frameOffset;
            this.commitStart = commitStart;
            this.frameBody = frameBody;
            this.through = through;
        }

        /// Decides which values move, before the record is queued: all of them if moving those on shared arrays frees
        /// at least as much heap as it allocates, what they would take once moved (a [LogValue] for a long one and an
        /// array of its own for a short one) being at most half of their bytes, else only those on arrays of their own,
        /// whose bytes alone the record then counts; unless at least `chunksMin` bytes of the values on shared arrays
        /// lie in runs, back to back on one array each, which then move an array at a time, and count with those on
        /// arrays of their own.
        ///
        /// A batch's values lie packed on arrays of about their bytes, so moving them all frees about their bytes, and
        /// moving some frees nothing while the others keep the arrays alive; a commit of short values frees little more
        /// than it copies, and leaves the heap it holds where it was. A value of at least [#LOG_VALUE_MIN] bytes on an
        /// array of its own frees that array whatever its record's other values do. A run of short values on one array
        /// moves for about 13 bytes per 1 KiB of the array, and frees it once its run is all the array holds.
        ///
        /// @return [#ALL], [#OWN], [#NONE] or [#CHUNKS]
        int settle(long chunksMin) {
            if (sharedMoved <= sharedBytes / 2) {
                return ALL;
            }
            if (ownBytes != 0) {
                ownOnly = true;
                shortest = ownShortest;
            }
            if (sharedBytes >= chunksMin) {
                var runBytes = findRuns();
                if (runBytes >= chunksMin) {
                    chunked = true;
                    bytes = runBytes + ownBytes;
                    return CHUNKS;
                }
                dropRuns();
            }
            if (!ownOnly) {
                return NONE;
            }
            bytes = ownBytes;
            return OWN;
        }

        /// Finds the runs of the record's values that lie back to back on one shared array, in the order of the
        /// writes, and returns their bytes, those of runs that stay left out.
        ///
        /// A batch's values of paged key spaces lie on its arrays in the order of its writes, each array filled before
        /// the next, so the values the record lists of one array are one run, unless a value the record does not list,
        /// replaced by a write of the same batch or of a key space whose values stay, lies between two of them.
        ///
        /// The frame holds each value after its put's kind, key space and key, a few bytes, but a write between two
        /// values of an array, a value on an array of its own, can put any number of bytes between them, which a read
        /// of their chunk would read and decrypt for nothing. So a run is cut where the bytes of the body between the
        /// values of one block of [LogChunk#BLOCK] bytes would pass [LogChunk#GAP]: a block's bytes then lie in at
        /// most [LogChunk#BLOCK] plus [LogChunk#GAP] bytes of the body. The runs cut from one array move only all
        /// together, the array being freed only once every value on it moves, and only if they average
        /// [LogChunk#PIECE_MIN] bytes; a run without a byte stays, as it frees nothing.
        private long findRuns() {
            var capacity = 16;
            runHomes = new MemorySegment[capacity];
            runFirsts = new int[capacity];
            runLasts = new int[capacity];
            runValues = new int[capacity];
            runStarts = new int[capacity];
            runEnds = new int[capacity];
            runs = 0;
            // Where the last run's last value ends in the commit's body, and the bytes of the body between its values
            // in its block gapBlock
            var bodyEnd = 0;
            var gapBlock = -1;
            var gapBytes = 0L;
            for (var i = 0; i < count; i++) {
                var version = versions[i];
                var home = version.sharedSource();
                if (home == null) {
                    continue;
                }
                var offset = version.sourceOffset();
                var length = version.valueLength();
                var body = offsets[i];
                var last = runs - 1;
                if (last >= 0 && runHomes[last] == home) {
                    if (runEnds[last] != offset || body < bodyEnd) {
                        // A gap, a value before the run's end, or one the frame holds before the run's last value:
                        // the run stays, and so does this value
                        runEnds[last] = -1;
                        continue;
                    }
                    var at = offset - runStarts[last];
                    var cut = false;
                    if (at % LogChunk.BLOCK != 0) {
                        // A place inside a block: the body between the two values is part of the block's reads
                        if (at / LogChunk.BLOCK != gapBlock) {
                            gapBlock = at / LogChunk.BLOCK;
                            gapBytes = 0;
                        }
                        gapBytes += body - bodyEnd;
                        cut = gapBytes > LogChunk.GAP;
                    }
                    if (!cut) {
                        runEnds[last] = offset + length;
                        runLasts[last] = i;
                        runValues[last]++;
                        bodyEnd = body + length;
                        continue;
                    }
                }
                if (runs == runHomes.length) {
                    capacity = runs * 2;
                    runHomes = Arrays.copyOf(runHomes, capacity);
                    runFirsts = Arrays.copyOf(runFirsts, capacity);
                    runLasts = Arrays.copyOf(runLasts, capacity);
                    runValues = Arrays.copyOf(runValues, capacity);
                    runStarts = Arrays.copyOf(runStarts, capacity);
                    runEnds = Arrays.copyOf(runEnds, capacity);
                }
                runHomes[runs] = home;
                runFirsts[runs] = i;
                runLasts[runs] = i;
                runValues[runs] = 1;
                runStarts[runs] = offset;
                runEnds[runs] = offset + length;
                runs++;
                bodyEnd = body + length;
                gapBlock = -1;
                gapBytes = 0;
            }
            var runBytes = 0L;
            var first = 0;
            while (first < runs) {
                // The runs cut from one array, which follow each other
                var home = runHomes[first];
                var stop = first;
                var bytes = 0L;
                var moves = true;
                while (stop < runs && runHomes[stop] == home) {
                    var length = runEnds[stop] - runStarts[stop];
                    moves &= runEnds[stop] >= 0 && length > 0 && length <= LogChunk.MAX_BYTES;
                    bytes += length;
                    stop++;
                }
                moves &= stop - first == 1 || bytes >= (long) (stop - first) * LogChunk.PIECE_MIN;
                if (moves) {
                    runBytes += bytes;
                } else {
                    for (var run = first; run < stop; run++) {
                        runEnds[run] = -1;
                    }
                }
                first = stop;
            }
            return runBytes;
        }

        private void dropRuns() {
            runHomes = null;
            runFirsts = null;
            runLasts = null;
            runValues = null;
            runStarts = null;
            runEnds = null;
            runs = 0;
        }

        /// {@return the sequence of the commit}
        long sequence() {
            return sequence;
        }

        /// {@return whether values move to the log: some are long enough, or move an array at a time, and its frame is
        /// in a segment's cache or every value is long enough to read back from the device}
        private boolean logs() {
            if (segment == null) {
                return false;
            }
            if (chunked) {
                return !through || ownOnly && ownShortest >= THROUGH_VALUE_MIN;
            }
            return large && (!through || shortest >= THROUGH_VALUE_MIN);
        }

        /// Moves the values, unless a long one cannot move to the log, then forgets them.
        ///
        /// The runs of values that move an array at a time stay in a group the log wrote through, whose pages that
        /// write took out of the system's cache: a read of one of them would go to the device.
        private void spill(SpillQueue queue, Crypto crypto) {
            if (chunked) {
                if (segment != null && !through) {
                    spillRuns(queue, crypto);
                }
                if (ownOnly && segment != null && (!through || ownShortest >= THROUGH_VALUE_MIN)) {
                    spillValues(queue, crypto);
                }
            } else if (!large || logs()) {
                spillValues(queue, crypto);
            }
            release();
        }

        /// Moves the values one by one: to the log, or to arrays of their own.
        private void spillValues(SpillQueue queue, Crypto crypto) {
            var logged = 0;
            var copied = 0;
            for (var i = 0; i < count; i++) {
                var version = versions[i];
                // A value on a shared array stays on a segment until a spill moves it, so one on an array is on its own
                if (ownOnly && version.ownArray() == null) {
                    continue;
                }
                var framed = segment != null && framesAlone(segment, frameBody, version.valueLength()) ? frameBody : 0;
                if (version.spill(crypto, segment, frameOffset, commitStart + offsets[i], framed, null)) {
                    if (version.valueLength() >= LOG_VALUE_MIN) {
                        logged++;
                    } else {
                        copied++;
                    }
                }
            }
            queue.loggedValues.addAndGet(logged);
            queue.copiedValues.addAndGet(copied);
        }

        /// Moves each run of values on a shared array to a [LogChunk] of its place in the frame, its block tags made
        /// from the array before any value moves. A run whose values lie apart from where the record found them stays.
        private void spillRuns(SpillQueue queue, Crypto crypto) {
            long[][] highs = null;
            int[][] lows = null;
            if (takeTags(queue)) {
                highs = runHighs;
                lows = runLows;
            }
            if (lows == null) {
                highs = new long[runs][];
                lows = new int[runs][];
                for (var run = 0; run < runs; run++) {
                    if (runEnds[run] >= 0) {
                        tagRun(run, crypto, queue.encrypted, highs, lows);
                    }
                }
            }
            var chunks = 0;
            var values = 0;
            for (var run = 0; run < runs; run++) {
                if (runEnds[run] < 0) {
                    continue;
                }
                var chunk = chunkOf(run, highs[run], lows[run]);
                if (chunk == null) {
                    continue;
                }
                chunks++;
                var home = runHomes[run];
                for (var i = runFirsts[run]; i <= runLasts[run]; i++) {
                    if (versions[i].spillToChunk(home, chunk)) {
                        values++;
                    }
                }
            }
            queue.loggedChunks.addAndGet(chunks);
            queue.chunkedValues.addAndGet(values);
        }

        /// {@return the place in the log of the values of run `run`, tagged block by block from the array they lie
        /// on, or `null` if they no longer lie back to back where the record found them, or not in the order of the
        /// frame}
        ///
        /// Values that lie in the frame's body as they lie on the array, as a split frame's section holds them, get a
        /// chunk without a place per value ([LogChunk]).
        ///
        /// @param highs the first polynomials of the tags of the run's blocks ([#tagRun]), or `null` without encryption
        /// @param lows the low 32 bits of the second polynomials of those tags, or their CRC-32C
        private LogChunk chunkOf(int run, long[] highs, int[] lows) {
            var home = runHomes[run];
            var values = runValues[run];
            var start = runStarts[run];
            var firstBody = commitStart + offsets[runFirsts[run]];
            var at = 0;
            var next = start;
            var bodyEnd = Integer.MIN_VALUE;
            var contiguous = true;
            for (var i = runFirsts[run]; i <= runLasts[run]; i++) {
                var version = versions[i];
                if (version.sharedSource() != home) {
                    continue;
                }
                var body = commitStart + offsets[i];
                if (at == values || version.sourceOffset() != next || body < bodyEnd) {
                    return null;
                }
                contiguous &= body - firstBody == next - start;
                at++;
                next += version.valueLength();
                bodyEnd = body + version.valueLength();
            }
            if (at != values || next != runEnds[run]) {
                return null;
            }
            int[] starts = null;
            int[] bodies = null;
            if (!contiguous) {
                starts = new int[values + 1];
                bodies = new int[values];
                at = 0;
                for (var i = runFirsts[run]; i <= runLasts[run]; i++) {
                    var version = versions[i];
                    if (version.sharedSource() == home) {
                        starts[at] = version.sourceOffset();
                        bodies[at++] = commitStart + offsets[i];
                    }
                }
                starts[values] = next;
            }
            return new LogChunk(segment, frameOffset, start, next, firstBody, sequence, offsets[runFirsts[run]], starts,
                    bodies, highs, lows);
        }

        /// Tags the blocks of run `run` into `highs` and `lows` at its index, from the bytes of the array it lies on,
        /// bound to the commit's sequence and to where its first value starts in the commit's body
        /// ([LogChunk#place(int, int)]): the array is never written once its batch is prepared, so tags made while the
        /// log writes the frame hold the bytes the frame holds.
        private void tagRun(int run, Crypto crypto, boolean encrypted, long[][] highs, int[][] lows) {
            var start = runStarts[run];
            var length = runEnds[run] - start;
            var bytes = crypto.scratch(length);
            MemorySegment.copy(runHomes[run], start, MemorySegment.ofArray(bytes), 0, length);
            var blocks = LogChunk.blocks(length);
            var runHighs = encrypted ? new long[blocks] : null;
            var runLows = new int[blocks];
            var first = offsets[runFirsts[run]];
            for (var block = 0; block < blocks; block++) {
                var from = block * LogChunk.BLOCK;
                crypto.tagBlock(bytes, from, Math.min(LogChunk.BLOCK, length - from), sequence,
                        LogChunk.place(first, block), runHighs, runLows, block);
            }
            highs[run] = runHighs;
            lows[run] = runLows;
        }

        /// Starts a thread that tags the blocks of the runs while the log writes and forces the commit's frame, so the
        /// move of the values, once the frame is durable, takes the tags rather than computing them on the thread of
        /// the commit that waits for it ([SpillQueue#moveOwn(long)]). A thread the system refuses leaves them to the
        /// move. The ciphers are taken here, so the thread takes no lock and never parks before it publishes the tags.
        void tagAhead(SpillQueue queue) {
            var crypto = queue.takeCrypto();
            try {
                Thread.ofVirtual().name("ledger-chunk-tags").start(() -> {
                    try {
                        computeAhead(queue, crypto);
                    } finally {
                        queue.giveBack(crypto);
                    }
                });
            } catch (Throwable _) {
                queue.giveBack(crypto);
            }
        }

        /// Tags the blocks of the runs ahead of the move, unless the move claimed them first, and publishes the tags,
        /// none if they could not be made, waking the move if it waits for them.
        private void computeAhead(SpillQueue queue, Crypto crypto) {
            if (!TAGS.compareAndSet(this, TAGS_NONE, TAGS_RUNNING)) {
                return;
            }
            long[][] highs = null;
            int[][] lows = null;
            try {
                var hook = queue.tagHookForTests;
                if (hook != null) {
                    hook.run();
                }
                var encrypted = queue.encrypted;
                var count = runs;
                var made = new long[count][];
                var madeLows = new int[count][];
                for (var run = 0; run < count; run++) {
                    if (runEnds[run] >= 0) {
                        tagRun(run, crypto, encrypted, made, madeLows);
                    }
                }
                highs = made;
                lows = madeLows;
            } catch (RuntimeException | OutOfMemoryError _) {
                // The move tags them itself; a record released meanwhile, its runs gone, is not moved at all
            } finally {
                runHighs = highs;
                runLows = lows;
                tags = TAGS_DONE;
                var waiter = tagWaiter;
                if (waiter != null) {
                    LockSupport.unpark(waiter);
                }
            }
        }

        /// {@return whether the thread that tags ahead published the tags of the runs, [#runHighs] and [#runLows]; with
        /// `runLows` `null` if it made none, having failed; `false` if the move claimed them before that thread
        /// started, or that thread did not end within [SpillQueue#TAGS_WAIT] nanoseconds}
        ///
        /// Waits for that thread if it is computing them: it started, so it holds a carrier and takes no lock until it
        /// is done, whatever the application's virtual threads hold. The wait is bounded all the same, the move then
        /// tagging the blocks itself, so no move waits on a thread that lost its carrier. An interrupt does not end the
        /// wait, and is kept.
        private boolean takeTags(SpillQueue queue) {
            if (TAGS.compareAndSet(this, TAGS_NONE, TAGS_DONE)) {
                return false;
            }
            if (tags == TAGS_RUNNING) {
                tagWaiter = Thread.currentThread();
                var interrupted = false;
                var deadline = System.nanoTime() + queue.tagsWait;
                long left;
                while (tags == TAGS_RUNNING && (left = deadline - System.nanoTime()) > 0) {
                    LockSupport.parkNanos(this, left);
                    interrupted |= Thread.interrupted();
                }
                tagWaiter = null;
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                if (tags == TAGS_RUNNING) {
                    return false;
                }
            }
            var lows = runLows;
            if (lows != null) {
                var tagged = 0;
                for (var run : lows) {
                    if (run != null) {
                        tagged++;
                    }
                }
                queue.taggedAhead.addAndGet(tagged);
            }
            return true;
        }

        /// {@return whether the values on shared arrays move an array at a time ([#CHUNKS])}
        boolean chunked() {
            return chunked;
        }

        /// Forgets the values.
        private void release() {
            versions = null;
            offsets = null;
            count = 0;
            dropRuns();
            runHighs = null;
            runLows = null;
        }
    }

    /// The ciphers of one thread at a time: those that authenticate a value moved to the log and decrypt it back.
    ///
    /// A value is decrypted by an AES-CTR cipher started at the counter block that holds its first byte, one cipher per
    /// key of the last few segment uses read: a value of a use read before only sets a new counter and keeps the key
    /// schedule. The keystream is made and applied by the JDK's counter mode, which the log's AES-GCM runs on too, so
    /// it is compiled by the time the first checkpoint after an open reads thousands of values back at once, where a
    /// loop of this class would still run in the interpreter.
    ///
    /// The tag of a value in an encrypted store is a pair of polynomials over the field of [#PRIME] elements, evaluated
    /// at the two secret points of the queue: one coefficient per 7 bytes of the value, after a leading 1 and the
    /// version's stamp and before the value's length, so distinct values, or the same value of another version, are
    /// distinct polynomials.
    /// Two distinct polynomials of `n` coefficients agree at a uniformly drawn point with a probability of at most
    /// `n / (2^61 - 1)`, and the two points are independent. A tag never leaves the process, so bytes changed in the
    /// file, whatever change the CTR keystream lets anyone make, pass for the value with a probability of at most
    /// `n / (2^61 - 1)` times `n / (2^61 - 1) + 2^-32`, the second factor that of the 32 bits a tag keeps of the second
    /// polynomial: below 2^-80 for a value of 12 KiB, about the bound of a 96-bit GMAC of the same value, below 2^-75
    /// for one of 1 MiB, the longest that moves with the default large values, and below 2^-64 for the longest a frame
    /// holds, which a store without a file of large values may move.
    ///
    /// The polynomials are evaluated four coefficients of the value at a time, as the sum of each times its power of the
    /// point, so that the four products do not wait for one another, reduced modulo the prime once for the four; the
    /// result is the one Horner's rule gives.
    ///
    /// @implNote This implementation tags with a universal hash of its own, not a MAC of the JDK: a store that recovers
    /// a long log after a crash tags every value it replays while the JVM has compiled little, when the C2 compilation
    /// of an HMAC inlined at its call site takes hundreds of milliseconds of a compiler thread, and a GMAC takes an
    /// initialization and two copies of the value per tag; the loop of the polynomials compiles in a few.
    static final class Crypto {
        // AES-CTR ciphers, each for the key at the same index of ctrKeys; null before the first decryption under that
        // key and after one that failed
        private final Cipher[] ctrs;
        private final SecretKeySpec[] ctrKeys;
        // The AES-GCM cipher that opens the frames of framed values, or null before the first and after one that
        // failed other than by authentication
        private Cipher gcm;
        // The index the next key without a cipher takes
        private int nextKey;
        // The counter block a value's keystream starts from, and the keystream of the bytes of that block before it
        private final byte[] counter;
        private final byte[] skipped;
        // The points of the polynomials of the tags, or null without encryption
        private final long[] keys;
        // The second, third and fourth powers of the points, in that order for the first and then for the second, or
        // null without encryption
        private final long[] powers;
        private final CRC32C crc;
        // The two polynomials of the last tag
        private long first;
        private long second;
        private byte[] scratch;
        // A buffer over scratch, which a read of the log fills, or null before the first
        private ByteBuffer scratchBuffer;

        private Crypto(SpillQueue queue) {
            this.keys = queue.encrypted ? queue.tagKeys() : null;
            this.powers = keys == null ? null : powersOf(keys);
            this.crc = new CRC32C();
            this.ctrs = new Cipher[KEPT_KEYS];
            this.ctrKeys = new SecretKeySpec[KEPT_KEYS];
            this.counter = new byte[BLOCK];
            this.skipped = new byte[BLOCK];
        }

        /// {@return the second, third and fourth powers of the two points of `keys`, those of the first and then those
        /// of the second}
        private static long[] powersOf(long[] keys) {
            var powers = new long[6];
            for (var i = 0; i < 2; i++) {
                var point = keys[i];
                var square = multiply(point, point);
                var cube = multiply(square, point);
                powers[3 * i] = square;
                powers[3 * i + 1] = cube;
                powers[3 * i + 2] = multiply(cube, point);
            }
            return powers;
        }

        /// {@return an array of at least `length` bytes, which only this holder uses until it is given back}
        byte[] scratch(int length) {
            var array = scratch;
            if (array == null || array.length < length) {
                array = new byte[Math.max(length, 2 << 10)];
                scratch = array;
                scratchBuffer = null;
            }
            return array;
        }

        /// {@return a buffer over the whole of the array [#scratch(int)] last returned, made once per array, whose
        /// position and limit the caller sets}
        ByteBuffer scratchBuffer() {
            var buffer = scratchBuffer;
            if (buffer == null) {
                buffer = ByteBuffer.wrap(scratch);
                scratchBuffer = buffer;
            }
            return buffer;
        }

        private void trim() {
            if (scratch != null && scratch.length > KEPT_SCRATCH) {
                scratch = null;
                scratchBuffer = null;
            }
        }

        /// {@return the place in the log of the `length` bytes at `offset` of `home`, the value of the version stamped
        /// `stamp`, which lies at `bodyIndex` of the body of the frame at `frameOffset` of `segment`}
        ///
        /// A read-only segment hides its array, so the value is copied into the scratch to be tagged.
        LogValue logValue(LogSegmentView segment, long frameOffset, int bodyIndex, long stamp, MemorySegment home,
                          int offset, int length) {
            var value = scratch(length);
            MemorySegment.copy(home, offset, MemorySegment.ofArray(value), 0, length);
            return logValue(segment, frameOffset, bodyIndex, stamp, value, 0, length);
        }

        /// {@return the place in the log of the `length` bytes at `offset` of `value`, the value of the version stamped
        /// `stamp`, which lies at `bodyIndex` of the body of the frame at `frameOffset` of `segment`, tagged where they
        /// lie}
        LogValue logValue(LogSegmentView segment, long frameOffset, int bodyIndex, long stamp, byte[] value,
                          int offset, int length) {
            if (keys == null) {
                crc.reset();
                crc.update(value, offset, length);
                return new LogValue(segment, frameOffset, bodyIndex, 0, (int) crc.getValue());
            }
            tag(value, offset, length, stamp);
            return new LogValue(segment, frameOffset, bodyIndex, first, (int) second);
        }

        /// Decrypts the `length` bytes after the first `skip` of `sealed` into `target` from `offset`: the bytes from the
        /// block holding the value's first byte at `bodyIndex` of the body of the frame at `frameOffset`, sealed under
        /// `key`. The bytes of `sealed` stay as they are.
        ///
        /// A body is sealed from the counter block after the first, `nonce || 2`, so the block of a byte at `i` of it
        /// is `nonce || 2 + i / 16`; a body holds far fewer than 2^36 bytes, so the counter never carries into the
        /// nonce.
        ///
        /// @throws IllegalStateException if the cipher fails, which leaves its state unknown, so it is not kept
        void decrypt(SecretKeySpec key, long frameOffset, int bodyIndex, byte[] sealed, int skip, int length,
                     byte[] target, int offset) {
            var slot = ctr(key);
            var cipher = ctrs[slot];
            Bytes.i32(counter, 0, 0);
            Bytes.i64(counter, Integer.BYTES, frameOffset);
            Bytes.i32(counter, Integer.BYTES + Long.BYTES, 2 + (bodyIndex >>> 4));
            try {
                // The key the cipher last had, whose schedule it keeps
                cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(counter));
                if (skip > 0) {
                    cipher.update(sealed, 0, skip, skipped, 0);
                }
                cipher.update(sealed, skip, length, target, offset);
            } catch (GeneralSecurityException | RuntimeException exception) {
                ctrs[slot] = null;
                ctrKeys[slot] = null;
                throw new IllegalStateException("AES failed", exception);
            }
        }

        /// Decrypts in place the `length` sealed bytes at `offset` of `value`, the value at `bodyIndex` of the body of the
        /// frame at `frameOffset`, sealed under `key`, with the AES-CTR cipher of `key` as
        /// [#decrypt(SecretKeySpec, long, int, byte\[\], int, int, byte\[\], int)] does: the keystream comes from the
        /// counter blocks alone, so the bytes before the value in its first block are not needed.
        ///
        /// The cipher decrypts a piece of at most [#KEPT_SCRATCH] bytes at a time into the scratch, which is copied
        /// back: a piece far shorter than the chunks the JDK's counter mode cuts a call into keeps its loop over them
        /// out of the code the JIT compiled from the seals, and the cipher would copy the input of a call that writes
        /// over it.
        ///
        /// @throws IllegalStateException if the cipher fails, which leaves its state unknown, so it is not kept
        void decryptInPlace(SecretKeySpec key, long frameOffset, int bodyIndex, byte[] value, int offset, int length) {
            var slot = ctr(key);
            var cipher = ctrs[slot];
            Bytes.i32(counter, 0, 0);
            Bytes.i64(counter, Integer.BYTES, frameOffset);
            Bytes.i32(counter, Integer.BYTES + Long.BYTES, 2 + (bodyIndex >>> 4));
            var skip = bodyIndex & (BLOCK - 1);
            var staging = scratch(KEPT_SCRATCH);
            try {
                cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(counter));
                if (skip > 0) {
                    // The keystream of the bytes before the value in its block, from any bytes
                    cipher.update(counter, 0, skip, skipped, 0);
                }
                for (var at = 0; at < length; ) {
                    var piece = Math.min(KEPT_SCRATCH, length - at);
                    cipher.update(value, offset + at, piece, staging, 0);
                    System.arraycopy(staging, 0, value, offset + at, piece);
                    at += piece;
                }
            } catch (GeneralSecurityException | RuntimeException exception) {
                ctrs[slot] = null;
                ctrKeys[slot] = null;
                throw new IllegalStateException("AES failed", exception);
            }
        }

        /// Opens the frame at `frameOffset` of a segment use sealed under `key`, read whole into `sealed`: its header,
        /// then its body of `body` bytes sealed, then its tag; the body goes to `plain` from its start.
        ///
        /// The frame is the AES-GCM message the log sealed, the frame's offset its nonce and its header the additional
        /// data, so the tag authenticates the header, the body and the place of the frame under the key of the use.
        /// `plain` is a separate array: the JDK's GCM runs its combined intrinsic only when it does not decrypt in
        /// place.
        ///
        /// @return whether the frame authenticates; if not, `plain` holds no byte of it
        /// @throws IllegalStateException if the cipher fails otherwise, which leaves its state unknown, so it is not
        ///                               kept
        boolean openFrame(SecretKeySpec key, long frameOffset, byte[] sealed, int body, byte[] plain) {
            try {
                var cipher = gcm;
                if (cipher == null) {
                    cipher = Cipher.getInstance("AES/GCM/NoPadding");
                    gcm = cipher;
                }
                cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(LogWriter.TAG_BYTES * 8, LogWriter.nonce(frameOffset)));
                cipher.updateAAD(sealed, 0, LogWriter.FRAME_HEADER);
                cipher.doFinal(sealed, LogWriter.FRAME_HEADER, body + LogWriter.TAG_BYTES, plain, 0);
                return true;
            } catch (AEADBadTagException _) {
                return false;
            } catch (GeneralSecurityException | RuntimeException exception) {
                gcm = null;
                throw new IllegalStateException("AES-GCM failed", exception);
            }
        }

        /// {@return the index of the AES-CTR cipher for `key`, made now in place of the one whose key came the longest
        /// ago if none is}
        private int ctr(SecretKeySpec key) {
            for (var slot = 0; slot < KEPT_KEYS; slot++) {
                if (ctrKeys[slot] == key && ctrs[slot] != null) {
                    return slot;
                }
            }
            var slot = nextKey;
            nextKey = (slot + 1) % KEPT_KEYS;
            if (ctrs[slot] == null) {
                try {
                    ctrs[slot] = Cipher.getInstance("AES/CTR/NoPadding");
                } catch (GeneralSecurityException exception) {
                    throw new IllegalStateException("AES is not available", exception);
                }
            }
            ctrKeys[slot] = key;
            return slot;
        }

        /// {@return whether the `length` bytes at `offset` of `value` are the value `logged` holds the tag of, of the
        /// version stamped `stamp`}
        boolean verifies(byte[] value, int offset, int length, long stamp, LogValue logged) {
            if (keys == null) {
                crc.reset();
                crc.update(value, offset, length);
                return (int) crc.getValue() == logged.tagLow();
            }
            tag(value, offset, length, stamp);
            // Every bit compared whatever the first that differs
            return ((first ^ logged.tagHigh()) | ((int) second ^ logged.tagLow())) == 0;
        }

        /// Tags the `length` bytes at `offset` of `block`, block `index` of a [LogChunk] of the commit at `sequence`,
        /// bound to its place by `place` ([LogChunk#place(int, int)]): the first polynomial goes to `highs`, unless
        /// it is `null`, as it is without encryption, and the low 32 bits of the second, or the CRC-32C of the block
        /// without encryption, to `lows`.
        void tagBlock(byte[] block, int offset, int length, long sequence, long place, long[] highs, int[] lows,
                      int index) {
            if (keys == null) {
                crc.reset();
                crc.update(block, offset, length);
                lows[index] = (int) crc.getValue();
                return;
            }
            tag(block, offset, length, sequence, place);
            highs[index] = first;
            lows[index] = (int) second;
        }

        /// {@return whether the `length` bytes at `offset` of `block` are the block of a [LogChunk] whose tag is
        /// `high` and `low`, of the commit at `sequence` and bound to its place by `place`}
        boolean verifiesBlock(byte[] block, int offset, int length, long sequence, long place, long high, int low) {
            if (keys == null) {
                crc.reset();
                crc.update(block, offset, length);
                return (int) crc.getValue() == low;
            }
            tag(block, offset, length, sequence, place);
            // Every bit compared whatever the first that differs
            return ((first ^ high) | ((int) second ^ low)) == 0;
        }

        /// Evaluates the two polynomials of the tag of the `length` bytes at `offset` of `value`, the value of the
        /// version stamped `stamp`, into [#first] and [#second]: by Horner's rule, the coefficients 1, the stamp in two
        /// words, the value in words of 7 bytes, the last one shorter, and the length, four of the value's words at a
        /// time while four whole ones are left.
        private void tag(byte[] value, int offset, int length, long stamp) {
            tag(value, offset, length, stamp >>> 8, stamp & 0xFF);
        }

        /// Evaluates the two polynomials of a tag into [#first] and [#second], as [#tag(byte\[\], int, int, long)]
        /// does, with `head` and `tail` for the two coefficients after the leading 1, each below 2^61.
        private void tag(byte[] value, int offset, int length, long head, long tail) {
            var x = keys[0];
            var y = keys[1];
            // The leading coefficient 1, so no coefficient after it can be dropped as a leading zero
            var a = x;
            var b = y;
            var word = head;
            a = multiply(a + word, x);
            b = multiply(b + word, y);
            word = tail;
            a = multiply(a + word, x);
            b = multiply(b + word, y);
            var end = offset + length;
            var at = offset;
            var x2 = powers[0];
            var x3 = powers[1];
            var x4 = powers[2];
            var y2 = powers[3];
            var y3 = powers[4];
            var y4 = powers[5];
            // Four words at a time, the last read the 8 bytes after the first three; the four products, folded below 2^63
            // but not reduced, sum below 2^64, which one reduction takes below the prime
            for (; at + 3 * WORD + Long.BYTES <= end; at += 4 * WORD) {
                var w0 = Bytes.i64(value, at) >>> 8;
                var w1 = Bytes.i64(value, at + WORD) >>> 8;
                var w2 = Bytes.i64(value, at + 2 * WORD) >>> 8;
                var w3 = Bytes.i64(value, at + 3 * WORD) >>> 8;
                a = reduce(fold(a + w0, x4) + fold(w1, x3) + fold(w2, x2) + fold(w3, x));
                b = reduce(fold(b + w0, y4) + fold(w1, y3) + fold(w2, y2) + fold(w3, y));
            }
            // A long read takes 8 bytes: the top 7 of it are a word
            for (; at + Long.BYTES <= end; at += WORD) {
                word = Bytes.i64(value, at) >>> 8;
                a = multiply(a + word, x);
                b = multiply(b + word, y);
            }
            for (; at < end; at += WORD) {
                word = 0;
                var stop = Math.min(at + WORD, end);
                for (var i = at; i < stop; i++) {
                    word = (word << 8) | (value[i] & 0xFF);
                }
                a = multiply(a + word, x);
                b = multiply(b + word, y);
            }
            first = multiply(a + length, x);
            second = multiply(b + length, y);
        }

        /// {@return `sum`, read as unsigned, modulo [#PRIME]}
        private static long reduce(long sum) {
            var reduced = (sum & PRIME) + (sum >>> 61);
            return reduced >= PRIME ? reduced - PRIME : reduced;
        }

        /// {@return the two polynomials of the tag of the `length` bytes at `offset` of `value`, of the version stamped
        /// `stamp`, and the two points they are evaluated at, in this order}
        long[] tagForTests(byte[] value, int offset, int length, long stamp) {
            tag(value, offset, length, stamp);
            return new long[]{first, second, keys[0], keys[1]};
        }

        /// {@return a number below 2^63 that is `a` times `b` modulo [#PRIME], for `a` below 2^62 and `b` below 2^61}
        ///
        /// The 123 bits of the product fold into 64 as [#multiply(long, long)] folds them, without the reduction after.
        private static long fold(long a, long b) {
            var high = Math.unsignedMultiplyHigh(a, b);
            var low = a * b;
            return (high << 3) + (low >>> 61) + (low & PRIME);
        }

        /// {@return `a` times `b` modulo [#PRIME], both below 2^62}
        static long multiply(long a, long b) {
            var high = Math.unsignedMultiplyHigh(a, b);
            var low = a * b;
            // 2^61 is 1 modulo the prime: fold the 124 bits of the product into 64, then into 61
            var folded = (high << 3) + (low >>> 61) + (low & PRIME);
            var reduced = (folded & PRIME) + (folded >>> 61);
            return reduced >= PRIME ? reduced - PRIME : reduced;
        }
    }
}

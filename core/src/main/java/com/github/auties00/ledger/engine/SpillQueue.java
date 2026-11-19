package com.github.auties00.ledger.engine;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
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
/// A record whose every value that moves is at least [#THROUGH_VALUE_MIN] bytes on an array of its own
/// ([Record#eager()]) is not counted in the budget: its values leave the heap as soon as its frame is durable, in the order of the commits with the records of
/// the budget, as one positioned read brings such a value back about as fast as it was written, a framed one
/// authenticated by its frame's tag, where keeping it would hold an array of 128 KiB or more per commit. The first read
/// of such a value written moments ago reads it back from the log.
///
/// A record leaves the queue once its values moved, or once a published checkpoint holds its commit. Values are not
/// moved:
/// - of a batch's shared arrays in a key space until samples of its puts show that it replaces or removes few of
///   its values before a checkpoint ([ValueSlab.Overwrites#spills()]): a record lists versions, which keep their
///   batch's arrays alive until the record leaves the queue, and moving a value no reader can reach any more costs a
///   tag of its bytes for nothing. A version listed and then replaced counts in the held bytes until its record
///   leaves, a share of them the samples keep under an eighth. A value on an array of its own moves without them: the
///   array is the value's alone and lives as long as its version in any case, so the record keeps no other bytes
///   alive. In a key space that replaces such values before a checkpoint, the thread tags some of them for nothing,
///   and those of a record the budget counts stay until it leaves, at most the budget of them;
/// - of a group the log wrote through to stable storage, whose pages that write took out of the system's cache, so a
///   read back would go to the device, unless every value the record lists is at least [#THROUGH_VALUE_MIN] bytes;
/// - of a batch on a [ValueSlab]'s chunks, which the slab bounds already, even beside a value on an array of its
///   own, of a transaction, or of one with a unit of [BlobFile]. A value a recovery copied to an array of its own
///   moves, to the log only;
/// - of a commit whose values on shared arrays would take, once moved, more than half the bytes they take now
///   ([Record#settle()]), a value shorter than [#PAYING_VALUE_MIN] counted at the array of its own it would take, or
///   at its length if a bulk commit's run packs it in a chunk of its own: a value shorter than [#LOG_VALUE_MIN] goes
///   to such an array or chunk, which frees the array it shared only once every other value on it left too, and one
///   of 96 to 255 bytes goes to the log, saving less than 216 bytes of heap for a read back of its own by the next
///   checkpoint. So a commit of mostly short values, like an app-state snapshot of 100 to
///   300 byte values, stays where it is rather than trade its frame for a [LogValue] and a positioned read per value, a
///   hundred thousand of each for a snapshot, its frame leaving the heap only once the thread moved the last of them,
///   well after the commit is durable. The commit's values on arrays of their own move all the same, each freeing its
///   array for a [LogValue].
///
/// The puts a bulk commit keeps in a run of its own ([PackedRun]) list in its record as the run's entries rather than as
/// versions, and move as a version's value would, each by one switch of the entry's word ([RunValues]).
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
/// ([LiveLedger#replay(long, byte\[\], int, int, FrameBody.ValueFrames, ReplayValues, LogSegmentView, long, long)]).
final class SpillQueue {
    /// The shortest value that moves to the log rather than to an array of its own, or of a [PackedRun] to a chunk of
    /// the run's: a [LogValue] takes 40 bytes, an array of the value 16 more than the value, 8-aligned, so from 96 bytes
    /// the log saves at least 72 bytes of heap per value, and a value a run keeps in the log takes 32 bytes of the run's
    /// arrays (its stamp, its place, where it lies in its body and its tag) where one in a chunk takes its length, a
    /// third or less from 96 bytes; each for a positioned read back by the checkpoint that writes it. Below, the log
    /// saves too little heap to pay for the read.
    static final int LOG_VALUE_MIN = 96;
    /// The shortest value that [Record#settle()] counts at the 40 bytes of its [LogValue]; a shorter one counts at the
    /// array of its own it would take below [#LOG_VALUE_MIN], whether it goes to the log or not. A value of 96 to 255
    /// bytes so moves to the log beside longer values whose move pays, as the arrays they share leave the heap only once
    /// every value on them moved, but never makes a record's move pay alone: it saves less than 216 bytes for a read back
    /// of its own, and a commit of thousands of them would leave its frame on the heap until the thread had tagged and
    /// placed the last one.
    static final int PAYING_VALUE_MIN = 256;
    /// The bytes a [LogValue] takes on the heap.
    private static final int LOG_VALUE_BYTES = 40;
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
    private final boolean encrypted;
    // The two points the polynomials of the tags are evaluated at, from 1 to PRIME - 1, drawn for this open at the
    // first value moved or read back and never stored; null before, and without encryption
    private volatile long[] tagKeys;
    private final Object lock;
    // The records not taken that the budget counts, oldest first; guarded by the lock
    private final ArrayDeque<Record> records;
    // The records not taken whose values all leave once durable, outside the budget ([Record#eager()]), oldest first;
    // guarded by the lock
    private final ArrayDeque<Record> eager;
    // The sequence of the first record of eager, Long.MAX_VALUE when it is empty, which the log's durability path
    // reads without the lock; written holding the lock
    private volatile long eagerHead = Long.MAX_VALUE;
    // The segments values moved to, ordered by number, which no checkpoint has retired; guarded by the lock
    private final ArrayList<LogSegmentView> segments;
    // The bytes of values the records of the queue the budget counts list; written holding the lock
    private volatile long held;
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
    // The room for values of the records drained, for tests; guarded by the lock
    private long drainedRoom;
    // The records whose values on shared arrays stay, as their move would not pay, for tests
    private final AtomicLong unpaidRecords = new AtomicLong();

    /// Creates the queue of a store.
    ///
    /// @param budget the bytes of values whose frames are durable the queue holds before it moves them
    /// @param encrypted whether the store's log is encrypted, so the values read back are authenticated by a keyed hash
    SpillQueue(long budget, boolean encrypted) {
        this.budget = budget;
        this.lowWater = budget - budget / 4;
        this.encrypted = encrypted;
        this.lock = new Object();
        this.records = new ArrayDeque<>();
        this.eager = new ArrayDeque<>();
        this.segments = new ArrayList<>();
        this.running = new AtomicBoolean();
        this.cryptos = new AtomicReferenceArray<>(Math.max(4, 2 * Runtime.getRuntime().availableProcessors()));
        this.pool = HeapPool.process();
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

    /// Reads the latest durable sequence from `log` from now on, and moves values whenever the queue is over budget or
    /// holds a durable record the budget does not count.
    void useLog(LogWriter log) {
        useDurable(log::durableSequence);
    }

    /// Reads the latest durable sequence from `durableSequence` from now on, a thread moving values included, and moves
    /// values whenever the queue is over budget or holds a durable record the budget does not count.
    void useDurable(LongSupplier durableSequence) {
        this.durable = durableSequence;
        if (over() || eagerDurable(durableSequence)) {
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

    /// {@return the bytes of the log's last budget, whose values a recovery keeps on the heap ([#keepsReplayed(long)])}
    long replayBudget() {
        return budget;
    }

    /// Records that a recovery moves values of the commit at `sequence` it replays to `segment`, before the first
    /// one moves there ([#moveReplayed(Crypto, Version, LogSegmentView, long, int, byte\[\])]), so a checkpoint that
    /// retires segments sees the commit.
    void noteReplayed(long sequence, LogSegmentView segment) {
        synchronized (lock) {
            note(segment, sequence);
        }
    }

    /// Records that the values of the commit at `sequence` move to `segment`, which joins the segments at its place by
    /// number if it is the first commit to; holding the lock.
    ///
    /// The segments stay ordered by number whatever order commits move in: a record of the budget may stay queued while
    /// later records outside it move to newer segments, and [#published(long, long, long)],
    /// [#retirableBefore(long, ReaderTable)] and [#holdsBack()] stop at the first segment past their bound.
    private void note(LogSegmentView segment, long sequence) {
        if (!segment.note(sequence)) {
            return;
        }
        var number = segment.number();
        var at = segments.size();
        // Nearly always the newest, so the search starts from the end
        while (at > 0 && segments.get(at - 1).number() > number) {
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

    /// Tags a value a recovery replays into a [PackedRun], the `length` bytes at `bodyIndex` of `frame`, the opened
    /// body of a frame the log holds durably, of the write stamped `stamp`, where they lie, and counts it among the
    /// values in the log. The tag is left in `crypto` ([Crypto#tagHigh()], [Crypto#tagLow()]), which the run records
    /// with the value's place, so no [LogValue] is made per value.
    ///
    /// @param crypto the caller's, until it gives it back ([#takeCrypto()])
    void tagReplayedInto(Crypto crypto, long stamp, byte[] frame, int bodyIndex, int length) {
        crypto.tagOnly(frame, bodyIndex, length, stamp);
        loggedValues.incrementAndGet();
    }

    /// Queues `record`, of a published commit after every one queued, and starts moving values if the queue is over
    /// its budget, or if the budget does not count the record ([Record#eager()]) and its commit is durable already.
    void add(Record record) {
        if (record.count == 0 && record.runs == null) {
            return;
        }
        var settled = record.settle();
        if (settled != Record.ALL) {
            unpaidRecords.incrementAndGet();
            if (settled == Record.NONE) {
                record.release();
                return;
            }
        }
        var outside = record.eager();
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (outside) {
                if (eager.isEmpty()) {
                    eagerHead = record.sequence;
                }
                eager.addLast(record);
            } else {
                records.addLast(record);
                held = held + record.bytes;
            }
        }
        // The log may have made the group durable, and found no record to move, before the record was queued: the
        // write of the head above and the read of the durable sequence here, both volatile, order against the log's
        // write of the durable sequence and read of the head in durableAdvanced, so one of the two starts the thread
        if (outside ? record.sequence <= current.getAsLong() : over()) {
            start();
        }
    }

    /// Starts moving values if the queue is over its budget or its oldest record the budget does not count is durable;
    /// called by the log as a group becomes durable, which may make the oldest record's values movable. Takes no lock.
    void durableAdvanced() {
        if (!running.get() && (over() || eagerDurable(current))) {
            start();
        }
    }

    /// {@return whether the oldest record the budget does not count is durable by `durableSequence`, without the lock}
    private boolean eagerDurable(LongSupplier durableSequence) {
        var head = eagerHead;
        // Before the store has a log every sequence is durable, Long.MAX_VALUE included, which no record has
        return head != Long.MAX_VALUE && head <= durableSequence.getAsLong();
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

    /// {@return whether the oldest record the budget does not count is durable, or the queue is over its budget and its
    /// oldest record's frame is durable}
    private boolean movable() {
        var log = durable;
        if (eagerDurable(log)) {
            return true;
        }
        if (!over()) {
            return false;
        }
        synchronized (lock) {
            var first = records.peekFirst();
            return first != null && first.sequence <= log.getAsLong();
        }
    }

    /// Moves the values of the oldest records whose commits `durableSequence` gives durable: those the budget does not
    /// count, and those it counts once the queue holds more than its budget, until it holds three quarters of it, on the
    /// caller's thread.
    void drain(LongSupplier durableSequence) {
        drain(durableSequence, lowWater);
    }

    /// Moves the values of the oldest records whose commits `durableSequence` gives durable, in the order of their
    /// commits, on the caller's thread: those the budget does not count, and those it counts once the queue holds more
    /// than its budget, until it holds `downTo` bytes or fewer, or from the first if `downTo` is negative.
    ///
    /// A drain a durable record outside the budget started moves no record the budget counts while the queue holds no
    /// more than its budget, so the newest values of the budget stay where they are whatever lengths are committed.
    private void drain(LongSupplier durableSequence, long downTo) {
        var crypto = takeCrypto();
        // Whether the drain moves records the budget counts: from when the queue holds more than its budget to when it
        // holds downTo bytes or fewer
        var trimming = downTo < 0;
        try {
            while (!closed) {
                Record record;
                synchronized (lock) {
                    // Read before the records' locations, which the log writer sets before their frames are durable
                    var durableNow = durableSequence.getAsLong();
                    if (held > budget) {
                        trimming = true;
                    } else if (held <= downTo) {
                        trimming = false;
                    }
                    var counted = records.peekFirst();
                    if (counted != null && (!trimming || counted.sequence > durableNow)) {
                        counted = null;
                    }
                    var outside = eager.peekFirst();
                    if (outside != null && outside.sequence > durableNow) {
                        outside = null;
                    }
                    if (outside != null && (counted == null || outside.sequence < counted.sequence)) {
                        record = outside;
                        eager.pollFirst();
                        var next = eager.peekFirst();
                        eagerHead = next == null ? Long.MAX_VALUE : next.sequence;
                    } else if (counted != null) {
                        record = counted;
                        records.pollFirst();
                        held = held - record.bytes;
                    } else {
                        return;
                    }
                    drainedRecords.incrementAndGet();
                    drainedRoom += record.versions.length;
                    if (record.logs()) {
                        // Before any value moves there, so a checkpoint that retires segments sees the commit
                        note(record.segment, record.sequence);
                    }
                }
                record.spill(this, crypto);
            }
        } finally {
            giveBack(crypto);
        }
    }

    /// Moves the values of every record whose commit `durableSequence` gives durable, whatever the budget, on the
    /// caller's thread; for tests.
    void drainAllForTests(LongSupplier durableSequence) {
        drain(durableSequence, -1);
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
            }
            if ((record = eager.peekFirst()) != null && record.sequence <= cut) {
                do {
                    eager.pollFirst();
                    record.release();
                } while ((record = eager.peekFirst()) != null && record.sequence <= cut);
                eagerHead = record == null ? Long.MAX_VALUE : record.sequence;
            }
            for (var segment : segments) {
                if (segment.number() >= logStart) {
                    break;
                }
                segment.cover(latest);
            }
        }
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
            for (var record : eager) {
                record.release();
            }
            eager.clear();
            eagerHead = Long.MAX_VALUE;
        }
        retireAll();
    }

    /// {@return the bytes of values the queued records the budget counts list; for tests}
    long heldForTests() {
        return held;
    }

    /// {@return the sequences of the queued records the budget does not count, oldest first; for tests}
    long[] eagerSequencesForTests() {
        synchronized (lock) {
            return eager.stream().mapToLong(Record::sequence).toArray();
        }
    }

    /// {@return the room for values of the records whose values a drain took to move so far; for tests}
    long drainedRoomForTests() {
        synchronized (lock) {
            return drainedRoom;
        }
    }

    /// {@return the values moved to the log so far; for tests}
    long loggedValuesForTests() {
        return loggedValues.get();
    }

    /// {@return the values moved to arrays of their own so far; for tests}
    long copiedValuesForTests() {
        return copiedValues.get();
    }

    /// {@return the room for values of each record the queue holds that the budget counts, oldest first; for tests}
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
    /// ([Record#settle()]), whether or not the record's values on arrays of their own move; for tests}
    long unpaidRecordsForTests() {
        return unpaidRecords.get();
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
        // The entries of the runs of the commit's bulk puts it lists, a run's at a time, or null before the first
        private RunValues[] runs;
        /// The verdict of [#settle()] that every value of the record may move.
        static final int ALL = 0;
        /// The verdict of [#settle()] that only the record's values on arrays of their own may move.
        static final int OWN = 1;
        /// The verdict of [#settle()] that no value of the record moves.
        static final int NONE = 2;

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
        // Whether only the values on arrays of their own move ([#settle()])
        private boolean ownOnly;
        // Set by the log writer as it seals the frame: the segment use, the frame's offset, where the commit's body
        // starts in the frame's body, the bytes of the frame's body, 0 if unknown, and whether the group was written
        // through; for a split commit, those of its head, and where its value frames lie
        private LogSegmentView segment;
        private long frameOffset;
        private int commitStart;
        private int frameBody;
        private boolean through;
        private ValueFramePlaces valueFrames;

        private Record(long sequence, int capacity) {
            this.sequence = sequence;
            this.versions = new Version[capacity];
            this.offsets = new int[capacity];
        }

        /// Lists `version`, whose value of `length` bytes starts at `offset` of the commit's body.
        ///
        /// @param offset where the value starts in the commit's body; or, for a value a split body left to its section,
        ///               `-1` minus where it starts in the section, which a value frame holds
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
                sharedMoved += length >= PAYING_VALUE_MIN ? LOG_VALUE_BYTES : (ARRAY_HEADER + length + 7) & -8;
            }
        }

        /// Lists `entry` of the run `values` names, whose value of `length` bytes starts at `offset` of the commit's
        /// body, the write `op` of the commit's batch, on an array other values share.
        ///
        /// @param offset where the value starts in the commit's body, or for a value a split body left to its section,
        ///               `-1` minus where it starts in the section, as [#add(Version, int, int)] takes it
        ///
        /// A value shorter than [#LOG_VALUE_MIN] counts in the verdict at its length, the bytes it takes packed in a
        /// chunk of the run's own ([PackedRun#spill]), where a version's would take an array of its own.
        void addRun(RunValues values, int entry, int op, int offset, int length) {
            if (runs == null) {
                runs = new RunValues[]{values};
            } else if (runs[runs.length - 1] != values && !lists(values)) {
                // One per key space a bulk commit keeps a run of, which the writes of a batch may interleave
                runs = Arrays.copyOf(runs, runs.length + 1);
                runs[runs.length - 1] = values;
            }
            values.add(entry, op, offset);
            bytes += length;
            large |= length >= LOG_VALUE_MIN;
            shortest = Math.min(shortest, length);
            sharedBytes += length;
            sharedMoved += length >= PAYING_VALUE_MIN ? LOG_VALUE_BYTES
                    : length >= LOG_VALUE_MIN ? (ARRAY_HEADER + length + 7) & -8 : length;
        }

        // Whether the record lists entries of the run `values` names already
        private boolean lists(RunValues values) {
            for (var listed : runs) {
                if (listed == values) {
                    return true;
                }
            }
            return false;
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

        /// Records how the split body of the commit groups its section into value frames, before the record is queued:
        /// value frame `i` holds the `lengths[i]` bytes of the section from `starts[i]`.
        void valueFrames(int[] starts, int[] lengths) {
            this.valueFrames = new ValueFramePlaces(starts, lengths);
        }

        /// Records where the value frames of the split commit lie, after [#locate] located its head: value frame `i`
        /// at `offsets[i]` of the same segment use.
        void locateValueFrames(long[] offsets) {
            valueFrames.offsets = offsets;
        }

        /// {@return the offset of the frame that holds the value listed at `offset` ([#add(Version, int, int)])}
        long frameOf(int offset) {
            if (offset >= 0) {
                return frameOffset;
            }
            var frame = valueFrames == null ? -1 : valueFrames.find(-1 - offset);
            return frame < 0 ? frameOffset : valueFrames.offsets[frame];
        }

        /// {@return where the value listed at `offset` ([#add(Version, int, int)]) starts in the body of its frame}
        int indexOf(int offset) {
            if (offset >= 0) {
                return commitStart + offset;
            }
            var start = -1 - offset;
            var frame = valueFrames == null ? -1 : valueFrames.find(start);
            return frame < 0 ? 0 : start - valueFrames.starts[frame];
        }

        /// {@return the bytes of the body of the frame that holds the value listed at `offset`
        /// ([#add(Version, int, int)]), 0 if unknown}
        int bodyOf(int offset) {
            if (offset >= 0) {
                return frameBody;
            }
            var frame = valueFrames == null ? -1 : valueFrames.find(-1 - offset);
            return frame < 0 ? 0 : valueFrames.lengths[frame];
        }

        /// {@return the segment use that holds the commit's frames, or `null` if the store moves no value}
        LogSegmentView segment() {
            return segment;
        }

        /// Decides which values move, before the record is queued: all of them if moving those on shared arrays frees
        /// at least as much heap as it allocates, what they would take once moved (a [LogValue] for one of at least
        /// [#PAYING_VALUE_MIN] bytes and an array of its own for a shorter one, whether [#LOG_VALUE_MIN] sends it to the
        /// log or not, but its length for an entry of a bulk commit's run shorter than [#LOG_VALUE_MIN], which a chunk
        /// of the run packs) being at most half of their bytes, else only those on arrays of their own, whose bytes alone the
        /// record then counts.
        ///
        /// A batch's values lie packed on arrays of about their bytes, so moving them all frees about their bytes, and
        /// moving some frees nothing while the others keep the arrays alive; a commit of short values frees little heap
        /// for what its move costs, and leaves the heap it holds where it was. A value of at least [#LOG_VALUE_MIN] bytes
        /// on an array of its own frees that array whatever its record's other values do.
        ///
        /// @return [#ALL], [#OWN] or [#NONE]
        int settle() {
            if (sharedMoved <= sharedBytes / 2) {
                return ALL;
            }
            if (ownBytes == 0) {
                return NONE;
            }
            ownOnly = true;
            bytes = ownBytes;
            shortest = ownShortest;
            return OWN;
        }

        /// {@return whether, once [settled][#settle()], every value the record moves is on an array of its own and at
        /// least [#THROUGH_VALUE_MIN] bytes, so it leaves the heap once durable outside the budget}
        ///
        /// Such a record moves to the log even of a group written through ([#logs()]).
        boolean eager() {
            return ownBytes > 0 && bytes == ownBytes && ownShortest >= THROUGH_VALUE_MIN;
        }

        /// {@return the sequence of the commit}
        long sequence() {
            return sequence;
        }

        /// {@return whether values move to the log: some are long enough, and its frame is in a segment's cache or every
        /// value is long enough to read back from the device}
        private boolean logs() {
            return large && segment != null && (!through || shortest >= THROUGH_VALUE_MIN);
        }

        /// Moves the values, unless a long one cannot move to the log, then forgets them.
        private void spill(SpillQueue queue, Crypto crypto) {
            if (!large || logs()) {
                var logged = 0;
                var copied = 0;
                for (var i = 0; i < count; i++) {
                    var version = versions[i];
                    // A value on a shared array stays on a segment until a spill moves it, so one on an array is on its own
                    if (ownOnly && version.ownArray() == null) {
                        continue;
                    }
                    var offset = offsets[i];
                    var body = bodyOf(offset);
                    var framed = segment != null && framesAlone(segment, body, version.valueLength()) ? body : 0;
                    if (version.spill(crypto, segment, frameOf(offset), indexOf(offset), framed, null)) {
                        if (version.valueLength() >= LOG_VALUE_MIN) {
                            logged++;
                        } else {
                            copied++;
                        }
                    }
                }
                // On arrays other values share, so they stay when only the values on arrays of their own move
                for (var i = 0; runs != null && !ownOnly && i < runs.length; i++) {
                    var moved = runs[i].spill(crypto, this);
                    logged += moved[0];
                    copied += moved[1];
                }
                queue.loggedValues.addAndGet(logged);
                queue.copiedValues.addAndGet(copied);
            }
            release();
        }

        /// Forgets the values.
        private void release() {
            versions = null;
            offsets = null;
            count = 0;
            runs = null;
        }
    }

    /// Where the value frames of a split commit lie, and the value frame the last lookup found, from which the next one
    /// goes on: the values of a record are listed in the order of the writes, and so lie in the order of the section.
    private static final class ValueFramePlaces {
        private final int[] starts;
        private final int[] lengths;
        // Where each value frame starts in its segment, once the log located them; read after the commit is durable
        private long[] offsets;
        private int cursor;

        ValueFramePlaces(int[] starts, int[] lengths) {
            this.starts = starts;
            this.lengths = lengths;
        }

        /// {@return the value frame that holds the byte at `start` of the section, the last one for a start past the
        /// section, or -1 if there is none}
        int find(int start) {
            var count = starts.length;
            if (count == 0) {
                return -1;
            }
            var frame = cursor;
            if (start < starts[frame]) {
                frame = 0;
            }
            while (frame + 1 < count && starts[frame + 1] <= start) {
                frame++;
            }
            cursor = frame;
            return frame;
        }
    }

    /// The entries of the run of a bulk commit's puts to one key space that a [Record] lists: where each value starts in
    /// the commit's body and the position of its write in the batch, which its tag binds.
    ///
    /// Filled while the commit applies, before its run is built; the commit gives it the run before it queues the record.
    static final class RunValues {
        private PackedRun run;
        private int[] entries;
        private int[] offsets;
        private int[] ops;
        private int count;

        /// Creates the list of the entries of a run, with room for `capacity` of them.
        RunValues(int capacity) {
            var room = Math.max(1, capacity);
            this.entries = new int[room];
            this.offsets = new int[room];
            this.ops = new int[room];
        }

        private void add(int entry, int op, int offset) {
            if (count == entries.length) {
                entries = Arrays.copyOf(entries, 2 * count);
                offsets = Arrays.copyOf(offsets, 2 * count);
                ops = Arrays.copyOf(ops, 2 * count);
            }
            entries[count] = entry;
            offsets[count] = offset;
            ops[count++] = op;
        }

        /// Names the run the entries are of, built once the commit applied.
        void run(PackedRun run) {
            this.run = run;
        }

        /// {@return the values moved to the log and to a chunk, in this order: the run's [PackedRun#spill], the frames
        /// of the commit located by `record`}
        private int[] spill(Crypto crypto, Record record) {
            var target = run;
            if (target == null || count == 0) {
                return new int[2];
            }
            return target.spill(crypto, record, entries, offsets, ops, count);
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

        /// Tags the `length` bytes at `offset` of `value`, the value of the version stamped `stamp`, as
        /// [#logValue(LogSegmentView, long, int, long, byte\[\], int, int)] tags them, leaving the tag in [#tagHigh()]
        /// and [#tagLow()] until the next tag.
        void tagOnly(byte[] value, int offset, int length, long stamp) {
            if (keys == null) {
                crc.reset();
                crc.update(value, offset, length);
                first = 0;
                second = (int) crc.getValue();
                return;
            }
            tag(value, offset, length, stamp);
        }

        /// {@return the high part of the last tag [#tagOnly(byte\[\], int, int, long)] made, as [LogValue#tagHigh()]
        /// keeps it}
        long tagHigh() {
            return first;
        }

        /// {@return the low part of the last tag [#tagOnly(byte\[\], int, int, long)] made, as [LogValue#tagLow()]
        /// keeps it}
        int tagLow() {
            return (int) second;
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

        /// Evaluates the two polynomials of the tag of the `length` bytes at `offset` of `value`, the value of the
        /// version stamped `stamp`, into [#first] and [#second]: by Horner's rule, the coefficients 1, the stamp in two
        /// words, the value in words of 7 bytes, the last one shorter, and the length, four of the value's words at a
        /// time while four whole ones are left.
        private void tag(byte[] value, int offset, int length, long stamp) {
            var x = keys[0];
            var y = keys[1];
            // The leading coefficient 1, so no coefficient after it can be dropped as a leading zero
            var a = x;
            var b = y;
            var word = stamp >>> 8;
            a = multiply(a + word, x);
            b = multiply(b + word, y);
            word = stamp & 0xFF;
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

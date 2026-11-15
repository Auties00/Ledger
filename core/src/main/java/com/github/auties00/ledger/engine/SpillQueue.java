package com.github.auties00.ledger.engine;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.LongSupplier;
import java.util.zip.CRC32C;

/// The paged values waiting for a checkpoint that may leave the heap for the log, which holds them durably too.
///
/// A value a batch put to a paged key space stays on its batch's array until a checkpoint writes it, and the array
/// with it: up to six intervals of log, hundreds of megabytes after a history sync. Each commit with such values
/// queues a [Record] of them once it is published, which the log writer locates in the frame it seals. Once the queue
/// holds more than its budget of values whose frames are durable, a virtual thread moves the values of the oldest
/// records off their arrays, until three quarters of the budget is left: a value of at least [#LOG_VALUE_MIN] bytes to
/// its place in the log ([LogValue]), with a tag that authenticates it, a shorter one to an array of its own. A
/// batch's array no version needs any more is then garbage. The newest values, those of the budget, stay where they
/// are, so reads of recent writes do not change; a read of a value in the log reads it back with one positioned read.
///
/// The values of a group the log wrote through past the system's cache move too, once the thread read the frames that
/// hold them back from the device ([#WARM_SPAN]): that write left none of their pages in the cache, and a read of each
/// value from the device, as a checkpoint reads them one at a time in key order, would stall it. The read brings the
/// pages back into the cache, so later reads find the values there as they find those of a group that was forced. A
/// read of the frames that fails, or finds the segment retired, leaves the record's values where they are. Each value
/// read back is checked against its tag as any other: in an encrypted store a page that does not hold what was written
/// fails it; without encryption the check is the CRC-32C of the value, as for every value that moved.
///
/// A record leaves the queue once its values moved, or once a published checkpoint holds its commit. Values are not
/// moved:
/// - of a key space until samples of its commits show that it replaces or removes few of its values before a
///   checkpoint ([ValueSlab.Overwrites#spills()]): a record lists versions, which keep their batch's arrays alive until
///   the record leaves the queue, and moving a value no reader can reach any more costs a tag of its bytes for
///   nothing. A version listed and then replaced counts in the held bytes until its record leaves, a share of them
///   the samples keep under an eighth;
/// - of a batch on a [ValueSlab]'s chunks, which the slab bounds already, of a transaction, of a value a batch put on
///   an array of its own, or of one with a unit of [BlobFile]. A value a recovery copied to an array of its own moves,
///   to the log only;
/// - of a commit that a checkpoint in flight writes ([#checkpointTakes(long)]): its values leave the heap once that
///   checkpoint is published, and a value moved before the checkpoint reaches it is read back from the log, with a
///   read of its frame first if its group bypassed the system's cache, where the checkpoint copies it from its array
///   otherwise. The thread drops such a record unmoved, so the held bytes count only the commits the checkpoint leaves.
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
    /// The most bytes of log one read of frames written through covers, from the first it reads: the frames of the
    /// records after the one the thread is about to move are read with it while they are durable, in the same segment
    /// use, and among those the drain will take, so a drain reads them in a few sequential reads; the thread starts the
    /// read of the next span before it moves the values of the current one.
    static final int WARM_SPAN = 256 << 10;
    /// The bytes a read of frames written through takes at a time: the size of its direct buffer, so the read takes no
    /// temporary buffer of the JDK, and of the hold one read takes on a channel that serializes positioned reads, as
    /// on Windows, which a reader of a value of the segment may wait behind.
    private static final int WARM_PIECE = 64 << 10;
    /// The bytes of an AES block.
    static final int BLOCK = 16;
    /// The prime 2^61 - 1, the modulus of the polynomials of the tags.
    private static final long PRIME = (1L << 61) - 1;
    /// The bytes of a word of a tagged value: 7, so a word is below [#PRIME].
    private static final int WORD = 7;
    /// The scratch past which a [Crypto] given back lets go of it: the values of messages take a few kilobytes.
    private static final int KEPT_SCRATCH = 16 << 10;
    /// The keystream a [Crypto] makes in one call, a multiple of [#BLOCK]: its arrays of counter blocks and keystream
    /// keep this size, and a longer value is decrypted a piece at a time.
    private static final int KEYSTREAM = 4 << 10;
    private static final SecureRandom KEYS = new SecureRandom();

    private final long budget;
    // The held bytes the thread moves values down to
    private final long lowWater;
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
    // Whether the thread that moves values runs
    private final AtomicBoolean running;
    // The threads that move values started and not yet returned, the one a thread starts as it stops included
    private final AtomicInteger alive;
    private volatile boolean closed;
    // The sequence of the last commit a checkpoint in flight writes, or -1 while none runs; written holding the store's
    // publishing role
    private volatile long taken = -1;
    // The latest sequence the log holds durably; before the store has a log, the commits queued are those a recovery
    // replays from it, every one durable
    private volatile LongSupplier durable = () -> Long.MAX_VALUE;
    // Reads the supplier the queue holds at each call: a thread that started moving values during a recovery goes on
    // after the store attaches its log, and a supplier it took at its start would let it move the values of live
    // commits the log does not hold durably yet, and skip the volatile read of the log's durable sequence that orders
    // the writer's locating of a record before the thread reads it
    private final LongSupplier current = () -> durable.getAsLong();
    private final AtomicReferenceArray<Crypto> cryptos;
    // Guards what the reads of frames written through left: taken before the lock of the queue, never while holding it
    private final Object warmLock;
    // The segment use the last read of frames written through read, and where the bytes it read in order end; guarded
    // by warmLock
    private LogSegmentView warmView;
    private long warmedTo;
    // The read of frames written through in flight, or done and not yet taken into warmView and warmedTo: one at a
    // time, so they share one buffer; guarded by warmLock
    private Warm warming;
    // The buffer of those reads, direct and of WARM_PIECE bytes, made at the first; guarded by warmLock
    private ByteBuffer warmBuffer;
    // Whether every record whose values go to the log has its frames read first, as if its group bypassed the cache;
    // for tests
    private volatile boolean warmsEveryRecord;
    // The values moved to the log and to arrays of their own, the records drained, the bytes of frames read back, and
    // the records released unmoved after that read failed, for tests
    private final AtomicLong loggedValues = new AtomicLong();
    private final AtomicLong copiedValues = new AtomicLong();
    private final AtomicLong drainedRecords = new AtomicLong();
    private final AtomicLong warmedBytes = new AtomicLong();
    private final AtomicLong unwarmedRecords = new AtomicLong();
    private final AtomicLong takenRecords = new AtomicLong();

    /// Creates the queue of a store.
    ///
    /// @param budget the bytes of values whose frames are durable the queue holds before it moves them
    /// @param encrypted whether the store's log is encrypted, so the values read back are authenticated by a keyed hash
    SpillQueue(long budget, boolean encrypted) {
        this.budget = budget;
        this.lowWater = budget - budget / 4;
        this.encrypted = encrypted;
        this.lock = new Object();
        this.warmLock = new Object();
        this.records = new ArrayDeque<>();
        this.segments = new ArrayList<>();
        this.running = new AtomicBoolean();
        this.alive = new AtomicInteger();
        this.cryptos = new AtomicReferenceArray<>(Math.max(4, 2 * Runtime.getRuntime().availableProcessors()));
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

    /// {@return the budget for a heap of `maxMemory` bytes: a 256th, from 4 MiB to 64 MiB}
    static long defaultBudget(long maxMemory) {
        return Math.clamp(maxMemory / 256, 4L << 20, 64L << 20);
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
            if (segment.note(sequence)) {
                segments.add(segment);
            }
        }
    }

    /// Moves the value of `version`, which a recovery replayed from `frame`, at once to its place in the log, on the
    /// caller's thread: `frame` is the opened body of the frame at `frameOffset` of `segment`, which holds it durably,
    /// and `version` is on it, at `bodyIndex`, and at least [#LOG_VALUE_MIN] bytes. The tag is made where the value
    /// lies in `frame`, which the version no longer keeps.
    ///
    /// @param crypto the caller's, until it gives it back ([#takeCrypto()])
    /// @return whether the value moved
    boolean moveReplayed(Crypto crypto, Version version, LogSegmentView segment, long frameOffset, int bodyIndex, byte[] frame) {
        if (!version.spill(crypto, segment, frameOffset, bodyIndex, frame)) {
            return false;
        }
        loggedValues.incrementAndGet();
        return true;
    }

    /// Queues `record`, of a published commit after every one queued, and starts moving values if the queue is over
    /// its budget.
    void add(Record record) {
        if (record.count == 0) {
            return;
        }
        var diagAdd = System.nanoTime();
        synchronized (lock) {
            ScratchCounters.add("addLockWaitNanos", System.nanoTime() - diagAdd);
            if (closed) {
                return;
            }
            records.addLast(record);
            held = held + record.bytes;
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
        alive.incrementAndGet();
        ScratchCounters.add("spillStarts", 1);
        try {
            Thread.ofVirtual().name("ledger-spill").start(this::run);
        } catch (Throwable _) {
            running.set(false);
            alive.decrementAndGet();
        }
    }

    private void run() {
        try {
            try {
                drain(current);
            } finally {
                running.set(false);
            }
            // A group that became durable as the thread stopped found it running
            if (movable()) {
                start();
            }
        } finally {
            alive.decrementAndGet();
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
                // Where the frames of a record that are read back before its values move end, or -1 if none are
                var extent = -1L;
                synchronized (lock) {
                    record = records.peekFirst();
                    if (record == null || held <= downTo || record.sequence > durableSequence.getAsLong()) {
                        return;
                    }
                    records.pollFirst();
                    held = held - record.bytes;
                    drainedRecords.incrementAndGet();
                    if (record.sequence <= taken) {
                        ScratchCounters.add("drainTaken", 1);
                        takenRecords.incrementAndGet();
                        record.release();
                        continue;
                    }
                    if (record.logs()) {
                        // Before any value moves there, and before a read of its frames opens its channel, so a
                        // checkpoint that retires segments sees the commit and closes the channel
                        var segment = record.segment;
                        if (segment.note(record.sequence)) {
                            segments.add(segment);
                        }
                        if (warms(record)) {
                            extent = record.extent();
                        }
                    }
                }
                var diagWarm = System.nanoTime();
                var diagWarmed = extent >= 0 && !warm(record, extent, durableSequence, downTo);
                if (extent >= 0) {
                    ScratchCounters.add("drainWarmNanos", System.nanoTime() - diagWarm);
                    ScratchCounters.add("drainWarmRecords", 1);
                }
                if (diagWarmed) {
                    // The values stay on their arrays rather than leave for a place that a read would fetch from the
                    // device; a retired segment's commit is in a published checkpoint, which drops the record anyway
                    unwarmedRecords.incrementAndGet();
                    record.release();
                    continue;
                }
                var diagStart = System.nanoTime();
                record.spill(this, crypto);
                ScratchCounters.add("drainNanos", System.nanoTime() - diagStart);
                ScratchCounters.add("drained", 1);
            }
        } finally {
            giveBack(crypto);
        }
    }

    /// {@return whether the values of `record` move only once its frames are read back: some go to the log, and its
    /// group bypassed the system's cache}
    private boolean warms(Record record) {
        return record.logs() && (record.uncached || warmsEveryRecord);
    }

    /// Reads the frames of `record`, which [#warms(Record)] and which the drain took, back from its segment up to
    /// `extent`, unless an earlier read did; then, if no read is in flight, starts reading the frames of the records
    /// after it on a thread of its own, so they are in the cache by the time the drain moves their values.
    ///
    /// @param downTo the held bytes the drain stops at, past which no record's frames are read
    /// @return whether every byte up to `extent` was read: not if the read failed or found the segment retired
    private boolean warm(Record record, long extent, LongSupplier durableSequence, long downTo) {
        var view = record.segment;
        while (true) {
            Warm wait;
            Warm ahead = null;
            var inline = false;
            synchronized (warmLock) {
                if (warmView == view && warmedTo >= extent) {
                    // Once half a span is left before the drain, the next is read while it moves the rest
                    if (warming == null && !closed && warmedTo - extent <= WARM_SPAN / 2) {
                        ahead = span(view, warmedTo, warmedTo, true, durableSequence, downTo);
                        warming = ahead;
                    }
                    wait = null;
                } else {
                    wait = warming;
                    if (wait == null) {
                        var from = warmView == view ? Math.max(record.frameOffset, warmedTo) : record.frameOffset;
                        wait = span(view, from, extent, false, durableSequence, downTo);
                        warming = wait;
                        inline = true;
                    }
                }
            }
            if (wait == null) {
                if (ahead != null) {
                    start(ahead);
                }
                return true;
            }
            if (inline) {
                wait.run(this);
            }
            var read = wait.await();
            synchronized (warmLock) {
                if (warming == wait) {
                    warming = null;
                }
                if (read) {
                    warmedTo = warmView == wait.view ? Math.max(warmedTo, wait.to) : wait.to;
                    warmView = wait.view;
                } else {
                    if (warmView == wait.view && wait.view.retired()) {
                        warmView = null;
                    }
                    // The bytes of the record were among those the failed read was to read
                    if (wait.view == view && wait.to >= extent) {
                        return false;
                    }
                }
            }
        }
    }

    /// {@return the read of the frames of `view` from `from` to at least `end`, and on over the frames of the records
    /// the queue holds that are durable and in `view`, as far as [#WARM_SPAN] from its first byte reaches; `null` if
    /// it would read nothing}
    ///
    /// The durable sequence is read before the location of any record, which it publishes; the locations and values
    /// of the records are read holding the lock of the queue, which a checkpoint takes to release them. Holding
    /// `warmLock`.
    ///
    /// @param ahead whether the read starts at the first frame of a record that ends past `from` rather than at `from`,
    ///              as a read ahead of the drain does, which so leaves out frames no record needs read
    /// @param downTo the held bytes the drain stops at: the frames of the records it will not take are not read
    private Warm span(LogSegmentView view, long from, long end, boolean ahead, LongSupplier durableSequence, long downTo) {
        var durable = durableSequence.getAsLong();
        var started = !ahead;
        var diagSpan = System.nanoTime();
        var diagWalked = 0L;
        try {
        synchronized (lock) {
            ScratchCounters.add("spanLockWaitNanos", System.nanoTime() - diagSpan);
            var left = held - downTo;
            var skipped = taken;
            for (var next : records) {
                if (left <= 0 || closed || next.sequence > durable) {
                    break;
                }
                left -= next.bytes;
                diagWalked++;
                if (next.sequence <= skipped || !warms(next)) {
                    continue;
                }
                // Compared by use, not by number: a file is reused under a new use and key
                if (next.segment != view) {
                    break;
                }
                var nextEnd = next.extent();
                if (nextEnd <= end) {
                    continue;
                }
                if (!started) {
                    from = Math.max(from, next.frameOffset);
                    started = true;
                } else if (nextEnd - from > WARM_SPAN) {
                    break;
                }
                end = nextEnd;
            }
        }
        } finally {
            ScratchCounters.add("spanNanos", System.nanoTime() - diagSpan);
            ScratchCounters.add("spanCalls" + (ahead ? "Ahead" : "Inline"), 1);
            ScratchCounters.add("spanWalked", diagWalked);
        }
        if (end <= from) {
            return null;
        }
        if (warmBuffer == null) {
            warmBuffer = ByteBuffer.allocateDirect(WARM_PIECE);
        }
        return new Warm(view, from, end, warmBuffer);
    }

    /// Starts `ahead` on a thread of its own, or runs it on this one if the system refuses a thread, since a drain may
    /// wait for it.
    private void start(Warm ahead) {
        try {
            Thread.ofVirtual().name("ledger-warm").start(() -> ahead.run(this));
        } catch (Throwable _) {
            ahead.run(this);
        }
    }

    /// Records that a checkpoint writes every commit up to `cut` into its trees, as it cuts them, so the thread moves no
    /// value of those commits until it is published ([#published(long, long, long)]) or given up ([#checkpointDropped()]);
    /// holding the store's publishing role.
    void checkpointTakes(long cut) {
        taken = cut;
    }

    /// Records that the checkpoint that cut the commits [#checkpointTakes(long)] named failed, so their records still
    /// queued move as any other; holding the store's publishing role.
    ///
    /// A record the thread dropped while that checkpoint ran keeps its values on its batch's arrays until the next
    /// checkpoint is published.
    void checkpointDropped() {
        taken = -1;
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
            for (var segment : segments) {
                if (segment.number() >= logStart) {
                    break;
                }
                segment.cover(latest);
            }
        }
        // Once no record of the commits it wrote is left
        taken = -1;
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
        }
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

    /// {@return the records whose values a drain took to move so far; for tests}
    long drainedRecordsForTests() {
        return drainedRecords.get();
    }

    /// {@return the bytes of frames read back so far before their values moved; for tests}
    long warmedBytesForTests() {
        return warmedBytes.get();
    }

    /// {@return the records released with their values in place because their frames could not be read back; for
    /// tests}
    long unwarmedRecordsForTests() {
        return unwarmedRecords.get();
    }

    /// {@return the records dropped unmoved because a checkpoint in flight writes their commits; for tests}
    long takenRecordsForTests() {
        return takenRecords.get();
    }

    /// {@return the sequence of the last commit a checkpoint in flight writes, or -1 while none runs; for tests}
    long takenForTests() {
        return taken;
    }

    /// Reads back the frames of every record whose values go to the log before they move, as if every group bypassed
    /// the system's cache; for tests.
    void warmEveryRecordForTests() {
        warmsEveryRecord = true;
    }

    /// {@return the segments values moved to that no checkpoint retired, by number; for tests}
    long[] segmentsForTests() {
        synchronized (lock) {
            return segments.stream().mapToLong(LogSegmentView::number).toArray();
        }
    }

    /// Waits until no thread that moves values runs, nor one that stopping started, so a drain a test runs next is the
    /// only one; for tests.
    void awaitIdleForTests() throws InterruptedException {
        while (alive.get() != 0) {
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
        ScratchCounters.addWho("cryptoNew", 1);
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
        // The bytes of the values, and whether one is at least LOG_VALUE_MIN
        private long bytes;
        private boolean large;
        // Set by the log writer as it seals the frame: the segment use, the frame's offset, where the commit's body
        // starts in the frame's body, and whether the group was written past the system's cache
        private LogSegmentView segment;
        private long frameOffset;
        private int commitStart;
        private boolean uncached;

        private Record(long sequence, int capacity) {
            this.sequence = sequence;
            this.versions = new Version[capacity];
            this.offsets = new int[capacity];
        }

        /// Lists `version`, whose value of `length` bytes starts at `offset` of the commit's body.
        void add(Version version, int offset, int length) {
            if (count == versions.length) {
                versions = Arrays.copyOf(versions, count * 2);
                offsets = Arrays.copyOf(offsets, count * 2);
            }
            versions[count] = version;
            offsets[count++] = offset;
            bytes += length;
            large |= length >= LOG_VALUE_MIN;
        }

        /// Records where the log wrote the frame of the commit: at `frameOffset` of the use `segment`, the commit's body
        /// at `commitStart` of the frame's body, written through past the system's cache if `uncached`, so its frames
        /// are read back before its values move.
        ///
        /// @param segment the use, or `null` if the store moves no value
        void locate(LogSegmentView segment, long frameOffset, int commitStart, boolean uncached) {
            this.segment = segment;
            this.frameOffset = frameOffset;
            this.commitStart = commitStart;
            this.uncached = uncached;
        }

        /// {@return the sequence of the commit}
        long sequence() {
            return sequence;
        }

        /// {@return whether values move to the log: some are long enough, and a segment holds its frame; one written
        /// past the system's cache moves once a read of the frame brings its pages back there}
        private boolean logs() {
            return large && segment != null;
        }

        /// {@return where the last value the record lists ends in its segment}
        ///
        /// Read by the drain that took the record, or holding the lock of the queue, which a checkpoint takes to
        /// release the values.
        private long extent() {
            var end = 0L;
            for (var i = 0; i < count; i++) {
                end = Math.max(end, (long) offsets[i] + versions[i].valueLength());
            }
            return frameOffset + LogWriter.FRAME_HEADER + commitStart + end;
        }

        /// Moves the values, unless a long one cannot move to the log, then forgets them.
        private void spill(SpillQueue queue, Crypto crypto) {
            if (!large || logs()) {
                var logged = 0;
                var copied = 0;
                for (var i = 0; i < count; i++) {
                    var version = versions[i];
                    if (version.spill(crypto, segment, frameOffset, commitStart + offsets[i], null)) {
                        if (version.valueLength() >= LOG_VALUE_MIN) {
                            logged++;
                        } else {
                            copied++;
                        }
                    }
                }
                ScratchCounters.add("spillLogged", logged);
                ScratchCounters.add("spillCopied", copied);
                queue.loggedValues.addAndGet(logged);
                queue.copiedValues.addAndGet(copied);
            }
            if (large && !logs()) {
                ScratchCounters.add(uncached ? "spillKeptThrough" : "spillKeptOther", count);
            }
            release();
        }

        /// Forgets the values.
        private void release() {
            versions = null;
            offsets = null;
            count = 0;
        }
    }

    /// A read of the frames from `from` to `to` of a segment use, which brings their pages into the system's cache.
    ///
    /// It runs once, on the thread of the drain that needs it or on one of its own, and any number of drains wait for
    /// it.
    private static final class Warm {
        private final LogSegmentView view;
        private final long from;
        private final long to;
        private final ByteBuffer buffer;
        private final CountDownLatch done;
        // Whether every byte was read; written before done counts down
        private boolean read;

        private Warm(LogSegmentView view, long from, long to, ByteBuffer buffer) {
            this.view = view;
            this.from = from;
            this.to = to;
            this.buffer = buffer;
            this.done = new CountDownLatch(1);
        }

        /// Reads the frames, unless `queue` closes first.
        private void run(SpillQueue queue) {
            var diagRun = System.nanoTime();
            try {
                read = view.warm(from, to - from, buffer, () -> queue.closed);
                ScratchCounters.addWho("warmRunNanos", System.nanoTime() - diagRun);
                ScratchCounters.addWho("warmRunBytes", to - from);
                ScratchCounters.add(read ? "warmRunOk" : "warmRunFailed", 1);
                if (read) {
                    queue.warmedBytes.addAndGet(to - from);
                }
            } finally {
                done.countDown();
            }
        }

        /// {@return whether every byte was read, once the read ran}
        private boolean await() {
            var interrupted = false;
            try {
                while (true) {
                    try {
                        done.await();
                        return read;
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /// The ciphers of one thread at a time: those that authenticate a value moved to the log and decrypt it back.
    ///
    /// Neither allocates per value: the keystream of a value is AES-ECB of its counter blocks under a cipher keyed once
    /// per segment use, where a cipher in CTR mode takes a new IV and with it a new initialization per value.
    ///
    /// The tag of a value in an encrypted store is a pair of polynomials over the field of [#PRIME] elements, evaluated
    /// at the two secret points of the queue: one coefficient per 7 bytes of the value, after a leading 1 and the
    /// version's stamp and before the value's length, so distinct values, or the same value of another version, are
    /// distinct polynomials.
    /// Two distinct polynomials of `n` coefficients agree at a uniformly drawn point with a probability of at most
    /// `n / (2^61 - 1)`, and the two points are independent. A tag never leaves the process, so bytes changed in the
    /// file, whatever change the CTR keystream lets anyone make, pass for the value with a probability of at most
    /// `n / (2^61 - 1)` times `n / (2^61 - 1) + 2^-32`, the second factor that of the 32 bits a tag keeps of the second
    /// polynomial: below 2^-80 for a value of 12 KiB, about the bound of a 96-bit GMAC of the same value.
    ///
    /// @implNote This implementation tags with a universal hash of its own, not a MAC of the JDK: a store that recovers
    /// a long log after a crash tags every value it replays while the JVM has compiled little, when the C2 compilation
    /// of an HMAC inlined at its call site takes hundreds of milliseconds of a compiler thread, and a GMAC takes an
    /// initialization and two copies of the value per tag; the loop of the polynomials compiles in a few.
    static final class Crypto {
        // AES-ECB under the key it last decrypted from by key rather than with a segment use's cipher; made at the first
        // such decryption
        private Cipher blocks;
        // The key blocks is under, or null before the first such decryption
        private SecretKeySpec blocksKey;
        // The points of the polynomials of the tags, or null without encryption
        private final long[] keys;
        private final CRC32C crc;
        // The two polynomials of the last tag
        private long first;
        private long second;
        private byte[] scratch;
        // A buffer over scratch, which a read of the log fills, or null before the first
        private ByteBuffer scratchBuffer;
        // The counter blocks of a piece of a value and the keystream they encrypt to, of KEYSTREAM bytes each, or null
        // before the first decryption
        private byte[] counters;
        private byte[] keystream;

        private Crypto(SpillQueue queue) {
            this.keys = queue.encrypted ? queue.tagKeys() : null;
            this.crc = new CRC32C();
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
        /// `key`.
        ///
        /// A body is sealed from the counter block after the first, `nonce || 2`, so the block of a byte at `i` of it
        /// is `nonce || 2 + i / 16`; a body holds far fewer than 2^36 bytes, so the counter never carries into the
        /// nonce.
        void decrypt(SecretKeySpec key, long frameOffset, int bodyIndex, byte[] sealed, int skip, int length,
                     byte[] target, int offset) {
            try {
                if (blocks == null) {
                    blocks = newBlocks();
                }
                if (key != blocksKey) {
                    blocksKey = null;
                    blocks.init(Cipher.ENCRYPT_MODE, key);
                    blocksKey = key;
                }
            } catch (GeneralSecurityException exception) {
                throw new IllegalStateException("AES failed", exception);
            }
            try {
                decrypt(blocks, frameOffset, bodyIndex, sealed, skip, length, target, offset);
            } catch (IllegalStateException exception) {
                blocksKey = null;
                throw exception;
            }
        }

        /// Decrypts as [#decrypt(SecretKeySpec, long, int, byte\[\], int, int, byte\[\], int)] does, with `cipher`, an
        /// AES-ECB cipher ([#newBlocks()]) initialized to encrypt under the key of the value's segment use.
        ///
        /// The keystream is made a piece of [#KEYSTREAM] bytes at a time, so the holder's arrays keep their size
        /// whatever the value's.
        ///
        /// @throws IllegalStateException if the cipher fails, which leaves its state unknown
        void decrypt(Cipher cipher, long frameOffset, int bodyIndex, byte[] sealed, int skip, int length, byte[] target,
                     int offset) {
            if (counters == null) {
                counters = new byte[KEYSTREAM];
                keystream = new byte[KEYSTREAM];
            }
            var bytes = (skip + length + BLOCK - 1) & -BLOCK;
            var first = 2 + (bodyIndex >>> 4);
            for (var start = 0; start < bytes; start += KEYSTREAM) {
                var piece = Math.min(KEYSTREAM, bytes - start);
                for (var block = 0; block < piece; block += BLOCK) {
                    Bytes.i32(counters, block, 0);
                    Bytes.i64(counters, block + Integer.BYTES, frameOffset);
                    Bytes.i32(counters, block + Integer.BYTES + Long.BYTES, first + ((start + block) >>> 4));
                }
                try {
                    cipher.update(counters, 0, piece, keystream, 0);
                } catch (GeneralSecurityException exception) {
                    throw new IllegalStateException("AES failed", exception);
                }
                // The value's bytes in this piece: from skip in the first, up to skip + length in the last
                var to = Math.min(start + piece, skip + length);
                for (var i = Math.max(start, skip); i < to; i++) {
                    target[offset + i - skip] = (byte) (sealed[i] ^ keystream[i - start]);
                }
            }
        }

        /// {@return a new AES-ECB cipher, which makes the keystream blocks of a value once initialized under a key}
        static Cipher newBlocks() {
            try {
                return Cipher.getInstance("AES/ECB/NoPadding");
            } catch (GeneralSecurityException exception) {
                throw new IllegalStateException("AES is not available", exception);
            }
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
        /// words, the value in words of 7 bytes, the last one shorter, and the length.
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

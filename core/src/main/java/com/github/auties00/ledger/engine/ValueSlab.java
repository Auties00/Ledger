package com.github.auties00.ledger.engine;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.LongSupplier;

/// The copies of the small values a store's fastest commits keep until a checkpoint, packed into arrays of one G1
/// region each.
///
/// A commit of one small value otherwise keeps it on an array of its own, with a read-only view of the array and the
/// view's session: three objects beside its [Version], all of which live until a checkpoint, several young collections
/// later, so at hundreds of thousands of writes a second the collector spends a good part of its time copying them
/// from survivor space to survivor space and to the old generation. Here a value is a span of a chunk's one view, which
/// its version holds as the view, an offset and a length. An array just short of a region is humongous, so it takes
/// the region alone and no young or mixed collection copies it. An array of half a region to almost a whole one would
/// waste what it leaves of its region, and a smaller one would be copied whole, so every chunk is
/// [the same size][#capacity()].
///
/// A chunk stays reachable while a version holds a value on it, overwritten values beside it included, so a slab only
/// serves a store that fills chunks fast, whose chunks a checkpoint soon lets go of, and a key space that does not
/// overwrite or remove most of what it writes before a checkpoint takes it, as [Overwrites] judges. The slab starts
/// cold, when it takes no value; it [measures][#note(long)] the bytes it would have taken, over all threads, and turns
/// hot once they would have filled [#FAST_CHUNKS] chunks a second for [#FAST_WINDOWS] windows in a row. It turns cold
/// again after [#SLOW_WINDOWS] windows at a [#SLOW_SHARE]th of that rate, or at a [tick][#tick(boolean)] of a store that
/// went quiet, and lets go of its chunks then: a store of occasional writes never makes one.
///
/// Only the collector G1 treats such an array so: a parallel collector copies it and ZGC relocates it, dead bytes
/// included. Under any other collector, or without the module `jdk.management` to ask, the slab stays cold for good.
/// It asks only once a store is fast, as loading the management classes would cost an open tens of milliseconds, and
/// a question that fails, as it may on the tight heap of a fast store, turns it off too: where a value goes never fails
/// a commit.
///
/// It is safe for concurrent use: a copy claims its bytes with a compare-and-set, as in [KeySlab], and a chunk is never
/// used again once replaced, nor recycled. A commit that claimed bytes on a chunk just dropped still copies into it,
/// and its version keeps the chunk reachable. A value is published by the release store that makes its version the
/// head of its chain, as the thread that makes the version copied the value before. A thread that finds no chunk and
/// is refused one takes no lock again until the next window, so a store past its limits pays a few reads a commit.
final class ValueSlab {
    /// The bytes of a region a chunk's array leaves for its header and the alignment: enough to take exactly one region.
    private static final int HEADROOM = 64;
    /// The smallest region G1 makes, by which the rate that makes the slab ask for the real one is set.
    private static final long SMALLEST_REGION = 1L << 20;
    /// The chunks a second the values must fill for the slab to turn hot: the most chunks a store holds, so that the
    /// tails of those it holds fill within a second.
    private static final int FAST_CHUNKS = 8;
    /// The shortest time over which the slab measures the rate of the values.
    private static final long WINDOW_NANOS = 250_000_000L;
    /// The windows in a row at the fast rate that turn the slab hot.
    private static final int FAST_WINDOWS = 2;
    /// The windows in a row below a [#SLOW_SHARE]th of the fast rate that turn it cold: a checkpoint that holds commits
    /// back for a second or two ends in one such window, and must not.
    private static final int SLOW_WINDOWS = 8;
    /// The fraction of the fast rate below which a window counts as slow.
    private static final int SLOW_SHARE = 4;
    /// The time without a window after which a tick turns the slab cold, whatever its last windows found.
    private static final long SILENCE_NANOS = IdleTicker.PERIOD_MILLIS * 1_000_000L / 2;
    /// The chunks a store holds at once, a power of two: the tails of more would be heap neither the values nor a
    /// checkpoint needs.
    private static final int MAX_HELD = 8;
    /// The fraction of the heap the chunks every store of the process holds at once may take.
    private static final int HEAP_SHARE = 64;
    /// The stripes of the chunks, as many as [KeySlab]'s, so threads running at once seldom share one, up to
    /// [#MAX_HELD]: with more, a thread on a stripe past the chunks the store may hold would never have one.
    private static final int STRIPES = Math.min(Integer.highestOneBit(Math.clamp(2L * Runtime.getRuntime().availableProcessors() - 1, 1, 63)) << 1, MAX_HELD);
    /// The counters of the measurement, a power of two.
    private static final int COUNTERS = 8;
    /// The `int`s from one counter to the next: 128 bytes, the pair of lines Intel's spatial prefetcher fetches together.
    private static final int COUNTER_STRIDE = 32;
    /// A counter that passes a multiple of `1 << NOTE_BITS` bytes looks at the clock.
    private static final int NOTE_BITS = 18;
    private static final VarHandle USED;
    /// What a stripe holds while a thread makes its next chunk: it takes nothing, so no other thread makes one too.
    private static final Chunk FILLING;
    /// The chunks of every store of the process.
    private static final Budget PROCESS = new Budget(Runtime.getRuntime().maxMemory() / HEAP_SHARE);
    // The region of every slab made from now on, to which it is forced hot, or 0
    private static volatile long forcedRegionForTests;
    // The clock and the region of every slab a store makes from now on, or null for the JVM's
    private static volatile LongSupplier clockForTests;
    private static volatile LongSupplier regionForTests;

    static {
        try {
            USED = MethodHandles.lookup().findVarHandle(ChunkUsed.class, "used", int.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
        FILLING = new Chunk(0);
    }

    private final LongSupplier clock;
    private final LongSupplier region;
    private final Budget budget;
    private final AtomicReferenceArray<Chunk> stripes;
    // The chunks the stripes hold, and those a thread is making
    private final AtomicInteger held;
    // The capacity of every chunk this slab made
    private final AtomicLong made;
    // The bytes the chunks no stripe holds left unused
    private final AtomicLong tails;
    // The start of the window, whose closer the compare-and-set on it picks
    private final AtomicLong windowStart;
    // The start of the window in which the limits last refused a chunk: no thread without a chunk asks again in it
    private volatile long refusedWindow;
    // The bytes each thread's counter saw, racy: a lost addition only lowers the rate; made with the first note
    private int[] counted;
    // The sum of the counters at the start of the window, and the windows in a row at each rate; the closer's alone
    private int windowTotal;
    private int fastWindows;
    private int slowWindows;
    // The bytes of a chunk, once the region is known, or 0
    private volatile int capacity;
    // Whether the slab takes values
    private volatile boolean hot;
    // Whether it never will, so it measures nothing either
    private volatile boolean off;
    // Whether the store closed, so the slab makes no more chunks
    private volatile boolean closed;
    // Whether a test decided hot and cold, so the measurement does not
    private volatile boolean forced;

    /// Makes a cold slab that asks the JVM for the region, once it needs it.
    ValueSlab() {
        this(clockForTests != null ? clockForTests : System::nanoTime, regionForTests != null ? regionForTests : ValueSlab::collectorRegion, PROCESS);
    }

    /// Makes a cold slab.
    ///
    /// @param clock the time, in nanoseconds
    /// @param region the bytes of a region of the collector the JVM runs, or 0 if it is not G1
    /// @param budget the chunks this slab shares with others
    ValueSlab(LongSupplier clock, LongSupplier region, Budget budget) {
        this.clock = clock;
        this.region = region;
        this.budget = budget;
        this.stripes = new AtomicReferenceArray<>(STRIPES);
        this.held = new AtomicInteger();
        this.made = new AtomicLong();
        this.tails = new AtomicLong();
        this.windowStart = new AtomicLong(clock.getAsLong());
        this.refusedWindow = Long.MIN_VALUE;
        var forcedRegion = forcedRegionForTests;
        if (forcedRegion > 0) {
            forceHotForTests(forcedRegion);
        }
    }

    /// {@return the bytes of a region of G1, or 0 if the JVM runs another collector or cannot say}
    static long collectorRegion() {
        return Collector.REGION;
    }

    /// {@return whether the slab takes values at the moment}
    boolean hot() {
        return hot;
    }

    /// {@return whether a value of this thread may find a place: the slab is hot, and the limits did not just refuse the
    /// chunk this thread's stripe lacks}
    boolean accepting() {
        return hot && (refusedWindow != windowStart.get() || stripes.get(stripe()) != null);
    }

    /// {@return whether the slab will never take a value, so a commit has no need to measure them}
    boolean off() {
        return off;
    }

    /// {@return the bytes of a chunk, 0 until the slab knows the region; for tests}
    int capacity() {
        return capacity;
    }

    /// Counts `bytes` of values the slab could take, toward the rate that turns it hot or cold.
    void note(long bytes) {
        if (bytes == 0 || off) {
            return;
        }
        var counters = counted;
        if (counters == null) {
            counters = new int[(COUNTERS + 1) * COUNTER_STRIDE];
            counted = counters;
        }
        var slot = (stripeHash() & (COUNTERS - 1)) * COUNTER_STRIDE + COUNTER_STRIDE;
        var before = counters[slot];
        var after = before + (int) bytes;
        counters[slot] = after;
        if ((before ^ after) >>> NOTE_BITS != 0) {
            closeWindow(counters);
        }
    }

    /// Closes the window if it has lasted long enough and no other thread closes it, turning the slab hot or cold by
    /// what it measured.
    private void closeWindow(int[] counters) {
        var start = windowStart.get();
        var now = clock.getAsLong();
        var elapsed = now - start;
        if (elapsed < WINDOW_NANOS || !windowStart.compareAndSet(start, now) || forced) {
            return;
        }
        var total = 0;
        for (var slot = COUNTER_STRIDE; slot < counters.length; slot += COUNTER_STRIDE) {
            total += counters[slot];
        }
        var rate = Integer.toUnsignedLong(total - windowTotal) * 1_000_000_000L / elapsed;
        windowTotal = total;
        var size = capacity;
        var fast = FAST_CHUNKS * (size == 0 ? SMALLEST_REGION : size + (long) HEADROOM);
        if (rate >= fast) {
            slowWindows = 0;
            if (size == 0) {
                // A window at the rate of the smallest region: the next ones are held to the real one
                learnRegion();
            } else if (++fastWindows >= FAST_WINDOWS) {
                hot = true;
            }
        } else {
            fastWindows = 0;
            if (rate >= fast / SLOW_SHARE) {
                slowWindows = 0;
            } else if (hot && ++slowWindows >= SLOW_WINDOWS) {
                cool();
            }
        }
    }

    /// Asks for the region, and turns the slab off for good if it cannot use it: the collector is not G1, a chunk takes
    /// more than the budget, or the question fails.
    ///
    /// This runs inside a commit that has applied nothing yet, so nothing may leave it: the first question loads the
    /// management classes, and an [OutOfMemoryError] there would leave [Collector] erroneous, with every later question
    /// of the process a [NoClassDefFoundError].
    private void learnRegion() {
        long bytes;
        try {
            bytes = region.getAsLong();
        } catch (Throwable _) {
            bytes = 0;
        }
        if (bytes <= HEADROOM || bytes > budget.limit) {
            off = true;
        } else {
            capacity = Math.toIntExact(bytes - HEADROOM);
        }
    }

    /// Copies a value into a chunk and records where as the home of write `index`, unless the slab declines it: the
    /// value does not fit a chunk, another thread is making the chunk of its stripe, or the limits refuse the stripe
    /// one. Never waits for another thread.
    ///
    /// @param source the array holding the value
    /// @param offset where the value starts in `source`
    /// @param length the bytes of the value
    /// @param spans the high half of the element at `slot` receives where the value starts in the chunk
    /// @param slot the element of `spans`
    /// @return the read-only view of the chunk that holds the value, or `null` if the slab declined it
    MemorySegment place(byte[] source, int offset, int length, long[] spans, int slot) {
        if (length > capacity) {
            return null;
        }
        var stripe = stripe();
        while (true) {
            var chunk = stripes.get(stripe);
            if (chunk == FILLING) {
                return null;
            }
            var start = chunk == null ? 0 : (int) USED.getVolatile(chunk);
            if (chunk == null || start + length > chunk.array.length) {
                if (chunk == null && refusedWindow == windowStart.get() || !replace(stripe, chunk)) {
                    return null;
                }
                continue;
            }
            if (USED.compareAndSet(chunk, start, start + length)) {
                System.arraycopy(source, offset, chunk.array, start, length);
                spans[slot] = (long) start << 32 | (int) spans[slot];
                return chunk.view;
            }
            // Another thread claimed from the chunk meanwhile: the loop reads its new end
        }
    }

    /// Gives stripe `stripe`, which held `full`, its next chunk.
    ///
    /// @return whether the stripe may hold a chunk now: `false` if no next chunk could be had
    private boolean replace(int stripe, Chunk full) {
        if (!stripes.compareAndSet(stripe, full, FILLING)) {
            // Another thread is making it, or has
            return true;
        }
        Chunk next = null;
        try {
            if (full != null) {
                retire(full);
            }
            next = make();
        } finally {
            stripes.set(stripe, next);
        }
        return next != null;
    }

    /// {@return a new chunk counted against the limits, or `null` if they, or the heap, allow none}
    private Chunk make() {
        var size = capacity;
        if (!hot) {
            return null;
        }
        if (!budget.reserve(this, size)) {
            refusedWindow = windowStart.get();
            return null;
        }
        try {
            var chunk = new Chunk(size);
            made.addAndGet(size);
            return chunk;
        } catch (OutOfMemoryError _) {
            // A region free as a whole is rarer than the room the same values take apart: they go back to their own
            // arrays, and the slab warms up again if the heap recovers
            held.decrementAndGet();
            cool();
            return null;
        }
    }

    /// Stops counting `chunk`, which no stripe holds any more, as held.
    private void retire(Chunk chunk) {
        held.decrementAndGet();
        tails.addAndGet(chunk.array.length - (int) USED.getVolatile(chunk));
    }

    /// Turns the slab cold and lets go of its chunks.
    private void cool() {
        if (!forced) {
            hot = false;
        }
        fastWindows = 0;
        slowWindows = 0;
        drop();
    }

    /// Takes the chunks off the stripes; the versions on them keep them until a checkpoint takes those.
    private void drop() {
        for (var stripe = 0; stripe < STRIPES; stripe++) {
            var chunk = stripes.get(stripe);
            if (chunk != null && chunk != FILLING && stripes.compareAndSet(stripe, chunk, null)) {
                retire(chunk);
            }
        }
    }

    /// Takes off the stripes the chunks that took no value since the last tick: those of threads that stopped, which a
    /// store that stays fast would otherwise hold against its limits.
    private void dropUnused() {
        for (var stripe = 0; stripe < STRIPES; stripe++) {
            var chunk = stripes.get(stripe);
            if (chunk == null || chunk == FILLING) {
                continue;
            }
            var used = (int) USED.getVolatile(chunk);
            if (used != chunk.usedAtTick) {
                chunk.usedAtTick = used;
            } else if (stripes.compareAndSet(stripe, chunk, null)) {
                retire(chunk);
            }
        }
    }

    /// Lets go of the chunks of a store that stopped filling them, at a tick of the [IdleTicker].
    ///
    /// @param idle whether the store committed nothing since the last tick
    void tick(boolean idle) {
        if (forced) {
            drop();
        } else if (idle || !hot || clock.getAsLong() - windowStart.get() >= SILENCE_NANOS) {
            cool();
        } else {
            dropUnused();
        }
    }

    /// Turns the slab cold for good and lets go of its chunks, as its store closes.
    void close() {
        closed = true;
        hot = false;
        drop();
        budget.leave(this);
    }

    /// {@return the heap the chunks take that no value accounts for, at most: the tails of those no stripe holds, and
    /// the whole of those the stripes do}
    long slackBytes() {
        return tails.get() + (long) held.get() * capacity;
    }

    /// {@return the capacity of every chunk this slab made so far}
    long chunkBytes() {
        return made.get();
    }

    /// {@return the capacity of the chunks this slab made that no stripe holds}
    long retiredChunkBytes() {
        return made.get() - (long) held.get() * capacity;
    }

    // Thread ids are consecutive: a multiplicative hash spreads them over the stripes
    private static int stripeHash() {
        return (int) ((Thread.currentThread().threadId() * 0x9E3779B97F4A7C15L) >>> 40);
    }

    private static int stripe() {
        return stripeHash() & (STRIPES - 1);
    }

    /// Forces the slab hot, whatever rate it measures, with chunks of a region of `regionBytes`; for tests.
    void forceHotForTests(long regionBytes) {
        forced = true;
        off = false;
        capacity = Math.toIntExact(regionBytes - HEADROOM);
        hot = true;
    }

    /// Forces the slab cold, whatever rate it measures; for tests.
    void forceColdForTests() {
        forced = true;
        off = true;
        hot = false;
    }

    /// Forces hot every slab made from now on, with chunks of a region of `regionBytes`, or none if 0; for tests.
    static void forceNewHotForTests(long regionBytes) {
        forcedRegionForTests = regionBytes;
    }

    /// Makes every slab a store makes from now on measure by `clock` and ask `region`, or the JVM if `null`; for tests
    /// that run a store's commits through the measurement with no timing involved.
    static void measureNewForTests(LongSupplier clock, LongSupplier region) {
        clockForTests = clock;
        regionForTests = region;
    }

    /// {@return the chunks the stripes hold; for tests}
    int heldForTests() {
        return held.get();
    }

    /// What several slabs may hold at once: a process with a slab per store would otherwise have to find room for all
    /// of their chunks at once, which on a small heap is an [OutOfMemoryError] rather than a waste.
    static final class Budget {
        /// The bytes of chunks the slabs may hold.
        final long limit;
        // The slabs that hold a chunk or did, weakly: a store never closed leaves once the collector takes it
        private final Set<ValueSlab> slabs;
        private final Object lock;
        // The chunks asked for, guarded by the lock
        private long reserves;

        /// Makes a budget of `limit` bytes of chunks.
        Budget(long limit) {
            this.limit = limit;
            this.slabs = Collections.newSetFromMap(new WeakHashMap<>());
            this.lock = new Object();
        }

        /// {@return whether it counted a chunk of `size` bytes as held by `slab`, which is open and holds fewer than it
        /// may}
        private boolean reserve(ValueSlab slab, int size) {
            synchronized (lock) {
                reserves++;
                if (slab.closed || slab.held.get() >= MAX_HELD) {
                    return false;
                }
                var total = (long) size;
                for (var other : slabs) {
                    total += (long) other.held.get() * other.capacity;
                }
                if (total > limit) {
                    return false;
                }
                slabs.add(slab);
                slab.held.incrementAndGet();
                return true;
            }
        }

        /// Stops counting `slab`.
        private void leave(ValueSlab slab) {
            synchronized (lock) {
                slabs.remove(slab);
            }
        }

        /// {@return the times a slab asked for a chunk, each one a pass through the process-wide lock; for tests}
        long reservesForTests() {
            synchronized (lock) {
                return reserves;
            }
        }
    }

    /// What a key space's commits say about whether its values may share chunks.
    ///
    /// A value on an array of its own leaves memory as soon as a commit replaces or removes it, usually dying in eden. On
    /// a chunk, it stays until the chunk's other values have gone too, so a key space that overwrites or removes versions
    /// no checkpoint has taken yet keeps their bytes for nothing, and one that removes what it writes within a young
    /// collection has no survivors for a chunk to save. A share `s` of the values replaced so keeps `s / (1 - s)` values'
    /// bytes dead for each value left, which is judged against what a value on its own array costs beside its bytes, so
    /// the limit of the share is `TOLERATED / (average + TOLERATED)`: two thirds for values of 128 bytes, a fifth for
    /// values of 1 KiB.
    ///
    /// A share is that of a whole interval between two checkpoints, since it grows over one, from none right after a
    /// checkpoint; the last interval counts for half of what is settled, so a burst of overwrites does not stop the
    /// slab for good. One commit in 64 is [sampled][#sampled(long)], on its committing thread: the counters are racy, an
    /// estimate that only decides where a value goes, and they are touched too rarely for the threads to queue on
    /// their line.
    static final class Overwrites {
        /// The top bits of a commit's hashed sequence that are clear when it is sampled.
        private static final int SAMPLE_BITS = 6;
        /// The bytes the dead values may cost each value left at the limit of the share: twice the 120 or so an array
        /// of its own, its view and the view's session cost.
        private static final int TOLERATED = 256;
        /// The samples at which the share so far is judged, so one too high stops the slab before the interval ends:
        /// about 4,096 puts, which a key space of large values passes before the slab can turn hot on it.
        private static final int JUDGE_EVERY = 64;
        /// The samples an interval needs to count.
        private static final int MIN_SAMPLES = 64;
        /// The samples at which the counters halve, so a store whose checkpoints fail does not overflow them.
        private static final int MAX_SAMPLES = 1 << 24;
        /// The value of a share of 1.
        private static final int WHOLE = 1 << 10;

        // The sampled puts of the interval and those that replaced a version of it, racy
        private int puts;
        private int replaced;
        // The moving average of the bytes of the sampled values, racy
        private int averageBytes;
        // The settled share of the past intervals, of WHOLE, or -1 if none counted yet
        private int settled;
        // The bound of the last checkpoint: a version above it is in no tree
        private volatile long cutBound;
        private volatile boolean worthwhile;

        Overwrites() {
            this.settled = -1;
            this.worthwhile = true;
        }

        /// {@return whether a commit with this sequence is sampled}
        ///
        /// The sequence is hashed so the sample does not follow a pattern of the commits: of a store that alternates a
        /// put and a removal, the multiples of 64 would all be puts, or all removals.
        static boolean sampled(long sequence) {
            return (sequence * 0x9E3779B97F4A7C15L) >>> Long.SIZE - SAMPLE_BITS == 0;
        }

        /// {@return whether the key space's values may share chunks}
        boolean worthwhile() {
            return worthwhile;
        }

        /// Counts a put of a value of `length` bytes that replaced `head`, the newest version in memory, if any.
        void sample(Version head, int length) {
            var sampled = puts + 1;
            var average = averageBytes;
            averageBytes = average == 0 ? length : average + ((length - average) >> 4);
            var over = replaced;
            if (head != null && !head.removes() && head.stamp > cutBound) {
                over++;
            }
            if (sampled >= MAX_SAMPLES) {
                sampled >>= 1;
                over >>= 1;
            }
            puts = sampled;
            replaced = over;
            if (sampled % JUDGE_EVERY == 0) {
                judge(Math.max(settled, share(sampled, over)));
            }
        }

        /// Counts a removal of `head`, the newest version in memory, if any, as a replacement of the value it holds.
        void sampleRemoval(Version head) {
            var over = replaced;
            if (head != null && !head.removes() && head.stamp > cutBound && over < MAX_SAMPLES) {
                replaced = over + 1;
            }
        }

        /// Ends the interval at a checkpoint, whose readers' bound is `bound`.
        void cut(long bound) {
            var sampled = puts;
            if (sampled >= MIN_SAMPLES) {
                var share = share(sampled, replaced);
                settled = settled < 0 ? share : (settled + share) / 2;
                judge(settled);
            }
            puts = 0;
            replaced = 0;
            cutBound = bound;
        }

        private void judge(int share) {
            worthwhile = share < (long) WHOLE * TOLERATED / (Math.max(averageBytes, 0) + (long) TOLERATED);
        }

        private static int share(int sampled, int over) {
            return (int) (Math.clamp(over, 0, sampled) * (long) WHOLE / sampled);
        }
    }

    abstract static class ChunkUsed extends Padded.Before {
        // The bytes claimed, through USED
        @SuppressWarnings("unused")
        volatile int used;
    }

    @SuppressWarnings("unused")
    abstract static class ChunkPadding extends ChunkUsed {
        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;
    }

    /// An array values are copied into, and the read-only view of it versions hold; its counter, which the threads of
    /// a stripe write at every value, is on lines of its own.
    private static final class Chunk extends ChunkPadding {
        private final byte[] array;
        private final MemorySegment view;
        // The bytes claimed at the last tick that saw the chunk, or -1; the ticks' alone
        private int usedAtTick;

        private Chunk(int capacity) {
            this.array = new byte[capacity];
            this.view = MemorySegment.ofArray(array).asReadOnly();
            this.usedAtTick = -1;
        }
    }

    /// What the JVM says of its collector, asked the first time a store is fast enough to need it.
    private static final class Collector {
        private static final long REGION = read();

        private static long read() {
            try {
                var diagnostics = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
                if (diagnostics == null || !Boolean.parseBoolean(diagnostics.getVMOption("UseG1GC").getValue())) {
                    return 0;
                }
                return Long.parseLong(diagnostics.getVMOption("G1HeapRegionSize").getValue());
            } catch (Throwable _) {
                // The module is optional: an application that does not resolve it gets a store without the slab. So
                // does one whose heap is too tight to load the classes, whose commit this must not fail
                return 0;
            }
        }
    }
}

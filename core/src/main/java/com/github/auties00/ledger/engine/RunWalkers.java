package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/// The reads of one paged key space that load its run once and then walk its chains: scans and counts of ranges, which
/// a [RunFreezer] waits for before it drops the chains it froze into a new run.
///
/// Such a read keeps the run it loaded to the end, while a chain it has not reached may leave the ordered map: one a
/// freeze dropped would then be in neither the run the read holds nor the map, and the read would miss its key. A read
/// of one key needs none of this: it reads the key's chain before the run, and the freeze publishes the new run before
/// it drops a chain.
///
/// Two phases count the reads, each in counters spread over lines of their own by thread, so the reads of different
/// threads touch no common line. A read counts itself in the current phase, then checks the phase is still current, and
/// only then loads the run; a freeze that published its run moves the phase on and waits until no read is counted in
/// the one before: every read that may have loaded the run before the new one has left by then, and every read counted
/// in the new phase loaded the new run.
final class RunWalkers {
    private static final VarHandle COUNTS = MethodHandles.arrayElementVarHandle(long[].class);
    // The counters of each phase, by thread
    private static final int STRIPES = 8;
    // The longs from one counter to the next, so each takes a cache line of its own
    private static final int SPACING = 8;
    // The waits between two looks at the counters past the first spins
    private static final long PARK_NANOS = 50_000;
    private static final int SPINS = 64;

    // Two phases of STRIPES counters each, SPACING longs apart
    private final long[] counts;
    // The phase reads count themselves in, 0 or 1; moved on only by a freeze, which one thread runs at a time
    private volatile int phase;

    RunWalkers() {
        this.counts = new long[2 * STRIPES * SPACING];
    }

    /// Counts a read that is about to load the run and walk the chains.
    ///
    /// @return what the read passes to [#exit(int)] once it is done
    int enter() {
        var stripe = (int) Thread.currentThread().threadId() & (STRIPES - 1);
        while (true) {
            var current = phase;
            var index = (current * STRIPES + stripe) * SPACING;
            COUNTS.getAndAdd(counts, index, 1L);
            // Read after the count: a freeze that moved the phase on before it counts this read in the new phase
            if (phase == current) {
                return index;
            }
            COUNTS.getAndAdd(counts, index, -1L);
        }
    }

    /// Counts out a read [#enter()] counted, once it no longer reads the run or the chains.
    void exit(int index) {
        COUNTS.getAndAddRelease(counts, index, -1L);
    }

    /// Waits, for at most `nanos`, until every read that may have loaded the run before the caller published a new
    /// one has left; called by one thread at a time, after it published the run.
    ///
    /// A read that waited for a freeze before, which gave up on it, is counted in the phase this one moves to: the phase
    /// moves only once that one is empty too, so no read is left behind in it.
    ///
    /// @return whether they all left; `false` if the time ran out first, so the caller drops no chain
    boolean await(long nanos) {
        var deadline = System.nanoTime() + nanos;
        var current = phase;
        if (!drained(1 - current, deadline)) {
            return false;
        }
        phase = 1 - current;
        return drained(current, deadline);
    }

    // Whether no read was counted in `side`, waiting until `deadline` for those that were to leave
    private boolean drained(int side, long deadline) {
        for (var spin = 0; ; spin++) {
            var total = 0L;
            for (var stripe = 0; stripe < STRIPES; stripe++) {
                total += (long) COUNTS.getVolatile(counts, (side * STRIPES + stripe) * SPACING);
            }
            if (total == 0) {
                return true;
            }
            if (System.nanoTime() - deadline >= 0) {
                return false;
            }
            if (spin < SPINS) {
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(PARK_NANOS);
            }
        }
    }
}

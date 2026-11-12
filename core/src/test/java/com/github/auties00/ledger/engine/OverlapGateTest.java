package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The decision of [LogWriter.Overlap#AUTO], epoch by epoch, on a clock the test moves: a try beside one another follows
/// the epochs on one lane of a busy store, stays while it beats them and gives way, for ever longer, while it does not.
///
/// Each epoch is [OverlapGate#EPOCH_GROUPS] groups over a time the test chooses, so its rate is the commits it made
/// over that time.
class OverlapGateTest {
    /// A gate on a clock of its own, and the sequence the next group makes durable.
    private static final class Run {
        private long now = 1_000_000_000L;
        private long sequence;
        private final OverlapGate gate = new OverlapGate(() -> now, 0);
        // Whether each epoch ran beside one another
        final List<Boolean> epochs = new ArrayList<>();

        /// Publishes one epoch of groups that make `commits` commits durable in `nanos`, each written through if
        /// `through` and waking two commits if `shared`.
        void epoch(long commits, long nanos, boolean through, boolean shared) {
            epochs.add(gate.on());
            var groups = OverlapGate.EPOCH_GROUPS;
            for (var group = 1; group <= groups; group++) {
                now += nanos / groups;
                sequence += commits / groups;
                gate.published(sequence, through, shared ? 2 : 1);
            }
        }

        /// Publishes epochs of `nanos` each until `count` have run, at `solo` commits an epoch on one lane and
        /// `beside` commits beside one another.
        void epochs(int count, long solo, long beside, long nanos) {
            for (var i = 0; i < count; i++) {
                epoch(gate.on() ? beside : solo, nanos, true, true);
            }
        }
    }

    private static final long EPOCH = 20_000_000;

    @Test
    @DisplayName("tries groups beside one another after the first epochs on one lane, and keeps them while they are faster, measuring one lane again every so often")
    void keepsTheFaster() {
        var run = new Run();
        run.epochs(60, 12_800, 19_200, EPOCH);
        assertEquals(List.of(false, false, false, true), run.epochs.subList(0, 4), "the first epochs");
        var beside = run.epochs.stream().filter(on -> on).count();
        assertTrue(beside > 50, () -> beside + " of 60 epochs beside one another");
        // One epoch on one lane, then a try, after each run of kept epochs
        assertTrue(beside < 58, () -> beside + " of 60 epochs beside one another: one lane is never measured again");
    }

    @Test
    @DisplayName("gives way to one lane when groups beside one another are no faster, trying again after twice as many epochs each time, up to 64")
    void backsOff() {
        var run = new Run();
        run.epochs(400, 12_800, 12_800, EPOCH);
        var tries = new ArrayList<Integer>();
        for (var i = 0; i < run.epochs.size(); i++) {
            if (run.epochs.get(i)) {
                tries.add(i);
                assertFalse(i + 1 < run.epochs.size() && run.epochs.get(i + 1), "a try that lost was kept, at epoch " + i);
            }
        }
        // A try after the first epochs, then 2, 4, 8, 16, 32 and 64 epochs on one lane between two
        assertEquals(3, tries.getFirst());
        for (var index = 1; index < tries.size(); index++) {
            var gap = tries.get(index) - tries.get(index - 1) - 1;
            assertEquals(Math.min(1 << Math.min(index, OverlapGate.MAX_MISSES), 64) + 1, gap,
                    "the epochs on one lane before try " + index);
        }
    }

    @Test
    @DisplayName("never tries groups beside one another while most groups are forced, or wake no more than one commit")
    void idleStores() {
        var forced = new Run();
        for (var i = 0; i < 40; i++) {
            forced.epoch(12_800, EPOCH, false, true);
        }
        assertFalse(forced.epochs.contains(true), "a try in a store whose groups are forced");
        var alone = new Run();
        for (var i = 0; i < 40; i++) {
            alone.epoch(12_800, EPOCH, true, false);
        }
        assertFalse(alone.epochs.contains(true), "a try in a store of one committer");
    }

    @Test
    @DisplayName("decides nothing on an epoch that measured a store at rest, and nothing before an epoch is long enough")
    void restingStores() {
        var run = new Run();
        run.epochs(3, 12_800, 19_200, EPOCH);
        assertTrue(run.gate.on(), "no try after the first epochs");
        // The try sat through a pause: it runs again after one epoch on one lane, and wins then
        run.epoch(1, OverlapGate.EPOCH_MAX_NANOS * 2, true, true);
        assertFalse(run.gate.on(), "a try that measured a pause was kept");
        run.epochs(1, 12_800, 19_200, EPOCH);
        assertTrue(run.gate.on(), "no try after the pause");
        run.epochs(1, 12_800, 19_200, EPOCH);
        assertTrue(run.gate.on(), "a try that won was not kept");
        // Groups faster than the shortest epoch: the epoch goes on until it lasted long enough
        var fast = new Run();
        for (var i = 0; i < 10; i++) {
            fast.epoch(12_800, OverlapGate.EPOCH_NANOS / 100, true, true);
        }
        assertFalse(fast.gate.on(), "an epoch ended before it lasted long enough");
    }
}

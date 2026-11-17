package com.github.auties00.ledger.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/// Freezes the published chains of a store's paged key spaces into their runs between checkpoints.
///
/// A write not checkpointed yet keeps a chain, a version, the place of its value in the log or an array of its own, a
/// slot of the hash index and of the blind and dirty lists: about 400 bytes a message over its key and value, held
/// until the checkpoint that takes it, a quarter of a million messages after a history sync. The same entries in a
/// [PackedRun] take about 80 for a message id and its 32-byte value, and 75 for a message whose value is in the log.
/// So once a key space has listed enough chains since its last freeze, this moves those whose only version every
/// reader sees, and whose value left its batch's array, into a new frozen run over the latest tree, made of the run
/// before and of them, and drops the chains. Readers find the entries in the run as they found them in the chains
/// ([Reads]), later writes of their keys find the run's entry as the key's state, and the next checkpoint merges the run
/// into the tree, as it merges the run of an open's replay. Nothing durable changes: the log holds every write, a
/// segment that holds a frozen value is retired as one that holds a chain's would be, and a recovery replays as ever.
///
/// A chain is frozen only if its newest version is its only one, no reader is below it, it is newer than the latest
/// tree and older than every range removal since that tree that covers its key, and its value is in the log, on an
/// array of its own shorter than [SpillQueue#LOG_VALUE_MIN], or a removal: a value still on its batch's array waits for
/// the [SpillQueue] to move it, as freezing it would copy what the batch keeps anyway. A freeze runs only while every
/// reader reads the latest tree, and while the key space's run, if it has one, is a frozen one no checkpoint has taken:
/// a run lies over one tree, which every one of its readers reads.
///
/// One freeze runs at a time, on a virtual thread of the store's, holding the checkpointer's lock, so no checkpoint
/// cuts while it moves the chains: it takes the key space's dirty list, publishes the new run, waits for the scans and
/// counts of ranges that may still hold the run before ([RunWalkers]), drops the chains a few hundred at a time holding
/// the publishing role, as the sweep prunes them, and gives back the list of the chains it did not drop. A chain a
/// commit wrote again meanwhile stays, and shadows its entry. The blind writes of the chains it drops move from the
/// key space's [BlindWrites] to the run's entries, in one publication of the list. A checkpoint that starts or waits
/// for the lock meanwhile ([Checkpointer#wanted()]) waits for no merge and no drop: the freeze gives the chains back
/// unmerged if it has not published its run, and otherwise drops no more of them, leaving the rest beside their
/// entries for the checkpoint to take.
///
/// It runs once the key space listed at least the store's least number of chains since its last freeze, and as many
/// as its frozen run holds, so the rebuilds copy at most twice the run they end with; and once more as the log nears
/// the next checkpoint, so that checkpoint starts with most of the chains frozen. Only a key space whose samples show
/// it replaces few of its values before a checkpoint freezes ([ValueSlab.Overwrites#spills()]), and one whose freeze
/// finds a quarter of its chains' keys frozen and dropped already, written again since, freezes no more until the next
/// cut: overwritten keys would only go from chains to the run and back. A chain an earlier freeze left beside its entry,
/// as its wait for the walkers ran out or a checkpoint came, is not one. A freeze that finds fewer chains ready than half of
/// those it waited for doubles the wait, so chains whose values never leave their arrays do not start one freeze after
/// another.
final class RunFreezer {
    /// The chains a paged key space lists before its first freeze of a checkpoint interval, by default.
    static final int FREEZE_MIN = 16_384;
    /// The longest a freeze waits for the scans and counts of ranges that may hold the run before its own: past it, it
    /// drops nothing, and the chains stay beside their entries until the next freeze or checkpoint.
    private static final long GRACE_NANOS = 50_000_000L;
    /// The chains a freeze drops per hold of the publishing role, as many as the sweep prunes: a commit that finds the
    /// role held waits for at most this many drops, a few tens of microseconds.
    private static final int DROP_CHUNK = 256;
    /// The share of the frozen chains, a quarter, whose keys the run held already from a dropped chain past which a key
    /// space freezes no more until the next cut.
    private static final int CHURN_SHARE = 4;
    /// How long no freeze starts after one could not run: a checkpoint ran or was about to, or a reader held an older
    /// tree, or a run a checkpoint took waited for one, none of which the commits that ask meanwhile change.
    private static final long QUIET_NANOS = 20_000_000L;

    private final LiveLedger ledger;
    private final Checkpointer checkpointer;
    private final PageFile pages;
    // The key spaces that freeze
    private final List<KeyspaceState> states;
    private final AtomicBoolean running;
    // The log's bytes at the cut of the interval whose freeze near the next checkpoint ran; the freeze's alone, read by
    // commits without synchronization to start one
    private volatile long nearCutDone;
    // The freezes that dropped chains so far, for tests
    private volatile long freezes;
    // Before this time, by System.nanoTime(), no commit starts a freeze
    private volatile long quietUntil;
    // What a freeze runs once it published its run, before it waits for the walks and drops chains, or null; for tests
    // that write or read there. Plain: set before the freeze runs
    private Runnable publishedHookForTests;
    // What a freeze runs after each hold of the publishing role that drops chains, without the role, or null; for
    // tests that count or commit between the drops and the publication of the list. Plain: set before the freeze runs
    private Runnable droppedHookForTests;

    RunFreezer(LiveLedger ledger, Checkpointer checkpointer, PageFile pages, List<KeyspaceState> states) {
        this.ledger = ledger;
        this.checkpointer = checkpointer;
        this.pages = pages;
        this.states = List.copyOf(states);
        this.running = new AtomicBoolean();
        this.nearCutDone = Long.MIN_VALUE;
        this.quietUntil = System.nanoTime();
    }

    /// Starts a freeze on a thread of its own if a key space listed enough chains for one, or the log nears the next
    /// checkpoint, unless one runs; called by a commit once it published, without a role. A thread the system refuses
    /// leaves the chains as they are.
    void maybeStart() {
        if (running.get() || !due() || checkpointer.wanted() || System.nanoTime() - quietUntil < 0) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Thread.ofVirtual().name("ledger-freeze").start(this::run);
        } catch (Throwable _) {
            running.set(false);
        }
    }

    /// Makes every freeze run `hook` once it published its run, before it waits for the walks of the key space and drops
    /// its chains; for tests that write or read there.
    void publishedHookForTests(Runnable hook) {
        this.publishedHookForTests = hook;
    }

    /// Makes every freeze run `hook` after each hold of the publishing role that drops chains, before it publishes
    /// the list of the blind writes; for tests that count or commit there.
    void droppedHookForTests(Runnable hook) {
        this.droppedHookForTests = hook;
    }

    /// {@return the freezes that dropped chains so far; for tests}
    long freezesForTests() {
        return freezes;
    }

    // Whether a key space is due for a freeze
    private boolean due() {
        var near = nearCut();
        for (var i = 0; i < states.size(); i++) {
            var state = states.get(i);
            if (state.freezeDue() || near && nearDue(state)) {
                return true;
            }
        }
        return false;
    }

    // Whether the log nears the next checkpoint and no freeze ran for that yet
    private boolean nearCut() {
        return checkpointer.nearCut() && nearCutDone != checkpointer.cutBytes();
    }

    // Whether `state` listed enough chains for a freeze before the next checkpoint: a quarter of the least
    private static boolean nearDue(KeyspaceState state) {
        return state.freezeAt != KeyspaceState.NO_FREEZE && state.overwrites.spills()
                && state.freshDirty + state.deferredDirty >= state.freezeMin() / 4;
    }

    private void run() {
        try {
            freezeDue();
        } catch (RuntimeException _) {
            // The chains a failed freeze took went back; the next one tries again
        } finally {
            running.set(false);
        }
        // Commits that came while it ran found it running
        if (due() && !ledger.isClosed()) {
            maybeStart();
        }
    }

    /// Freezes every key space that is due, holding the checkpointer's lock; nothing if a checkpoint runs or is about to,
    /// as it takes the chains anyway.
    void freezeDue() {
        if (!checkpointer.lockForFreeze()) {
            quietUntil = System.nanoTime() + QUIET_NANOS;
            return;
        }
        try {
            var near = nearCut();
            if (near) {
                nearCutDone = checkpointer.cutBytes();
            }
            for (var state : states) {
                if (ledger.isClosed() || checkpointer.wanted()) {
                    return;
                }
                if ((state.freezeDue() || near && nearDue(state)) && !freeze(state).started()) {
                    quietUntil = System.nanoTime() + QUIET_NANOS;
                }
            }
        } finally {
            checkpointer.unlockFreeze();
        }
    }

    /// Freezes the chains of `state` it can now, whatever the key space listed; for tests, which run it on their own
    /// thread.
    ///
    /// @return what it did, or `null` if a checkpoint ran or was about to
    Outcome freezeForTests(KeyspaceState state) {
        if (!checkpointer.lockForFreeze()) {
            return null;
        }
        try {
            return freeze(state);
        } finally {
            checkpointer.unlockFreeze();
        }
    }

    /// What a freeze did.
    ///
    /// @param candidates the chains it found it could freeze
    /// @param dropped those it dropped once their run was published; the others shadow their entries
    /// @param handed the blind writes it handed from the list to the run, among those dropped
    /// @param started whether it ran at all: every reader read the latest tree, and the key space had no run a
    ///                checkpoint took or of the open's replay
    record Outcome(int candidates, int dropped, int handed, boolean started) {
        static final Outcome NOT_STARTED = new Outcome(0, 0, 0, false);
    }

    /// Freezes the chains of `state` it can now, holding the checkpointer's lock.
    private Outcome freeze(KeyspaceState state) {
        var start = ledger.beginFreeze(state);
        if (start == null) {
            return Outcome.NOT_STARTED;
        }
        var taken = start.taken();
        var kept = new ArrayList<Chain>();
        var deferred = 0;
        var threshold = state.freezeAt;
        var next = threshold;
        Chain[] candidates = null;
        var found = 0;
        Chain[] chains = null;
        Version[] heads = null;
        // -1 until the candidates are checked again in key order, the chains then given back by their index
        var count = -1;
        // The candidates the freeze dropped, by their index in `chains`
        var dropped = new BitSet();
        PackedRun.Merge merge = null;
        var published = false;
        var handed = 0;
        var removals = since(start.removals(), start.treeBound());
        try {
            candidates = new Chain[taken.size()];
            for (var chain : taken) {
                var verdict = verdict(chain, chain.head, start, removals);
                if (verdict == FREEZES) {
                    candidates[found++] = chain;
                } else {
                    kept.add(chain);
                    if (verdict == LATER) {
                        deferred++;
                    }
                }
            }
            chains = Arrays.copyOf(candidates, found);
            ChainSort.sort(chains);
            heads = new Version[found];
            // Checked again in the order of the keys, each with the head the run takes: a commit may have written one
            // since. A key twice, an old chain dropped and one created since, cannot pass both checks
            var checked = 0;
            var rejected = new ArrayList<Chain>();
            for (var i = 0; i < found; i++) {
                var chain = chains[i];
                var head = chain.head;
                var verdict = verdict(chain, head, start, removals);
                if (verdict != FREEZES || checked > 0 && Chain.sameKey(chains[checked - 1], chain)) {
                    rejected.add(chain);
                    if (verdict == LATER) {
                        deferred++;
                    }
                    continue;
                }
                chains[checked] = chain;
                heads[checked] = head;
                checked++;
            }
            kept.addAll(rejected);
            count = checked;
            if (checkpointer.wanted()) {
                // The checkpoint takes the chains as they are, and waits for no merge
                return new Outcome(count, 0, 0, true);
            }
            // Most of what the key space listed is not ready: the next freeze waits for twice as many, so chains whose
            // values stay on their arrays do not start one freeze after another
            var few = count < threshold / 2;
            if (few) {
                next = threshold > KeyspaceState.NO_FREEZE / 2 ? KeyspaceState.NO_FREEZE : 2 * threshold;
            }
            if (count == 0) {
                return new Outcome(0, 0, 0, true);
            }
            merge = PackedRun.merge(start.previous(), start.effects(), chains, heads, count, start.base(), pages);
            if (merge == null) {
                next = KeyspaceState.NO_FREEZE;
                return new Outcome(count, 0, 0, true);
            }
            if (!few) {
                next = Math.max(state.freezeMin(), merge.run().size());
            }
            if ((long) merge.replaced() * CHURN_SHARE > count) {
                next = KeyspaceState.NO_FREEZE;
            }
            if (checkpointer.wanted()) {
                // Nothing published yet: the checkpoint takes the chains and the run before, as if no freeze ran
                merge = null;
                return new Outcome(count, 0, 0, true);
            }
            ledger.publishFrozen(state, merge.run());
            published = true;
            var hook = publishedHookForTests;
            if (hook != null) {
                hook.run();
            }
            if (!checkpointer.wanted() && state.walkers.await(GRACE_NANOS)) {
                var latest = ledger.latestSequence();
                var effects = merge.effects();
                // A checkpoint that comes meanwhile takes the chains left beside their entries, which shadow them
                for (var from = 0; from < count && !checkpointer.wanted(); from += DROP_CHUNK) {
                    var first = from;
                    var to = Math.min(count, from + DROP_CHUNK);
                    var finalChains = chains;
                    var finalHeads = heads;
                    var finalMerge = merge;
                    var handedHere = new int[1];
                    ledger.holdingPublishing(() -> {
                        for (var i = first; i < to; i++) {
                            var chain = finalChains[i];
                            if (!state.drop(chain, finalHeads[i], latest)) {
                                continue;
                            }
                            dropped.set(i);
                            var entry = finalMerge.entry(i);
                            if (effects[entry] != PackedRun.SHADOWED) {
                                // The run before counted the key's blind write: the chain's writes were exact against it
                                continue;
                            }
                            if (chain.blindState() == TreeSearch.UNKNOWN) {
                                // Taken by no tree yet, as every checkpoint settles those up to its cut: the run counts
                                // it from now on, against the tree it lies over, which the write's tree held the key
                                // as well as
                                chain.markFrozen();
                                effects[entry] = chain.blindRemoves() ? PackedRun.BLIND_REMOVE : PackedRun.BLIND_PUT;
                                handedHere[0]++;
                            } else {
                                effects[entry] = PackedRun.COUNTED;
                            }
                        }
                    });
                    handed += handedHere[0];
                    var dropHook = droppedHookForTests;
                    if (dropHook != null) {
                        dropHook.run();
                    }
                }
                if (!dropped.isEmpty()) {
                    freezes++;
                }
            }
            return new Outcome(count, dropped.cardinality(), handed, true);
        } finally {
            if (count < 0) {
                // Failed before the candidates were checked again: every one goes back
                for (var i = 0; i < found; i++) {
                    kept.add(candidates[i]);
                }
            }
            for (var i = 0; i < count; i++) {
                if (!dropped.get(i)) {
                    kept.add(chains[i]);
                }
            }
            finish(state, published ? merge : null, handed > 0, kept, deferred, next);
        }
    }

    /// Publishes the list of the blind writes that counts the writes of `merge`'s run, without those it handed over if
    /// `handed`, and gives `kept` back to the dirty list of `state`; then takes the dropped chains out of the ordered map.
    private void finish(KeyspaceState state, PackedRun.Merge merge, boolean handed, ArrayList<Chain> kept, int deferred,
                        int next) {
        var blind = state.blind;
        BlindWrites.Listing listing = null;
        for (var attempt = 0; ; attempt++) {
            if (merge != null && handed) {
                // Collected without the role, after every write was marked: what changes meanwhile is copied after
                listing = blind.listing();
            }
            var prepared = listing;
            var force = attempt >= 2;
            var done = new boolean[1];
            ledger.holdingPublishing(() -> {
                if (merge != null) {
                    if (prepared == null) {
                        blind.replaceFrozen(merge.run(), merge.effects());
                    } else if (!blind.publishFrozen(prepared, merge.run(), merge.effects(), force)) {
                        return;
                    }
                }
                state.giveBackDirty(kept, deferred, next);
                done[0] = true;
            });
            if (done[0]) {
                break;
            }
        }
        state.removeDropped();
        state.pruneListed(Long.MIN_VALUE);
    }

    // What a freeze makes of a chain: it freezes it, it may later, or it will not before the next cut
    private static final int FREEZES = 0;
    private static final int LATER = 1;
    private static final int NEVER = 2;

    /// {@return whether `chain`, whose newest version is `head`, freezes now ([#FREEZES]), may once its value leaves its
    /// batch's array or every reader sees it ([#LATER]), or not before the next cut ([#NEVER])}
    ///
    /// @param removals the range removals of the key space since the latest tree
    private static int verdict(Chain chain, Version head, LiveLedger.FreezeStart start, List<RangeRemoval> removals) {
        if (head == null || chain.released()) {
            return NEVER;
        }
        if (head.external() != null) {
            return NEVER;
        }
        if (head.stamp <= start.treeBound()) {
            // In the tree already: the prune after the next checkpoint drops it
            return NEVER;
        }
        if (head.older != null || head.stamp > start.oldestBound()) {
            // The sweep shortens the chain, and readers move on
            return LATER;
        }
        if (!head.removes()) {
            if (head.valueLength() > PackedRun.MAX_VALUE_LENGTH) {
                return NEVER;
            }
            if (!head.logged()) {
                var own = head.ownArray();
                if (own == null || own.length >= SpillQueue.LOG_VALUE_MIN) {
                    // On its batch's array, or on one of its own the queue moves to the log
                    return LATER;
                }
            }
        }
        if (!removals.isEmpty()) {
            var key = chain.key();
            for (var removal : removals) {
                if (removal.stamp() < head.stamp && removal.covers(key)) {
                    // A reader of the run would apply the removal to the newer entry
                    return NEVER;
                }
            }
        }
        return FREEZES;
    }

    // The range removals of `removals` stamped after `treeBound`
    private static List<RangeRemoval> since(RangeRemoval[] removals, long treeBound) {
        List<RangeRemoval> since = List.of();
        for (var removal : removals) {
            if (removal.stamp() > treeBound) {
                if (since.isEmpty()) {
                    since = new ArrayList<>();
                }
                since.add(removal);
            }
        }
        return since;
    }
}

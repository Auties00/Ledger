package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

/// A batch on its way through the store's commit pipeline: prepared by the thread that commits it, given a sequence in
/// the order commits arrive, applied by that thread once the commits before it that write the same keys have applied
/// theirs, and published in the order of the sequences.
///
/// Each move of [#state] publishes what the thread making it wrote before: the sequencer's [#sequence] and
/// [#predecessors] at [#SEQUENCED], the committing thread's [#applier] and [#failure] at [#APPLIED], the publisher's
/// [#failure] at [#DONE].
///
/// A waiting thread spins on the line of the commit it waits for, never on a line every commit writes, then parks
/// with no timeout: every move that a parked thread may wait for is followed, before its mover reads whether one
/// parks, by a full fence, the mover's or one it shares with the moves of a whole pass. The sequencer and the
/// publisher move commits with release stores and fence once per pass ([#wake()]); the committing thread moves its
/// own to [#APPLIED] with a volatile store, since commits on its keys may wait for it.
///
/// A platform thread waiting for its commit to publish, or for an earlier one to apply, asks the store's [SpinGate]
/// whether it spins on once the wait outlasts [#FREE_SPINS], and yields instead while the gate is closed: most waits
/// end before, and touch none of the gate's lines. A thread waiting for a sequence is not gated, since the step it
/// waits for is short and the thread takes it itself whenever the sequencing role is free.
final class PendingCommit {
    /// Waiting for a sequence.
    static final int ARRIVED = 0;
    /// Given a sequence, its writes not yet applied.
    static final int SEQUENCED = 1;
    /// Its writes applied, or found failing and taken back, waiting for the commits before it to publish.
    static final int APPLIED = 2;
    /// Visible, or failed for good.
    static final int DONE = 3;

    /// The spins of a waiting platform thread before it yields; about 68 microseconds: a parked thread costs the
    /// thread that wakes it a system call, on the sequencer's or the publisher's path.
    static final int PLATFORM_SPINS = 2_048;
    /// The spins of a waiting platform thread before it asks the [SpinGate] whether it spins on; about 8
    /// microseconds, longer than most waits for a commit to publish, past four of the help points of
    /// [#awaitHelping(int, Padded.Role, Padded.Role, Runnable, SpinGate)].
    static final int FREE_SPINS = 256;
    /// The yields of a waiting platform thread before it parks.
    private static final int PLATFORM_YIELDS = 256;
    /// The spins of a waiting virtual thread before it yields its carrier: a spinning virtual thread keeps the
    /// carrier from the thread it may be waiting for.
    private static final int VIRTUAL_SPINS = 32;
    /// The yields of a waiting virtual thread before it parks.
    private static final int VIRTUAL_YIELDS = 4;
    /// The longest a parked thread waits before it looks again, in nanoseconds; never needed, since every move wakes
    /// the threads parked for it, and only a guard against a wake-up the protocol misses.
    static final long PARK_NANOS = Boolean.getBoolean("ledger.debug.untimedPark") ? 0 : 50_000_000L;
    private static final VarHandle STATE;
    private static final VarHandle NEXT;
    private static final VarHandle WAITERS;
    private static final VarHandle ARRIVAL;

    static {
        try {
            var lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(PendingCommit.class, "state", int.class);
            NEXT = lookup.findVarHandle(PendingCommit.class, "next", PendingCommit.class);
            WAITERS = lookup.findVarHandle(PendingCommit.class, "waiters", Waiter.class);
            ARRIVAL = lookup.findVarHandle(PendingCommit.class, "arrival", PendingCommit.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    /// The batch.
    final ArrayWriteBatch writes;
    /// The thread that commits it, which the sequencer and the publisher wake.
    final Thread owner;
    /// The chain of each write's key, opened while the commit prepared, `null` for a range removal.
    final Chain[] chains;
    /// The key space of each write.
    final KeyspaceState[] states;
    /// The decoded value of each put to a resident key space, or `null`.
    final Object[] decoded;
    /// The unit of the file of large values of each put, or `null`.
    final PageRef[] units;
    /// What the lookups before the pipeline found in the trees, or `null`.
    final LiveLedger.Hints hints;
    /// Per write, the commit given a write of the same key just before this one, or `null`; set by the sequencer.
    PendingCommit[] predecessors;
    /// The commit given the next sequence, or `null`; set by the sequencer through [#follow(PendingCommit)].
    volatile PendingCommit next;
    /// The sequence, or 0 before the sequencer gives one; [#SEQUENCED] publishes it.
    long sequence;
    /// [#ARRIVED], [#SEQUENCED], [#APPLIED] or [#DONE].
    volatile int state;
    /// The writes as applied, which the publisher publishes; set by the committing thread before [#APPLIED].
    LiveLedger.Applier applier;
    /// Why the commit failed, or `null`; set before [#state] moves on.
    Throwable failure;
    /// Whether publishing the commit only moves the latest sequence; set by the committing thread before [#APPLIED].
    boolean light;
    /// The commit that arrived after this one, waiting for a sequence, or `null` before it links itself here and once
    /// the sequencer moved past this one.
    volatile PendingCommit arrival;
    /// The next commit the same pass of the sequencer gave a sequence, which that thread alone reads and writes.
    PendingCommit sequencedNext;
    /// The next commit the same pass of the publisher published, which that thread alone reads and writes.
    PendingCommit publishedNext;
    /// Whether the committing thread parks, or is about to, waiting for [#state] to move.
    private volatile boolean parking;
    /// The threads of later commits parked until this one applies, or `null`.
    private volatile Waiter waiters;

    PendingCommit(ArrayWriteBatch writes, Chain[] chains, KeyspaceState[] states, Object[] decoded, PageRef[] units,
                  LiveLedger.Hints hints) {
        this.writes = writes;
        this.owner = Thread.currentThread();
        this.chains = chains;
        this.states = states;
        this.decoded = decoded;
        this.units = units;
        this.hints = hints;
    }

    /// Creates the commit the pipeline starts from: the last one published when the store opened, at `sequence`.
    PendingCommit(long sequence) {
        this(null, null, null, null, null, null);
        this.sequence = sequence;
        this.state = DONE;
    }

    /// {@return whether its writes are applied, or it will never apply any}
    boolean applied() {
        return state >= APPLIED;
    }

    /// Waits, on the thread of a later commit, until this commit has applied its writes, or will never apply any.
    ///
    /// @param gate the gate of the store, which a platform thread asks whether it spins on past [#FREE_SPINS]
    void awaitApplied(SpinGate gate) {
        var virtual = Thread.currentThread().isVirtual();
        var spins = virtual ? VIRTUAL_SPINS : PLATFORM_SPINS;
        var yields = spins + (virtual ? VIRTUAL_YIELDS : PLATFORM_YIELDS);
        var holding = false;
        try {
            for (var spin = 0; !applied(); spin++) {
                if (spin == FREE_SPINS && !virtual) {
                    var admission = gate.admit();
                    if (admission == SpinGate.HOLDING) {
                        holding = true;
                    } else if (admission == SpinGate.GATED) {
                        // Yields from this turn on, its budget of yields unchanged
                        spins = spin;
                        yields = spins + PLATFORM_YIELDS;
                    }
                }
                if (spin < spins) {
                    Thread.onSpinWait();
                    continue;
                }
                if (holding) {
                    holding = false;
                    gate.release();
                }
                if (spin < yields) {
                    if (spin == spins && !virtual) {
                        gate.yields(spin);
                    }
                    Thread.yield();
                } else {
                    var waiter = new Waiter(Thread.currentThread());
                    while (true) {
                        var head = waiters;
                        waiter.next = head;
                        // A compare-and-set, so a full fence before the state is read again
                        if (WAITERS.compareAndSet(this, head, waiter)) {
                            break;
                        }
                    }
                    while (!applied()) {
                        park(this, gate);
                    }
                    return;
                }
            }
        } finally {
            if (holding) {
                gate.release();
            }
        }
    }

    /// Waits, on the committing thread, until [#state] reaches `wanted`, which another thread moves it to.
    ///
    /// @param wanted [#SEQUENCED] or [#DONE]
    ///
    /// @return whether it did; `false` after a park that ended before, so the caller may help and wait again
    boolean await(int wanted) {
        return await(wanted, null, null, null);
    }

    /// Waits as [#await(int)] does, and returns `false` instead of parking while `role` is free or held briefly:
    /// a thread that takes it briefly lets it go without looking for this commit, so this thread must help.
    ///
    /// While `roleWanted` is raised it parks instead, the role free or not: the thread that raised it takes the role
    /// next, and gives the waiting commits their turn when it lets it go. Helping is then forbidden, and a thread that
    /// kept spinning and yielding would only take the processor, or the carrier, from the one thread that can move on.
    ///
    /// @param roleWanted the flag raised for an operation that wants `role` for itself, or `null`
    /// @param gate the gate of the store, which lowers the thread's flag while it parks, or `null`
    boolean await(int wanted, Padded.Role role, Padded.Role roleWanted, SpinGate gate) {
        var virtual = Thread.currentThread().isVirtual();
        var spins = virtual ? VIRTUAL_SPINS : PLATFORM_SPINS;
        var yields = spins + (virtual ? VIRTUAL_YIELDS : PLATFORM_YIELDS);
        for (var spin = 0; state < wanted; spin++) {
            if (roleWanted != null && roleWanted.held()) {
                // Skips the spinning: the operation that wants the role runs for a while
                spin = Math.max(spin, yields);
            } else if (role != null && (spin & 63) == 63 && !role.held()) {
                return false;
            }
            if (spin < spins) {
                Thread.onSpinWait();
            } else if (spin < yields) {
                Thread.yield();
            } else {
                // Announced with a volatile store, a full fence, before the state and the role are read again
                parking = true;
                if (state < wanted && (role == null || (roleWanted != null && roleWanted.held())
                        || (role.held() && !role.heldBriefly()))) {
                    park(this, gate);
                }
                parking = false;
                return state >= wanted;
            }
        }
        return true;
    }

    /// Waits, on the committing thread, until [#state] reaches `wanted`, running `help` every few spins and before
    /// each yield while `role` is free, and once after each park.
    ///
    /// Unlike [#await(int, Padded.Role, Padded.Role, SpinGate)], it keeps its count of spins across the help, so it
    /// still parks once it has spun and yielded its budget: helping finds nothing to do while an earlier commit has
    /// not applied, and its thread wakes this one when it is published. While `roleWanted` is raised it parks at
    /// once, as that wait does. A platform thread that the gate does not let spin past [#FREE_SPINS] yields, still
    /// helping before each yield; one that spins holding a slot of the gate gives it back while it helps, since a
    /// help may publish and maintain the store for a while, and spins on only if it takes a slot again.
    ///
    /// @param gate the gate of the store, which a platform thread asks whether it spins on past [#FREE_SPINS]
    void awaitHelping(int wanted, Padded.Role role, Padded.Role roleWanted, Runnable help, SpinGate gate) {
        var virtual = Thread.currentThread().isVirtual();
        var spins = virtual ? VIRTUAL_SPINS : PLATFORM_SPINS;
        var yields = spins + (virtual ? VIRTUAL_YIELDS : PLATFORM_YIELDS);
        var holding = false;
        try {
            for (var spin = 0; state < wanted; spin++) {
                if (roleWanted.held()) {
                    spin = Math.max(spin, yields);
                } else if (spin == FREE_SPINS && !virtual) {
                    var admission = gate.admit();
                    if (admission == SpinGate.HOLDING) {
                        holding = true;
                    } else if (admission == SpinGate.GATED) {
                        // Yields from this turn on, its budget of yields unchanged
                        spins = spin;
                        yields = spins + PLATFORM_YIELDS;
                    }
                }
                if (spin < spins) {
                    if ((spin & 63) == 63 && !role.held()) {
                        if (holding) {
                            holding = false;
                            gate.release();
                            help.run();
                            holding = gate.reclaim();
                            if (!holding) {
                                // Yields from the next turn on
                                spins = spin + 1;
                                yields = spins + PLATFORM_YIELDS;
                            }
                        } else {
                            help.run();
                        }
                    } else {
                        Thread.onSpinWait();
                    }
                    continue;
                }
                if (holding) {
                    holding = false;
                    gate.release();
                }
                if (spin < yields) {
                    if (spin == spins && !virtual) {
                        gate.yields(spin);
                    }
                    if (!role.held()) {
                        help.run();
                    }
                    if (state < wanted) {
                        Thread.yield();
                    }
                } else {
                    // Announced with a volatile store, a full fence, before the state is read again
                    parking = true;
                    if (state < wanted) {
                        park(this, gate);
                    }
                    parking = false;
                    // A park that ended early, a spurious wake-up or the guard's timeout, helps once
                    if (state < wanted) {
                        help.run();
                    }
                }
            }
        } finally {
            if (holding) {
                gate.release();
            }
        }
    }

    /// Parks the current thread, a committing one, lowering its flag of `gate` while it does, unless `gate` is `null`.
    private static void park(Object blocker, SpinGate gate) {
        if (gate != null) {
            gate.parks();
        }
        try {
            if (PARK_NANOS == 0) {
                LockSupport.park(blocker);
            } else {
                LockSupport.parkNanos(blocker, PARK_NANOS);
            }
        } finally {
            if (gate != null) {
                gate.unparks();
            }
        }
    }

    /// Moves the commit to `state` with a release store, waking no thread: the mover calls [#wake()] after a full
    /// fence.
    void advanceLazily(int state) {
        STATE.setRelease(this, state);
    }

    /// Wakes the committing thread if it parks, after a full fence that followed the move it waits for.
    void wake() {
        if (parking) {
            LockSupport.unpark(owner);
        }
    }

    /// Moves the commit to `state` and wakes its committing thread if it parks.
    void advance(int state) {
        STATE.setVolatile(this, state);
        wake();
    }

    /// Moves the commit to [#APPLIED], on its committing thread, and wakes the later commits parked for it.
    void advanceApplied() {
        // A volatile store, a full fence, before the waiters are read
        STATE.setVolatile(this, APPLIED);
        if (waiters != null) {
            for (var waiter = (Waiter) WAITERS.getAndSet(this, (Waiter) null); waiter != null; waiter = waiter.next) {
                LockSupport.unpark(waiter.thread);
            }
        }
    }

    /// Makes `commit`, which arrived after this one, the next in the arrival list, with a release store.
    void linkArrival(PendingCommit commit) {
        ARRIVAL.setRelease(this, commit);
    }

    /// Forgets the commit that arrived after this one, once the sequencer moved past this one: as with [#unlink()],
    /// a consumed commit the collector promoted would otherwise keep every later arrival reachable.
    void unlinkArrival() {
        ARRIVAL.set(this, null);
    }

    /// Makes `commit` the commit given the sequence after this one's.
    void follow(PendingCommit commit) {
        NEXT.setRelease(this, commit);
    }

    /// Forgets the commit after this one, once that one is published.
    ///
    /// A published commit the collector moved to the old generation would otherwise keep every commit after it
    /// reachable through [#next], and each young collection would copy them all, with their batches, until the old
    /// generation is collected.
    void unlink() {
        NEXT.set(this, null);
    }

    /// Fails the commit before it was given a sequence, and wakes its committing thread.
    void fail(RuntimeException failure) {
        this.failure = failure;
        advance(DONE);
    }

    /// A thread parked until a commit applies; allocated only by a thread about to park, never reused.
    private static final class Waiter {
        final Thread thread;
        Waiter next;

        Waiter(Thread thread) {
            this.thread = thread;
        }
    }
}

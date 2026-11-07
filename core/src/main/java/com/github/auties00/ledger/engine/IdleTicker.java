package com.github.auties00.ledger.engine;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

/// The thread of the process that gives back what its stores hold while they are idle, one tick every
/// [#PERIOD_MILLIS]: the free arrays of pools no one took one from, the cached pages no read used, and the writes the
/// trees of a store that stopped writing lack.
///
/// It holds what it watches weakly, so a store that is never closed stops being watched once the collector takes it.
/// It starts with the first object it watches and never stops; between ticks it is a parked virtual thread.
final class IdleTicker {
    /// The time between two ticks: a pool that goes a tick without an acquire frees its arrays, a cached page that goes
    /// two ticks without a read leaves the cache, and a store that goes a tick without a commit checkpoints.
    static final long PERIOD_MILLIS = 30_000;
    // What the thread ticks, weakly; guarded by itself
    private static final Set<Watched> WATCHED = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    static {
        Thread.ofVirtual().name("ledger-idle-ticker").start(IdleTicker::run);
    }

    private IdleTicker() {
    }

    /// What the thread calls at every tick.
    interface Watched {
        /// Gives back what the object no longer needs, on the ticker's thread; it must not block for long, nor throw.
        void tick();
    }

    /// Calls [Watched#tick()] of `watched` at every tick, until [#unwatch(Watched)] or the collector takes it.
    static void watch(Watched watched) {
        WATCHED.add(watched);
    }

    /// Stops calling [Watched#tick()] of `watched`.
    static void unwatch(Watched watched) {
        WATCHED.remove(watched);
    }

    private static void run() {
        while (true) {
            try {
                Thread.sleep(PERIOD_MILLIS);
            } catch (InterruptedException _) {
                return;
            }
            for (var watched : WATCHED.toArray(Watched[]::new)) {
                try {
                    watched.tick();
                } catch (RuntimeException _) {
                    // One that fails loses its tick, not the others theirs
                }
            }
        }
    }
}

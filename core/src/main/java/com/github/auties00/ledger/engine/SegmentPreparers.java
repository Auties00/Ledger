package com.github.auties00.ledger.engine;

import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/// The daemon platform threads named `ledger-prepare` that prepare the log's segments, shared by every store of the
/// process.
///
/// A preparation creates and forces a file, tens of milliseconds on Windows, so it runs on a platform thread of its
/// own rather than on the store's pool, where a flush queued behind it would hold up every commit waiting on that
/// flush. The log starts a preparation between a group's write and the wake of its commits, and creating a platform
/// thread takes the system far longer than waking one: so a thread that finished a preparation waits
/// [#KEEP_ALIVE_SECONDS] for the next, which the next segment, or the next store opened in the process, hands to it for
/// the cost of a wake. With no thread waiting, a virtual thread creates one, so neither the flush nor the commit that
/// starts a preparation waits for a thread to be created.
///
/// A preparation no thread could be started for stays unstarted; the rollover that needs it, or the close that keeps
/// its segment, runs it, as they run one whose thread has not started yet. A [FutureTask] runs once, whichever of them
/// gets to it first.
final class SegmentPreparers {
    /// How long a thread waits for another preparation before it ends.
    static final long KEEP_ALIVE_SECONDS = 60;

    // Where a thread waiting for a preparation takes one; an offer only succeeds while one waits
    private static final SynchronousQueue<FutureTask<?>> HANDOFF = new SynchronousQueue<>();
    // The threads waiting for a preparation, and the threads started, for tests
    private static final AtomicInteger WAITING = new AtomicInteger();
    private static final AtomicInteger STARTED = new AtomicInteger();

    private SegmentPreparers() {
    }

    /// Runs `preparation` on a thread of its own, handing it to a thread waiting for one if any does, otherwise to a
    /// new one, which a virtual thread starts; it returns without waiting for either.
    static void start(FutureTask<?> preparation) {
        if (HANDOFF.offer(preparation)) {
            return;
        }
        try {
            Thread.ofVirtual().name("ledger-prepare-start").start(() -> spawn(preparation));
        } catch (OutOfMemoryError _) {
            // Left unstarted, for the rollover or the close that needs it to run
        }
    }

    /// Hands `preparation` to a thread that came to wait meanwhile, or starts one for it.
    private static void spawn(FutureTask<?> preparation) {
        if (HANDOFF.offer(preparation)) {
            return;
        }
        try {
            Thread.ofPlatform().daemon().name("ledger-prepare").start(() -> serve(preparation));
            STARTED.incrementAndGet();
        } catch (OutOfMemoryError _) {
            // Left unstarted, for the rollover or the close that needs it to run
        }
    }

    /// Runs `first`, then every preparation handed over until none comes for [#KEEP_ALIVE_SECONDS].
    private static void serve(FutureTask<?> first) {
        var preparation = first;
        while (preparation != null) {
            // A FutureTask keeps whatever its work throws for whoever waits on it
            preparation.run();
            // Nothing interrupts these threads, but a preparation that did must not end the wait for the next
            var _ = Thread.interrupted();
            WAITING.incrementAndGet();
            try {
                preparation = HANDOFF.poll(KEEP_ALIVE_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
                preparation = null;
            } finally {
                WAITING.decrementAndGet();
            }
        }
    }

    /// {@return the threads waiting for a preparation, for tests}
    static int waitingForTests() {
        return WAITING.get();
    }

    /// {@return the threads started so far, for tests}
    static int startedForTests() {
        return STARTED.get();
    }
}

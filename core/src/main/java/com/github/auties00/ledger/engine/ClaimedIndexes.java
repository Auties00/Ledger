package com.github.auties00.ledger.engine;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

/// Work on the indexes below a size that the calling thread shares with virtual threads it starts, each thread taking
/// the next index no other took, and that the caller waits for by its indexes rather than by its threads.
///
/// An application's virtual threads may hold every carrier for as long as they compute, which nothing preempts: a
/// thread that gets no carrier takes no index, as the caller does every one no other took, so the caller never waits
/// for a thread with nothing left to do. Such a thread, once it starts, finds every index taken and ends at once,
/// without reading the work, which is let go of once every index is done, so it keeps nothing the work holds from the
/// collector for as long as it waits for a carrier.
final class ClaimedIndexes implements Runnable {
    private final int size;
    private final AtomicInteger claimed;
    private final AtomicInteger finished;
    private final AtomicReference<Throwable> failure;
    // Released once every index is finished
    private final Semaphore done;
    // Read only by a thread that holds an unfinished index; cleared once none does
    private IntConsumer work;

    private ClaimedIndexes(int size, IntConsumer work) {
        this.size = size;
        this.claimed = new AtomicInteger();
        this.finished = new AtomicInteger();
        this.failure = new AtomicReference<>();
        this.done = new Semaphore(0);
        this.work = work;
    }

    /// Runs `work` for every index from 0 below `size`, on the calling thread and on up to `helpers` virtual threads
    /// named `name`, and returns once every index is done, whatever the interrupt status of the caller, which it keeps.
    ///
    /// After the first failure the indexes not started are skipped. A thread the system refuses fails the work, as no
    /// thread may start after it.
    ///
    /// @throws RuntimeException the first failure of `work`, or of a thread that would not start
    /// @throws Error the first error, likewise
    static void run(String name, int size, int helpers, IntConsumer work) {
        if (helpers <= 0 || size <= 1) {
            for (var i = 0; i < size; i++) {
                work.accept(i);
            }
            return;
        }
        var indexes = new ClaimedIndexes(size, work);
        try {
            for (var i = Math.min(helpers, size - 1); i > 0; i--) {
                Thread.ofVirtual().name(name).start(indexes);
            }
        } catch (Throwable throwable) {
            indexes.failure.compareAndSet(null, throwable);
        }
        indexes.run();
        indexes.await();
    }

    /// Does the work of the indexes no thread took, until none is left; after a failure it only marks them finished.
    @Override
    public void run() {
        for (var i = claimed.getAndIncrement(); i < size; i = claimed.getAndIncrement()) {
            try {
                if (failure.get() == null) {
                    work.accept(i);
                }
            } catch (Throwable throwable) {
                failure.compareAndSet(null, throwable);
            } finally {
                if (finished.incrementAndGet() == size) {
                    done.release();
                }
            }
        }
    }

    /// Waits until every index is finished and throws the first failure; only after [#run()] on the same thread, which
    /// leaves no index to take.
    private void await() {
        done.acquireUninterruptibly();
        work = null;
        switch (failure.get()) {
            case null -> {
            }
            case RuntimeException exception -> throw exception;
            case Error error -> throw error;
            case Throwable other -> throw new IllegalStateException(other);
        }
    }
}

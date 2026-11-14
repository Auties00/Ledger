package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.IoPool;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/// An [IoPool] of daemon platform threads named `ledger-io-N`.
///
/// A pool made with [#PlatformIoPool(int)] has a fixed number of threads and a queue its tasks wait in. The process's
/// flusher ([#process()]), which the stores opened without a pool share, has no queue: a task goes to a thread that
/// waits for one, or to a new thread, and a thread ends after [#KEEP_ALIVE_SECONDS] without a task. A store's log runs
/// one flush at a time, so the flusher holds a thread per store flushing at the same moment, and none once the stores
/// have been idle for that long; it hands a task to the thread that waited the least, which for one busy store is the
/// thread of its last flush.
///
/// A task that finds no thread waiting while a thread of the flusher is between two tasks spins up to
/// [#HANDOFF_NANOS] for it to come back before a thread is made for it: a commit that writes its own group hands the
/// store's flush a task that ends at once, and the thread woken for it may not be waiting again by the store's next
/// commit, which with a fast device comes sooner than the system wakes a thread. Without the wait, each such commit
/// would make a thread that then waits a minute for work. The wait never parks, as the system may round a timed park up
/// to its timer resolution (a millisecond or more on Windows), and it is skipped when every thread runs a task: a thread
/// writing another store's flush does not come back in time, so the task gets a new thread at once. The task's caller
/// may hold its store's publishing role, which every other commit of that store waits for.
public final class PlatformIoPool implements IoPool {
    /// How long a thread of the process's flusher waits for another flush before it ends.
    static final long KEEP_ALIVE_SECONDS = 60;
    /// How long a task of the process's flusher spins for a thread of it between two tasks to come back before one is
    /// made: several times what a running thread takes to finish a task and wait for the next, and a fraction of the
    /// start of a new thread.
    static final long HANDOFF_NANOS = TimeUnit.MICROSECONDS.toNanos(100);
    // Guards process
    private static final Object PROCESS_LOCK = new Object();
    // The flusher of the stores opened without a pool, made at the first open; never shut down
    private static PlatformIoPool process;

    private final int threads;
    private final ThreadPoolExecutor executor;
    // Whether the pool is the process's flusher, which no store closes
    private final boolean shared;

    /// Creates a pool of `threads` threads, started as work arrives.
    ///
    /// @param threads the number of threads, at least 1
    /// @throws IllegalArgumentException if `threads` is below 1
    public PlatformIoPool(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("A pool needs at least one thread: " + threads);
        }
        this.threads = threads;
        this.executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                factory());
        this.shared = false;
    }

    /// Creates a pool with no queue and no bound on its threads, each made by `factory` as a task finds none waiting,
    /// and ended after [#KEEP_ALIVE_SECONDS] without a task.
    ///
    /// No bound, so the pool never refuses a task: a refused flush would stop its store's log.
    private PlatformIoPool(ThreadFactory factory) {
        this.threads = Integer.MAX_VALUE;
        var handoff = new Handoff();
        this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, handoff,
                factory);
        handoff.executor = executor;
        this.shared = true;
    }

    /// {@return the flusher the stores of the process opened without a pool share}
    static PlatformIoPool process() {
        synchronized (PROCESS_LOCK) {
            var pool = process;
            if (pool == null) {
                pool = process = new PlatformIoPool(factory());
            }
            return pool;
        }
    }

    /// {@return a pool shaped as the process's flusher, its threads made by `factory`, which no store closes; for
    /// tests}
    static PlatformIoPool sharedForTests(ThreadFactory factory) {
        return new PlatformIoPool(factory);
    }

    /// {@return the maker of the threads: daemons that inherit no inheritable thread locals}
    ///
    /// A thread of a pool may be made on a commit's thread and later run the flushes of other stores, so it carries
    /// none of that commit's thread locals.
    private static ThreadFactory factory() {
        return Thread.ofPlatform().daemon().inheritInheritableThreadLocals(false).name("ledger-io-", 0).factory();
    }

    @Override
    public int threads() {
        return threads;
    }

    /// {@return the executor the stores of this pool run their I/O on}
    Executor executor() {
        return executor;
    }

    /// {@return whether a task waits for a thread of the pool}
    boolean queued() {
        return !executor.getQueue().isEmpty();
    }

    /// {@return the threads the pool holds now, running a task or waiting for one, for tests}
    int threadsForTests() {
        return executor.getPoolSize();
    }

    /// {@return how many tasks of a pool shaped as the process's flusher spun for a thread to come back, for tests; 0
    /// for any other pool}
    long handoffWaitsForTests() {
        return executor.getQueue() instanceof Handoff handoff ? handoff.waits.sum() : 0;
    }

    /// Sets how long a thread of a pool shaped as the process's flusher waits for a task before it ends, for tests.
    void keepAliveForTests(long time, TimeUnit unit) {
        executor.setKeepAliveTime(time, unit);
    }

    /// The hand-off of the process's flusher: a task that finds no thread waiting spins up to [#HANDOFF_NANOS] for one
    /// while a thread of the flusher is between two tasks, and is refused at once, so that the executor makes a thread
    /// for it, while every thread runs a task.
    private static final class Handoff extends SynchronousQueue<Runnable> {
        // The tasks that spun for a thread, for tests
        private final LongAdder waits = new LongAdder();
        // The executor that takes from it, set once it is built
        private volatile ThreadPoolExecutor executor;

        @Override
        public boolean offer(Runnable task) {
            if (super.offer(task)) {
                return true;
            }
            var pool = executor;
            if (pool == null || pool.getActiveCount() >= pool.getPoolSize()) {
                return false;
            }
            waits.increment();
            var deadline = System.nanoTime() + HANDOFF_NANOS;
            do {
                Thread.onSpinWait();
                if (super.offer(task)) {
                    return true;
                }
            } while (System.nanoTime() - deadline < 0);
            return false;
        }
    }

    /// Stops the threads, unless the pool is the process's flusher, which lasts as long as the process.
    @Override
    public void close() {
        if (!shared) {
            executor.shutdown();
        }
    }
}

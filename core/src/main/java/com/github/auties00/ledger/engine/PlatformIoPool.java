package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.IoPool;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/// An [IoPool] of daemon platform threads named `ledger-io-N`.
public final class PlatformIoPool implements IoPool {
    private final int threads;
    private final ThreadPoolExecutor executor;

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
                Thread.ofPlatform().daemon().name("ledger-io-", 0).factory());
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

    @Override
    public void close() {
        executor.shutdown();
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.IoPool;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/// An [IoPool] of daemon platform threads named `ledger-io-N`.
public final class PlatformIoPool implements IoPool {
    private final int threads;
    private final ExecutorService executor;

    /// Creates a pool of `threads` threads, started as work arrives.
    ///
    /// @param threads the number of threads, at least 1
    /// @throws IllegalArgumentException if `threads` is below 1
    public PlatformIoPool(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("A pool needs at least one thread: " + threads);
        }
        this.threads = threads;
        this.executor = Executors.newFixedThreadPool(threads, Thread.ofPlatform().daemon().name("ledger-io-", 0).factory());
    }

    @Override
    public int threads() {
        return threads;
    }

    /// {@return the executor the stores of this pool run their I/O on}
    Executor executor() {
        return executor;
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}

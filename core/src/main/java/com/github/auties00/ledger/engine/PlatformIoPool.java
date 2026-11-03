package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.IoPool;

/// An [IoPool] of platform threads.
public final class PlatformIoPool implements IoPool {
    private final int threads;

    /// Creates a pool of `threads` threads.
    ///
    /// @param threads the number of threads, at least 1
    /// @throws IllegalArgumentException if `threads` is below 1
    public PlatformIoPool(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("A pool needs at least one thread: " + threads);
        }
        this.threads = threads;
    }

    @Override
    public int threads() {
        return threads;
    }

    @Override
    public void close() {
        // The threads start with the first store that uses the pool, which the engine does not open yet
    }
}

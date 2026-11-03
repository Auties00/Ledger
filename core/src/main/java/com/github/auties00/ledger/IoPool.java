package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.PlatformIoPool;

/// The platform threads that write and force the logs of one or more stores.
///
/// Stores write and force their logs on platform threads so a force never holds the carrier of a virtual thread, and
/// so an interrupted caller never interrupts a write midway, which would close the log under every other caller.
/// Several stores of one application can share a pool: a process holding many small stores needs a few threads,
/// not a few per store.
public sealed interface IoPool extends AutoCloseable permits PlatformIoPool {
    /// {@return a pool of `threads` platform threads}
    ///
    /// @param threads the number of threads, at least 1
    /// @throws IllegalArgumentException if `threads` is below 1
    static IoPool create(int threads) {
        return new PlatformIoPool(threads);
    }

    /// {@return a pool sized for the machine: one thread per two processors, from 1 to 4}
    static IoPool create() {
        return new PlatformIoPool(Math.clamp(Runtime.getRuntime().availableProcessors() / 2, 1, 4));
    }

    /// {@return the number of threads}
    int threads();

    /// Stops the threads.
    ///
    /// @throws IllegalStateException if a store using the pool is still open
    @Override
    void close();
}

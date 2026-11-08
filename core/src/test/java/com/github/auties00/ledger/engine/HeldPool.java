package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.IoPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/// The thread of a one-thread pool kept busy, so no flush queued on the pool runs until this is closed.
///
/// A commit that waits alone with the pool held always writes its own group. Otherwise it races the thread its append
/// woke for the group, and how often it wins depends on the machine: there are runs of 40 commits in which it never
/// does.
final class HeldPool implements AutoCloseable {
    private final CountDownLatch released = new CountDownLatch(1);

    /// Waits until the thread is held, past any flush it is running.
    HeldPool(IoPool pool) throws InterruptedException {
        var held = new CountDownLatch(1);
        ((PlatformIoPool) pool).executor().execute(() -> {
            held.countDown();
            try {
                // Bounded, so a commit that does wait for the pool fails its test where it would hang it
                released.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });
        held.await();
    }

    @Override
    public void close() {
        released.countDown();
    }
}

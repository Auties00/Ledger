package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The work on claimed indexes that the reads of many units and the seals of a checkpoint's flush share with virtual
/// threads: each index once, the first failure once every started index ended, and no wait for a thread that never got
/// a carrier.
@Timeout(300)
class ClaimedIndexesTest {
    @Test
    @DisplayName("does the work of every index once, on the caller and the helpers")
    void everyIndexOnce() {
        var size = 10_000;
        var done = new AtomicIntegerArray(size);
        var threads = ConcurrentHashMap.<Thread>newKeySet();
        var caller = Thread.currentThread();
        // A helper that starts at once could take every index before the caller takes its first, so the helpers hold
        // their first index until the caller has one: the caller claims after it starts them, whatever they do
        var callerClaimed = new CountDownLatch(1);
        ClaimedIndexes.run("test-claimed", size, 7, index -> {
            done.incrementAndGet(index);
            threads.add(Thread.currentThread());
            if (Thread.currentThread() == caller) {
                callerClaimed.countDown();
            } else {
                try {
                    if (!callerClaimed.await(60, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("the caller claimed no index");
                    }
                } catch (InterruptedException exception) {
                    throw new RuntimeException(exception);
                }
            }
        });
        for (var i = 0; i < size; i++) {
            assertEquals(1, done.get(i), "index " + i);
        }
        assertTrue(threads.contains(caller), "the caller did no index");
    }

    @Test
    @DisplayName("throws the first failure only once every index a thread started has ended, and skips the rest")
    void failure() {
        var size = 1_000;
        var failure = new IllegalStateException("the first");
        var running = new AtomicInteger();
        var finished = new AtomicInteger();
        var started = new CountDownLatch(2);
        var thrown = assertThrows(IllegalStateException.class, () -> ClaimedIndexes.run("test-claimed", size, 3, index -> {
            running.incrementAndGet();
            try {
                started.countDown();
                if (index == 0) {
                    // Until another index runs beside this one, which then outlives the throw
                    started.await(30, TimeUnit.SECONDS);
                    throw failure;
                }
                Thread.sleep(5);
                finished.incrementAndGet();
            } catch (InterruptedException exception) {
                throw new RuntimeException(exception);
            } finally {
                running.decrementAndGet();
            }
        }));
        assertSame(failure, thrown);
        assertEquals(0, running.get(), "an index still ran after the failure was thrown");
        assertTrue(finished.get() < size - 1, "no index was skipped after the failure");
    }

    @Test
    @DisplayName("ends while an application's virtual threads compute on every carrier, its helpers never starting")
    void busyCarriers() throws Exception {
        var size = 64;
        var done = new AtomicInteger();
        CheckpointThreadsTest.assertEndsWithBusyCarriers(() -> ClaimedIndexes.run("test-claimed", size, 8, _ -> done.incrementAndGet()));
        assertEquals(size, done.get());
    }

    @Test
    @DisplayName("keeps the interrupt of a caller interrupted before it starts, and still waits for every index")
    void interrupt() {
        var size = 256;
        var done = new AtomicInteger();
        Thread.currentThread().interrupt();
        try {
            ClaimedIndexes.run("test-claimed", size, 4, _ -> {
                done.incrementAndGet();
                Thread.onSpinWait();
            });
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals(size, done.get());
    }
}

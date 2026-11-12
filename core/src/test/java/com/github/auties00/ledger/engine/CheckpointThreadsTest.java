package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The threads a checkpoint runs on, beside each other and beside the application's.
class CheckpointThreadsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());

    // Long keys far apart, so the writes reach most of the leaves under every branch of a tree of two levels
    private static String key(int number) {
        return "%08d".formatted(Integer.reverse(number) >>> 1) + "k".repeat(700);
    }

    private static void write(Ledger ledger, int count, int step, java.util.function.IntFunction<String> value) throws InterruptedException {
        var batch = ledger.newBatch();
        for (var i = 0; i < count; i += step) {
            batch.put(MESSAGES, key(i), value.apply(i));
            if (i % (500 * step) == 500 * step - step) {
                ledger.commit(batch);
                batch = ledger.newBatch();
            }
        }
        ledger.commit(batch).awaitDurable();
    }

    @ParameterizedTest(name = "{0} keys, every {1}, the values {2} long or more")
    @CsvSource({"30000, 1, 900", "30000, 1, 40", "120000, 3, 40"})
    @Timeout(300)
    @DisplayName("a checkpoint that fails a write while it rewrites its trees in parallel has stopped every task when it throws, and the retry leaves every page used or free and every key readable")
    void failedWrite(int count, int step, int length) throws Exception {
        var disk = CrashFileSystem.create();
        var writes = new AtomicInteger();
        var failAfter = new AtomicInteger(Integer.MAX_VALUE);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null && path.getFileName().toString().equals(PageFile.FILE)) {
                writes.incrementAndGet();
                if (failAfter.getAndDecrement() == 1) {
                    throw new IOException("No space left on device");
                }
            }
        });
        var directory = disk.getPath("/store");
        java.util.function.IntFunction<String> before = _ -> "a".repeat(40);
        java.util.function.IntFunction<String> after = i -> "b".repeat(length + i % 3 * 300);
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                .checkpointEvery(1L << 40).build()) {
            write(ledger, count, 1, before);
            ledger.checkpoint();
            write(ledger, count, step, after);
            // The third write: the first flush happens with the update under way, once 8 MiB are queued
            failAfter.set(3);
            assertThrows(LedgerIoException.class, ledger::checkpoint);
            var atFailure = writes.get();
            // A task left running reaches its next flush within this
            Thread.sleep(500);
            assertEquals(atFailure, writes.get(), "a task of the failed checkpoint went on writing after it threw");
            ledger.checkpoint();
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES).build()) {
            assertEquals(count, ledger.size(MESSAGES));
            for (var i = 0; i < count; i++) {
                assertEquals(Optional.of((i % step == 0 ? after : before).apply(i)), ledger.get(MESSAGES, key(i)), "key " + i);
            }
        }
        CheckpointTest.verifyPages(directory, true);
    }

    @Test
    @Timeout(300)
    @DisplayName("a checkpoint on a platform thread ends while an application's virtual threads compute on every carrier")
    void busyCarriers() throws Exception {
        var disk = CrashFileSystem.create();
        try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                .checkpointEvery(1L << 40).build()) {
            write(ledger, 30_000, 1, _ -> "a".repeat(40));
            assertEndsWithBusyCarriers(ledger::checkpoint);
        }
    }

    @Test
    @Timeout(300)
    @DisplayName("the force beside a checkpoint that lags ends while an application's virtual threads compute on every carrier, whose own thread never starts")
    void forceOnBusyCarriers(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(KEY, "AES")), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1, () -> true);
            assertEndsWithBusyCarriers(() -> {
                try {
                    // Past the 32 MiB from which a checkpoint that lags forces beside its work
                    for (var i = 0; i < 8_200; i++) {
                        writer.write(PageFormat.VALUE, 0, space.allocate(1), 1, new byte[PageFormat.HEADER + 100], 100, false);
                    }
                    writer.flush();
                    writer.awaitForce();
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            });
        }
    }

    static void assertEndsWithBusyCarriers(Runnable work) throws InterruptedException {
        var carriers = Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors());
        var stop = new AtomicBoolean();
        var computing = new CountDownLatch(carriers);
        var threads = new ArrayList<Thread>();
        try {
            for (var i = 0; i < carriers; i++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    computing.countDown();
                    while (!stop.get()) {
                        Thread.onSpinWait();
                    }
                }));
            }
            assertTrue(computing.await(30, TimeUnit.SECONDS), "the virtual threads did not all get a carrier");
            var ended = new CountDownLatch(1);
            Thread.ofPlatform().daemon().start(() -> {
                work.run();
                ended.countDown();
            });
            // Time for the work on a machine with every processor busy, far from the moment it would hang
            assertTrue(ended.await(120, TimeUnit.SECONDS), "the work waits for a carrier");
        } finally {
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
        }
    }
}

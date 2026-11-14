package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The channels a data file and a file of large values read and write through: opened as callers at once need them,
/// never more than the limit, waited for when the system refuses another, and closed once the pool is, including those
/// a caller held at the close.
class ChannelPoolTest {
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    private static Path file(Path directory) throws IOException {
        var path = directory.resolve("file");
        Files.write(path, new byte[]{1, 2, 3, 4});
        return path;
    }

    @Test
    @DisplayName("opens one channel at once, another only while every open one is held, and none past the limit")
    void onDemand() throws Exception {
        try (var pool = new ChannelPool(file(directory()), 3, StandardOpenOption.READ)) {
            assertEquals(1, pool.openedForTests());
            var first = pool.take();
            pool.give(first);
            assertSame(first, pool.take(), "an idle channel is reused before another is opened");
            assertEquals(1, pool.openedForTests());
            var second = pool.take();
            var third = pool.take();
            assertEquals(3, pool.openedForTests());
            var taken = new AtomicReference<FileChannel>();
            var waiter = Thread.ofPlatform().start(() -> taken.set(pool.take()));
            waiter.join(200);
            assertTrue(waiter.isAlive(), "a fourth caller opened a channel past the limit");
            pool.give(second);
            waiter.join(10_000);
            assertSame(second, taken.get());
            assertEquals(3, pool.openedForTests());
            pool.give(first);
            pool.give(third);
            pool.give(taken.get());
            assertEquals(3, pool.openedForTests());
        }
    }

    @Test
    @DisplayName("waits for a held channel when the system refuses another, and counts the refused one as never opened")
    void refusedOpen() throws Exception {
        var directory = directory();
        var path = file(directory);
        try (var pool = new ChannelPool(path, 4, StandardOpenOption.READ)) {
            var first = pool.take();
            // The open channel keeps the file; a new open of the name fails, as one out of descriptors would
            Files.delete(path);
            var taken = new AtomicReference<FileChannel>();
            var waiter = Thread.ofPlatform().start(() -> taken.set(pool.take()));
            waiter.join(200);
            assertTrue(waiter.isAlive(), "a caller neither waited nor failed");
            assertEquals(1, pool.openedForTests());
            pool.give(first);
            waiter.join(10_000);
            assertSame(first, taken.get());
            var buffer = ByteBuffer.allocate(4);
            assertEquals(4, taken.get().read(buffer, 0), "the channel still reads the file");
            pool.give(taken.get());
        }
    }

    @Test
    @DisplayName("opens a channel in place of one an interrupt closed, keeping the count")
    void replacesClosed() throws Exception {
        try (var pool = new ChannelPool(file(directory()), 2, StandardOpenOption.READ)) {
            var first = pool.take();
            first.close();
            pool.give(first);
            var replaced = pool.take();
            assertNotSame(first, replaced);
            assertTrue(replaced.isOpen());
            assertEquals(1, pool.openedForTests());
            pool.give(replaced);
        }
    }

    @Test
    @DisplayName("closes the idle channels at the close, a held one when it comes back, and refuses callers after it")
    void close() throws Exception {
        var pool = new ChannelPool(file(directory()), 4, StandardOpenOption.READ);
        var held = pool.take();
        var idle = pool.take();
        pool.give(idle);
        pool.close();
        assertFalse(idle.isOpen());
        assertTrue(held.isOpen());
        pool.give(held);
        assertFalse(held.isOpen(), "a channel given back after the close stayed open");
        assertEquals(0, pool.openedForTests());
        assertThrows(IllegalStateException.class, pool::take);
    }

    @Test
    @DisplayName("ends the wait of a caller with every channel held once the pool closes")
    void closeWhileWaiting() throws Exception {
        var pool = new ChannelPool(file(directory()), 1, StandardOpenOption.READ);
        var held = pool.take();
        var failure = new AtomicReference<Throwable>();
        var waiter = Thread.ofPlatform().start(() -> {
            try {
                pool.give(pool.take());
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        });
        waiter.join(200);
        assertTrue(waiter.isAlive());
        pool.close();
        waiter.join(10_000);
        assertFalse(waiter.isAlive(), "the waiter still waits after the close");
        assertTrue(failure.get() instanceof IllegalStateException, "the waiter ended with " + failure.get());
        pool.give(held);
        assertFalse(held.isOpen());
    }

    @Test
    @DisplayName("hands each channel to one caller at a time under contention, opens at most the limit, and closes every one")
    void contention() throws Exception {
        var pool = new ChannelPool(file(directory()), 4, StandardOpenOption.READ);
        var held = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<FileChannel, Boolean>()));
        Set<FileChannel> seen = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var start = new CountDownLatch(1);
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 16; t++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    var buffer = ByteBuffer.allocate(1);
                    for (var i = 0; i < 2_000; i++) {
                        var channel = pool.take();
                        seen.add(channel);
                        if (!held.add(channel)) {
                            failures.add(new AssertionError("a channel was handed to two callers"));
                        }
                        buffer.clear();
                        channel.read(buffer, i % 4);
                        held.remove(channel);
                        pool.give(channel);
                    }
                } catch (Throwable throwable) {
                    failures.add(throwable);
                }
            }));
        }
        start.countDown();
        for (var thread : threads) {
            thread.join(60_000);
        }
        assertTrue(failures.isEmpty(), () -> "failures: " + failures);
        assertTrue(pool.openedForTests() <= 4, "the pool opened " + pool.openedForTests() + " channels");
        assertTrue(seen.size() <= 4, "callers saw " + seen.size() + " channels");
        pool.close();
        for (var channel : seen) {
            assertFalse(channel.isOpen());
        }
    }

    @Test
    @DisplayName("leaves an open store that read from one thread at a time with one channel to each of its files")
    void storeOpensOne() throws Exception {
        var directory = directory();
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32])).keyspace(NAMES).build()) {
            ledger.commit(ledger.newBatch().put(NAMES, "small", "v").put(NAMES, "large", "v".repeat(2 << 20))).awaitDurable();
            ledger.checkpoint();
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32])).keyspace(NAMES).build()) {
            assertEquals("v", ledger.get(NAMES, "small").orElseThrow());
            assertEquals(2 << 20, ledger.get(NAMES, "large").orElseThrow().length());
            var pages = ((LiveLedger) ledger).pagesForTests();
            assertEquals(1, pages.readChannelsForTests());
            assertEquals(1, pages.blobs().channelsForTests());
        }
    }

    @TempDir
    Path temporary;

    private Path directory() throws IOException {
        return Files.createTempDirectory(temporary, "pool");
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The stream a resident key space's loader reads its leaves from ([LeafStream]): the arrays it gives back to the pool,
/// the trees of every height, failed reads on its workers, a close while it reads on every channel, and an open that
/// loads at open while an application's virtual threads hold every carrier. Values of 1,400 bytes put two entries in a
/// leaf.
class ResidentStreamTest {
    private static final byte[] KEY = new byte[32];
    private static final int ENTRIES = 1_200;
    private static final Keyspace<String, String> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.utf8());
    // The value whose decoding fails, or null
    private static final AtomicReference<String> REFUSED = new AtomicReference<>();
    // The decoding that waits for the test, or null
    private static final AtomicReference<CountDownLatch[]> HELD = new AtomicReference<>();
    private static final AtomicReference<String> HELD_VALUE = new AtomicReference<>();
    // The memory the trusted decoder of TRUSTED read, as it read it, by key
    private static final ConcurrentLinkedQueue<Object[]> READ = new ConcurrentLinkedQueue<>();
    private static final Keyspace<String, String> FRAGILE = Keyspace.resident("fragile", KeyCodec.utf8(), ValueCodec.of(
            String::length,
            (value, target) -> MemorySegment.copy(value.getBytes(StandardCharsets.US_ASCII), 0, target, ValueLayout.JAVA_BYTE, 0, value.length()),
            source -> {
                var value = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII);
                if (value.equals(REFUSED.get())) {
                    throw new IllegalStateException("refused " + value);
                }
                var held = HELD.get();
                if (held != null && value.equals(HELD_VALUE.get())) {
                    held[0].countDown();
                    try {
                        assertTrue(held[1].await(60, TimeUnit.SECONDS), "never released");
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException(exception);
                    }
                }
                return value;
            }));
    private static final Keyspace<String, String> TRUSTED = Keyspace.resident("trusted", KeyCodec.utf8(), ValueCodec.ofTrusted(
            String::length,
            (value, target) -> MemorySegment.copy(value.getBytes(StandardCharsets.US_ASCII), 0, target, ValueLayout.JAVA_BYTE, 0, value.length()),
            source -> {
                var value = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII);
                // A decoder that breaks its contract by keeping the memory, so the test sees what happens to it after
                READ.add(new Object[]{value, source});
                return value;
            }));

    private static String key(int index) {
        return "s%05d".formatted(index);
    }

    private static String value(int index) {
        return "v" + index + "-" + "x".repeat(1_400);
    }

    private static Ledger open(Path directory, boolean atOpen) {
        return open(directory, atOpen, _ -> {
        });
    }

    private static Ledger open(Path directory, boolean atOpen, java.util.function.Consumer<? super LedgerException> failures) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(SESSIONS).keyspace(FRAGILE).keyspace(TRUSTED)
                .loadResidentAtOpen(atOpen).onBackgroundFailure(failures).build();
    }

    private static TreeMap<String, String> populate(Path directory, Keyspace<String, String> keyspace) {
        var model = new TreeMap<String, String>();
        try (var ledger = open(directory, false)) {
            for (var start = 0; start < ENTRIES; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < start + 100; i++) {
                    batch.put(keyspace, key(i), value(i));
                    model.put(key(i), value(i));
                }
                ledger.commit(batch);
            }
        }
        return model;
    }

    private static void awaitLoaded(LiveLedger ledger, Keyspace<?, ?> keyspace) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!ledger.residentLoadedForTests(keyspace)) {
            assertTrue(System.nanoTime() < deadline, "the load did not end");
            Thread.sleep(1);
        }
    }

    private static void checkAll(Ledger ledger, Keyspace<String, String> keyspace, TreeMap<String, String> model) {
        for (var entry : model.entrySet()) {
            assertEquals(Optional.of(entry.getValue()), ledger.get(keyspace, entry.getKey()), entry.getKey());
            assertEquals(Optional.of(entry.getValue()), ledger.readEncoded(keyspace, entry.getKey(),
                    segment -> new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII)), entry.getKey());
        }
        assertEquals(model.size(), ledger.size(keyspace));
        var visited = new ArrayList<String>();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
            visited.add(key);
            assertEquals(model.get(key), value, key);
            return true;
        });
        assertEquals(new ArrayList<>(model.keySet()), visited);
    }

    /// Pauses the loader at a step until the test releases it.
    private static final class Pause implements IntConsumer {
        private final int step;
        private final CountDownLatch paused = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        Pause(int step) {
            this.step = step;
        }

        @Override
        public void accept(int step) {
            if (step != this.step || paused.getCount() == 0) {
                return;
            }
            paused.countDown();
            try {
                assertTrue(released.await(60, TimeUnit.SECONDS), "never released");
            } catch (InterruptedException exception) {
                throw new IllegalStateException(exception);
            }
        }

        void awaitPaused() throws InterruptedException {
            assertTrue(paused.await(60, TimeUnit.SECONDS), "never paused at " + step);
        }

        void release() {
            released.countDown();
        }
    }

    /// {@return the reference of the leaf of the tree of `keyspace` that would hold `key`, read past the cache}
    private static PageRef leafOf(LiveLedger live, Keyspace<?, ?> keyspace, String key) {
        var pages = live.writerPoint(0).pages();
        var root = live.tree().root(live.state(keyspace).id);
        var ref = root.ref();
        var unit = pages.read(ref, PageFile.NODE, false);
        var bytes = MemorySegment.ofArray(key.getBytes(StandardCharsets.UTF_8));
        while (!TreeNodes.isLeaf(unit)) {
            ref = TreeNodes.childRef(unit, TreeNodes.childFor(unit, bytes));
            unit = pages.read(ref, PageFile.NODE, false);
        }
        return ref;
    }

    @Test
    @DisplayName("the leaves that reads cached before and during the load are installed from the cache, and none of the cache's arrays goes to the pool")
    void cachedLeavesStayOutOfThePool(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(0);
            live.hookResidentLoadsForTests(pause);
            var released = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>()));
            live.state(SESSIONS).loading.releaseHookForTests = released::add;
            pause.awaitPaused();
            // The reads of keys the loader has not reached cache their leaves
            var cached = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
            var pages = live.writerPoint(0).pages();
            var root = live.tree().root(live.state(SESSIONS).id);
            for (var i = 0; i < ENTRIES; i += 7) {
                assertEquals(Optional.of(value(i)), ledger.get(SESSIONS, key(i)));
                cached.add(TreeSearch.leaf(pages, root, key(i).getBytes(StandardCharsets.UTF_8)));
            }
            var failures = new ConcurrentLinkedQueue<String>();
            var stop = new AtomicBoolean();
            var readers = new ArrayList<Thread>();
            for (var reader = 0; reader < 4; reader++) {
                readers.add(Thread.ofPlatform().start(() -> {
                    var random = ThreadLocalRandom.current();
                    while (!stop.get()) {
                        var index = random.nextInt(ENTRIES);
                        try {
                            if (!ledger.get(SESSIONS, key(index)).equals(Optional.of(value(index)))) {
                                failures.add("read " + key(index) + " wrong");
                            }
                        } catch (RuntimeException exception) {
                            failures.add("read failed: " + exception);
                        }
                    }
                }));
            }
            pause.release();
            awaitLoaded(live, SESSIONS);
            stop.set(true);
            for (var reader : readers) {
                reader.join();
            }
            assertTrue(failures.isEmpty(), failures.size() + " failures, first: " + failures.peek());
            assertFalse(released.isEmpty(), "no array went back to the pool");
            for (var array : cached) {
                assertFalse(released.contains(array), "an array of the cache went back to the pool");
            }
            assertEquals(ENTRIES, live.state(SESSIONS).chains());
            checkAll(ledger, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("nothing the loader installs reads the arrays it gives back to the pool, which the pool hands out again")
    void installedEntriesOwnTheirMemory(@TempDir Path directory) throws Exception {
        var model = populate(directory, TRUSTED);
        READ.clear();
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(TRUSTED, key(0)));
            awaitLoaded(live, TRUSTED);
            // Whatever the pool holds now is written over, as the store's next use of it would
            var pool = HeapPool.process();
            var taken = new ArrayList<byte[]>();
            for (var i = 0; i < 4_096; i++) {
                var array = pool.acquire(PageFormat.SIZE);
                java.util.Arrays.fill(array, (byte) 0x7F);
                taken.add(array);
            }
            taken.forEach(pool::release);
            var changed = READ.stream().anyMatch(read -> {
                var segment = (MemorySegment) read[1];
                return !new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII).equals(read[0]);
            });
            assertTrue(changed, "the pool handed out none of the arrays the loader gave back");
            assertEquals(ENTRIES, live.state(TRUSTED).chains());
            checkAll(ledger, TRUSTED, model);
            for (var i = 0; i < ENTRIES; i += 97) {
                assertSame(ledger.get(TRUSTED, key(i)).orElseThrow(), ledger.get(TRUSTED, key(i)).orElseThrow());
            }
        } finally {
            READ.clear();
        }
    }

    @ParameterizedTest(name = "{0} entries, keys of {1} bytes, values of {2}: a root of level {3}")
    @CsvSource({"1, 6, 10, 0", "40, 6, 1400, 1", "1200, 6, 1400, 2", "1200, 900, 10, 3"})
    @DisplayName("a tree of any height loads, from its start and, after a failed load, from the middle")
    void treesOfEveryHeight(int entries, int keyLength, int valueLength, int least, @TempDir Path directory) throws Exception {
        var model = new TreeMap<String, String>();
        try (var ledger = open(directory, false)) {
            for (var start = 0; start < entries; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < Math.min(entries, start + 100); i++) {
                    var key = (key(i) + "p".repeat(keyLength)).substring(0, Math.max(6, keyLength));
                    var value = ("v" + i + "-" + "x".repeat(valueLength)).substring(0, valueLength);
                    batch.put(FRAGILE, key, value);
                    model.put(key, value);
                }
                ledger.commit(batch);
            }
        }
        var middle = new ArrayList<>(model.keySet()).get(entries / 2);
        var reports = new LinkedBlockingQueue<LedgerException>();
        // A tree of one entry has no middle to fail at
        REFUSED.set(entries > 1 ? model.get(middle) : null);
        try (var ledger = open(directory, false, reports::add)) {
            var live = (LiveLedger) ledger;
            var root = live.writerPoint(0).pages().read(live.tree().root(live.state(FRAGILE).id).ref(), PageFile.NODE, false);
            var level = TreeNodes.isLeaf(root) ? 0 : TreeNodes.level(root);
            if (least < 3) {
                assertEquals(least, level);
            } else {
                assertTrue(level >= least, "a tree of level " + level);
            }
            var first = model.firstKey();
            assertEquals(Optional.of(model.get(first)), ledger.get(FRAGILE, first));
            if (entries > 1) {
                assertNotNull(reports.poll(60, TimeUnit.SECONDS));
                assertFalse(live.residentLoadedForTests(FRAGILE));
                var loader = live.state(FRAGILE).loading;
                var limit = new String(loader.limit().toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
                assertTrue(limit.compareTo(middle) <= 0, limit);
            }
            REFUSED.set(null);
            // A write past the position retries the load, from where the first stopped
            ledger.commit(ledger.newBatch().put(FRAGILE, model.lastKey(), "last"));
            model.put(model.lastKey(), "last");
            awaitLoaded(live, FRAGILE);
            assertEquals(model.size(), live.state(FRAGILE).chains());
            checkAll(ledger, FRAGILE, model);
        } finally {
            REFUSED.set(null);
        }
        try (var ledger = open(directory, true)) {
            assertTrue(((LiveLedger) ledger).residentLoadedForTests(FRAGILE));
            checkAll(ledger, FRAGILE, model);
        }
    }

    @Test
    @DisplayName("a leaf that fails authentication on a worker fails the load past the leaves before it, every retry too, and no close waits for it")
    void corruptLeaf(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        PageRef corrupt;
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(0);
            live.hookResidentLoadsForTests(pause);
            corrupt = leafOf(live, SESSIONS, key(900));
            pause.release();
        }
        try (var channel = FileChannel.open(directory.resolve(PageFile.FILE), StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), PageFormat.offset(corrupt.page()) + 1_000);
        }
        var reports = new LinkedBlockingQueue<LedgerException>();
        try (var ledger = open(directory, false, reports::add)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            var report = reports.poll(60, TimeUnit.SECONDS);
            assertInstanceOf(LedgerCorruptedException.class, report);
            assertFalse(live.residentLoadedForTests(SESSIONS));
            // Every leaf before the one that failed is in memory
            var limit = new String(live.state(SESSIONS).loading.limit().toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
            assertTrue(limit.compareTo(key(898)) > 0 && limit.compareTo(key(900)) <= 0, limit);
            assertEquals(Optional.of(value(10)), ledger.get(SESSIONS, key(10)));
            assertEquals(Optional.of(value(1_100)), ledger.get(SESSIONS, key(1_100)));
            // A write retries it on every read channel, and fails again
            assertInstanceOf(LedgerCorruptedException.class, assertThrows(LedgerCorruptedException.class,
                    () -> ledger.commit(ledger.newBatch().put(SESSIONS, key(1_000), "x"))).getCause());
            assertNotNull(reports.poll(60, TimeUnit.SECONDS));
            assertEquals(LeafStream.MAX_WORKERS, live.state(SESSIONS).loading.workersForTests());
            assertEquals(ENTRIES, ledger.size(SESSIONS));
            ledger.commit(ledger.newBatch().put(SESSIONS, key(5), "below"));
            model.put(key(5), "below");
            assertEquals(Optional.of("below"), ledger.get(SESSIONS, key(5)));
        }
        assertThrows(LedgerCorruptedException.class, () -> open(directory, true));
    }

    @Test
    @DisplayName("a close while the loader reads on every channel waits for the reads its workers claimed, and the next open loads the rest")
    void closeDuringHurriedStream(@TempDir Path directory) throws Exception {
        var model = populate(directory, FRAGILE);
        var held = new CountDownLatch[]{new CountDownLatch(1), new CountDownLatch(1)};
        HELD_VALUE.set(value(400));
        HELD.set(held);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var ledger = open(directory, false);
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            ledger.get(FRAGILE, key(0));
            pause.awaitPaused();
            var waiting = executor.submit(() -> ledger.commit(ledger.newBatch().put(FRAGILE, key(1_000), "never")));
            Thread.sleep(100);
            pause.release();
            assertTrue(held[0].await(60, TimeUnit.SECONDS));
            assertEquals(LeafStream.MAX_WORKERS, live.state(FRAGILE).loading.workersForTests());
            var closing = executor.submit(ledger::close);
            Thread.sleep(100);
            assertFalse(closing.isDone());
            held[1].countDown();
            closing.get(60, TimeUnit.SECONDS);
            var failure = assertThrows(ExecutionException.class, () -> waiting.get(60, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertFalse(live.residentLoadedForTests(FRAGILE));
        } finally {
            held[1].countDown();
            HELD.set(null);
        }
        try (var reopened = open(directory, false)) {
            checkAll(reopened, FRAGILE, model);
        }
    }

    @Test
    @DisplayName("an open that loads at open ends while an application's virtual threads hold every carrier")
    void loadsWithoutCarriers(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        var carriers = Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors());
        var stop = new AtomicBoolean();
        var spinning = new CountDownLatch(carriers);
        var spinners = new ArrayList<Thread>();
        var opened = new AtomicReference<Ledger>();
        var failure = new AtomicReference<Throwable>();
        try {
            for (var i = 0; i < carriers; i++) {
                spinners.add(Thread.ofVirtual().start(() -> {
                    spinning.countDown();
                    while (!stop.get()) {
                        Thread.onSpinWait();
                    }
                }));
            }
            assertTrue(spinning.await(60, TimeUnit.SECONDS));
            var opener = Thread.ofPlatform().start(() -> {
                try {
                    opened.set(open(directory, true));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            opener.join(TimeUnit.SECONDS.toMillis(60));
            assertFalse(opener.isAlive(), "the open waits for a thread with no carrier");
            assertNull(failure.get());
        } finally {
            stop.set(true);
            for (var spinner : spinners) {
                spinner.join();
            }
        }
        try (var ledger = opened.get()) {
            assertTrue(((LiveLedger) ledger).residentLoadedForTests(SESSIONS));
            checkAll(ledger, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("scans, reads and writes beside loaders that run, pause and restart lose nothing")
    void raceTheStream(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        var expected = new ArrayList<>(model.keySet());
        for (var round = 0; round < 6; round++) {
            try (var ledger = open(directory, false)) {
                var live = (LiveLedger) ledger;
                var delay = round % 3;
                live.hookResidentLoadsForTests(_ -> {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException(exception);
                    }
                });
                var failures = new ConcurrentLinkedQueue<String>();
                var threads = new ArrayList<Thread>();
                for (var scan = 0; scan < 3; scan++) {
                    var descending = scan == 1;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            var keys = new ArrayList<String>();
                            ledger.forEach(SESSIONS, descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all(), (key, value) -> {
                                keys.add(key);
                                if (!value.equals(model.get(key))) {
                                    failures.add("scan read " + key + " wrong");
                                }
                                return true;
                            });
                            var order = new ArrayList<>(expected);
                            if (descending) {
                                Collections.reverse(order);
                            }
                            if (!keys.equals(order)) {
                                failures.add("scan read " + keys.size() + " keys");
                            }
                        } catch (RuntimeException exception) {
                            failures.add("scan failed: " + exception);
                        }
                    }));
                }
                var round0 = round;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var index = (round0 * 211) % ENTRIES;
                        ledger.commit(ledger.newBatch().put(SESSIONS, key(index), value(index)));
                        for (var i = 0; i < ENTRIES; i += 13) {
                            if (!ledger.get(SESSIONS, key(i)).equals(Optional.of(value(i)))) {
                                failures.add("read " + key(i) + " wrong");
                            }
                        }
                    } catch (RuntimeException exception) {
                        failures.add("writer failed: " + exception);
                    }
                }));
                for (var thread : threads) {
                    thread.join();
                }
                assertTrue(failures.isEmpty(), failures.size() + " failures, first: " + failures.peek());
                // Half the rounds close before the load ends
                if (round % 2 == 0) {
                    awaitLoaded(live, SESSIONS);
                    assertEquals(ENTRIES, live.state(SESSIONS).chains());
                }
            }
        }
        try (var ledger = open(directory, true)) {
            checkAll(ledger, SESSIONS, model);
        }
    }
}

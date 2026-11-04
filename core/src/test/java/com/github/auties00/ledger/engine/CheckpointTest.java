package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Checkpoints: the trees they write, what reads see across them, what reopening finds, and the pages they use.
class CheckpointTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
    private static final ValueCodec<Long> LONGS = ValueCodec.ofTrusted(_ -> Long.BYTES,
            (value, target) -> target.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, value),
            source -> source.get(ValueLayout.JAVA_LONG_UNALIGNED, 0));

    private static Ledger open(Path directory, Encryption encryption, Keyspace<?, ?>... keyspaces) {
        var options = Ledger.builder().directory(directory).encryption(encryption);
        for (var keyspace : keyspaces) {
            options.keyspace(keyspace);
        }
        return options.build();
    }

    private static Ledger open(Path directory, Keyspace<?, ?>... keyspaces) {
        return open(directory, Encryption.key(KEY), keyspaces);
    }

    private static void crash(Ledger ledger) {
        ((LiveLedger) ledger).closeWithoutCheckpoint();
    }

    @Nested
    @DisplayName("random histories")
    class RandomHistories {
        @ParameterizedTest(name = "encrypted: {0}, seed {1}")
        @CsvSource({"true, 3", "false, 5", "true, 17", "false, 23", "true, 41"})
        @DisplayName("reads match a model through checkpoints, reopens and crashes")
        void model(boolean encrypted, long seed, @TempDir Path directory) throws Exception {
            var random = new Random(seed);
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var models = new HashMap<Keyspace<String, String>, TreeMap<String, String>>();
            for (var keyspace : List.of(MESSAGES, CHATS, NOTES)) {
                models.put(keyspace, new TreeMap<>());
            }
            var ledger = open(directory, encryption, MESSAGES, CHATS, NOTES);
            var snapshots = new ArrayList<PinnedSnapshot>();
            try {
                for (var step = 0; step < 300; step++) {
                    var choice = random.nextInt(20);
                    if (choice == 0) {
                        ledger.checkpoint();
                    } else if (choice == 1 && snapshots.isEmpty()) {
                        ledger.close();
                        ledger = open(directory, encryption, MESSAGES, CHATS, NOTES);
                    } else if (choice == 2 && snapshots.isEmpty()) {
                        ledger.awaitDurable();
                        crash(ledger);
                        ledger = open(directory, encryption, MESSAGES, CHATS, NOTES);
                    } else if (choice == 3 && snapshots.size() < 2) {
                        snapshots.add(new PinnedSnapshot(ledger.snapshot(), copy(models)));
                    } else if (choice == 4 && !snapshots.isEmpty()) {
                        snapshots.removeFirst().snapshot().close();
                    } else if (choice == 5) {
                        transfer(ledger, models, random);
                    } else {
                        batch(ledger, models, random, step < 10 ? 600 : 40);
                    }
                    if (step % 3 == 0) {
                        for (var keyspace : models.keySet()) {
                            verify(ledger, keyspace, models.get(keyspace), random);
                        }
                        for (var pinned : snapshots) {
                            for (var keyspace : models.keySet()) {
                                verify(pinned.snapshot(), keyspace, pinned.models().get(keyspace), random);
                            }
                        }
                    }
                }
            } finally {
                for (var pinned : snapshots) {
                    pinned.snapshot().close();
                }
                ledger.close();
            }
            try (var reopened = open(directory, encryption, MESSAGES, CHATS, NOTES)) {
                for (var keyspace : models.keySet()) {
                    verify(reopened, keyspace, models.get(keyspace), random);
                }
            }
            verifyPages(directory, encrypted);
        }
    }

    private record PinnedSnapshot(Snapshot snapshot, Map<Keyspace<String, String>, TreeMap<String, String>> models) {
    }

    private static Map<Keyspace<String, String>, TreeMap<String, String>> copy(Map<Keyspace<String, String>, TreeMap<String, String>> models) {
        var copy = new HashMap<Keyspace<String, String>, TreeMap<String, String>>();
        models.forEach((keyspace, model) -> copy.put(keyspace, new TreeMap<>(model)));
        return copy;
    }

    private static String key(Random random) {
        return "k%04d".formatted(random.nextInt(3_000));
    }

    private static String value(Random random) {
        var length = switch (random.nextInt(25)) {
            case 0 -> 3_000 + random.nextInt(25_000);
            case 1 -> 1_950 + random.nextInt(100);
            default -> random.nextInt(120);
        };
        var builder = new StringBuilder(length);
        for (var i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return builder.toString();
    }

    private static void batch(Ledger ledger, Map<Keyspace<String, String>, TreeMap<String, String>> models, Random random, int writes) {
        var batch = ledger.newBatch();
        var keyspaces = List.copyOf(models.keySet());
        for (var i = 0; i < writes; i++) {
            var keyspace = keyspaces.get(random.nextInt(keyspaces.size()));
            var model = models.get(keyspace);
            var kind = random.nextInt(30);
            if (kind == 0) {
                var a = key(random);
                var b = key(random);
                var from = a.compareTo(b) <= 0 ? a : b;
                var to = from.equals(a) ? b : a;
                var bounds = random.nextInt(8);
                var range = bounds == 0 ? KeyRange.<String>all() : bounds == 1 ? KeyRange.atLeast(from)
                        : bounds == 2 ? KeyRange.<String>below(to) : KeyRange.between(from, to);
                batch.deleteRange(keyspace, range);
                subMap(model, range.from().orElse(null), range.to().orElse(null)).clear();
            } else if (kind < 8) {
                var key = key(random);
                batch.delete(keyspace, key);
                model.remove(key);
            } else {
                var key = key(random);
                var value = value(random);
                batch.put(keyspace, key, value);
                model.put(key, value);
            }
        }
        ledger.commit(batch);
    }

    private static void transfer(Ledger ledger, Map<Keyspace<String, String>, TreeMap<String, String>> models, Random random) {
        var key = key(random);
        var other = key(random);
        ledger.update(transaction -> {
            var value = transaction.get(NOTES, key).orElse("");
            transaction.put(NOTES, other, value + "!");
            transaction.delete(MESSAGES, key);
        });
        var notes = models.get(NOTES);
        notes.put(other, notes.getOrDefault(key, "") + "!");
        models.get(MESSAGES).remove(key);
    }

    private static NavigableMap<String, String> subMap(TreeMap<String, String> model, String from, String to) {
        if (from != null && to != null) {
            return from.compareTo(to) >= 0 ? new TreeMap<>() : model.subMap(from, true, to, false);
        }
        return from != null ? model.tailMap(from, true) : to != null ? model.headMap(to, false) : model;
    }

    private static void verify(View view, Keyspace<String, String> keyspace, TreeMap<String, String> model, Random random) {
        assertEquals(model.size(), view.size(keyspace), keyspace.name() + " size");
        var all = new ArrayList<Entry<String, String>>();
        view.forEach(keyspace, KeyRange.all(), (key, value) -> all.add(new Entry<>(key, value)));
        assertEquals(entries(model), all, keyspace.name() + " contents");
        for (var i = 0; i < 4; i++) {
            var a = key(random);
            var b = key(random);
            var from = a.compareTo(b) <= 0 ? a : b;
            var to = from.equals(a) ? b : a;
            var limit = 1 + random.nextInt(40);
            var expected = subMap(model, from, to);
            assertEquals(entries(expected).stream().limit(limit).toList(), view.range(keyspace, KeyRange.between(from, to), limit));
            assertEquals(entries(expected.descendingMap()).stream().limit(limit).toList(),
                    view.range(keyspace, KeyRange.between(from, to).reversed(), limit));
            assertEquals(entries(subMap(model, from, null)).stream().limit(limit).toList(), view.range(keyspace, KeyRange.atLeast(from), limit));
        }
        for (var i = 0; i < 30; i++) {
            var key = key(random);
            assertEquals(Optional.ofNullable(model.get(key)), view.get(keyspace, key), keyspace.name() + " " + key);
            var encoded = view.readEncoded(keyspace, key, segment -> new String(segment.toArray(ValueLayout.JAVA_BYTE), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals(Optional.ofNullable(model.get(key)), encoded);
        }
    }

    private static List<Entry<String, String>> entries(Map<String, String> map) {
        var entries = new ArrayList<Entry<String, String>>(map.size());
        map.forEach((key, value) -> entries.add(new Entry<>(key, value)));
        return entries;
    }

    @Nested
    @DisplayName("memory and space")
    class MemoryAndSpace {
        @Test
        @DisplayName("a checkpoint takes the paged entries out of memory and keeps the resident ones")
        void releasesMemory(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES, CHATS)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 5_000; i++) {
                    batch.put(MESSAGES, "m%05d".formatted(i), "message " + i);
                    batch.put(CHATS, "c%05d".formatted(i), "chat " + i);
                }
                ledger.commit(batch);
                var live = (LiveLedger) ledger;
                assertEquals(5_000, live.state(MESSAGES).entries.size());
                ledger.checkpoint();
                // The next commit's maintenance and the checkpoint's pruning drop what the tree holds
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "x"));
                assertTrue(live.state(MESSAGES).entries.size() <= 1, () -> live.state(MESSAGES).entries.size() + " entries in memory");
                assertEquals(5_000, live.state(CHATS).entries.size());
                assertEquals(Optional.of("message 4321"), ledger.get(MESSAGES, "m04321"));
                assertEquals(5_001, ledger.size(MESSAGES));
            }
        }

        @Test
        @DisplayName("overwriting the same keys reuses pages instead of growing the file")
        void reusesPages(@TempDir Path directory) throws IOException {
            try (var ledger = open(directory, MESSAGES)) {
                for (var round = 0; round < 60; round++) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 2_000; i++) {
                        batch.put(MESSAGES, "m%05d".formatted(i), "round " + round + " " + "x".repeat(i % 200));
                    }
                    if (round % 5 == 0) {
                        batch.put(MESSAGES, "large", "L".repeat(50_000 + round));
                    }
                    ledger.commit(batch);
                    ledger.checkpoint();
                }
            }
            // About 2,000 entries of up to 230 bytes and one 50 KB value: a few hundred KiB live
            var size = Files.size(directory.resolve(PageFile.FILE));
            assertTrue(size < 4L << 20, () -> "the data file grew to " + size + " bytes");
            verifyPages(directory, true);
        }

        @Test
        @DisplayName("a snapshot keeps the pages of its trees through later checkpoints")
        void snapshotPinsPages(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 3_000; i++) {
                    batch.put(MESSAGES, "m%05d".formatted(i), "first " + i);
                }
                ledger.commit(batch);
                ledger.checkpoint();
                try (var snapshot = ledger.snapshot()) {
                    for (var round = 0; round < 10; round++) {
                        var overwrite = ledger.newBatch();
                        for (var i = 0; i < 3_000; i++) {
                            overwrite.put(MESSAGES, "m%05d".formatted(i), "round " + round + " " + i);
                        }
                        ledger.commit(overwrite);
                        ledger.checkpoint();
                    }
                    assertEquals(Optional.of("first 1234"), snapshot.get(MESSAGES, "m01234"));
                    var seen = new AtomicLong();
                    snapshot.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                        assertTrue(value.startsWith("first "), value);
                        seen.incrementAndGet();
                        return true;
                    });
                    assertEquals(3_000, seen.get());
                }
                assertEquals(Optional.of("round 9 1234"), ledger.get(MESSAGES, "m01234"));
            }
        }
    }

    @Nested
    @DisplayName("automatic checkpoints")
    class Automatic {
        @Test
        @DisplayName("the store checkpoints on its own as the log grows, and reopens from its checkpoints")
        void triggered(@TempDir Path directory) throws Exception {
            var value = "v".repeat(1_000);
            try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                    .checkpointEvery(1 << 20).build()) {
                for (var i = 0; i < 4_000; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value + i));
                }
                var live = (LiveLedger) ledger;
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (live.tree() == null && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                    Thread.sleep(10);
                }
                assertTrue(live.tree() != null, "no checkpoint started on its own");
                ledger.awaitDurable();
                crash(ledger);
            }
            try (var ledger = open(directory, MESSAGES)) {
                assertEquals(4_000, ledger.size(MESSAGES));
                assertEquals(Optional.of(value + 3999), ledger.get(MESSAGES, "m03999"));
            }
        }
    }

    @Nested
    @DisplayName("work on the store's own threads")
    class BackgroundWork {
        private static boolean dataFile(Path path) {
            return path.getFileName() != null && path.getFileName().toString().equals(PageFile.FILE);
        }

        @Test
        @DisplayName("a checkpoint that fails on its own thread is reported, and the store goes on")
        void reportsFailures() throws Exception {
            var disk = CrashFileSystem.create();
            var failing = new AtomicBoolean(true);
            disk.intercept((path, kind) -> {
                if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                    throw new IOException("No space left on device");
                }
            });
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var directory = disk.getPath("/store");
            var value = "v".repeat(10_000);
            try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                    .checkpointEvery(1 << 20).onBackgroundFailure(failures::add).build()) {
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                var written = 0;
                while (failures.isEmpty() && System.nanoTime() < deadline) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(written++), value));
                    if (written > 300) {
                        Thread.sleep(10);
                    }
                }
                assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures);
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "failure")).awaitDurable();
                failing.set(false);
                ledger.checkpoint();
                assertEquals(written + 1, ledger.size(MESSAGES));
            }
            try (var ledger = open(directory, MESSAGES)) {
                assertEquals(Optional.of("failure"), ledger.get(MESSAGES, "after"));
                assertEquals(Optional.of(value), ledger.get(MESSAGES, "m00000"));
            }
        }

        @Test
        @DisplayName("commits wait for checkpoints slower than they are, so the log the trees lack stays bounded")
        void backpressure() throws Exception {
            var disk = CrashFileSystem.create();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            var every = 1 << 20;
            var value = "v".repeat(64 << 10);
            try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY))
                    .keyspace(MESSAGES).checkpointEvery(every).build()) {
                var checkpointer = ((LiveLedger) ledger).checkpointer();
                var largest = 0L;
                for (var i = 0; i < 200; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value));
                    largest = Math.max(largest, checkpointer.backlog());
                }
                // Three intervals, and the commit that found the log just under them
                var bound = 3L * every + 2L * value.length();
                var observed = largest;
                assertTrue(observed <= bound, () -> observed + " bytes of log the trees lacked, past " + bound);
                assertEquals(200, ledger.size(MESSAGES));
            }
        }
    }

    @Nested
    @DisplayName("key spaces left out")
    class LeftOut {
        @Test
        @DisplayName("survive the checkpoints of opens without them, from the trees and from the log")
        void hidden(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory, MESSAGES, NOTES)) {
                ledger.commit(ledger.newBatch().put(NOTES, "checkpointed", "1"));
            }
            var crashed = open(directory, MESSAGES, NOTES);
            crashed.commit(crashed.newBatch().put(NOTES, "logged", "2")).awaitDurable();
            crash(crashed);
            try (var ledger = open(directory, MESSAGES)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "m", "x"));
                ledger.checkpoint();
            }
            try (var ledger = open(directory, MESSAGES, NOTES)) {
                assertEquals(Optional.of("1"), ledger.get(NOTES, "checkpointed"));
                assertEquals(Optional.of("2"), ledger.get(NOTES, "logged"));
                assertEquals(2, ledger.size(NOTES));
                assertEquals(Optional.of("x"), ledger.get(MESSAGES, "m"));
            }
        }
    }

    @Nested
    @DisplayName("damage")
    class Damage {
        @Test
        @DisplayName("a damaged page of a tree fails the read that reaches it")
        void damagedPage(@TempDir Path directory) throws IOException {
            try (var ledger = open(directory, MESSAGES)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 2_000; i++) {
                    batch.put(MESSAGES, "m%05d".formatted(i), "value " + i);
                }
                ledger.commit(batch);
            }
            var file = directory.resolve(PageFile.FILE);
            var bytes = Files.readAllBytes(file);
            // A page in the middle of the units, past the superblocks
            var page = (bytes.length / PageFormat.SIZE) / 2;
            bytes[page * PageFormat.SIZE + PageFormat.HEADER + 100] ^= 1;
            Files.write(file, bytes);
            try (var ledger = open(directory, MESSAGES)) {
                assertThrows(LedgerCorruptedException.class, () -> ledger.forEach(MESSAGES, KeyRange.all(), (_, _) -> true));
            } catch (LedgerCorruptedException damagedOnOpen) {
                // The damaged unit was the catalog or the free list, which opening reads
            }
        }

        @Test
        @DisplayName("a torn superblock leaves the one before it, and the log replays the rest")
        void tornSuperblock(@TempDir Path directory) throws Exception {
            var ledger = open(directory, MESSAGES);
            try {
                ledger.commit(ledger.newBatch().put(MESSAGES, "a", "1"));
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(MESSAGES, "b", "2"));
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(MESSAGES, "c", "3")).awaitDurable();
            } finally {
                crash(ledger);
            }
            // Tear the newest superblock, as a crash during its write would
            var file = directory.resolve(PageFile.FILE);
            var bytes = Files.readAllBytes(file);
            var newest = 0;
            bytes[newest * PageFormat.SIZE + PageFormat.HEADER + 3] ^= 1;
            Files.write(file, bytes);
            // A checkpoint deletes log segments only once its superblock is durable, so the log still covers the older one
            try (var reopened = open(directory, MESSAGES)) {
                assertEquals(List.of("a", "b", "c"), reopened.range(MESSAGES, KeyRange.all(), 10).stream().map(Entry::key).toList());
            }
        }
    }

    @Nested
    @DisplayName("concurrency")
    class Concurrency {
        @Test
        @DisplayName("readers see consistent totals while checkpoints run under the writers")
        void transfersWithCheckpoints(@TempDir Path directory) throws Exception {
            var accounts = Keyspace.paged("accounts", KeyCodec.int64(), LONGS);
            var count = 64;
            var expected = count * 1_000L;
            try (var ledger = open(directory, accounts)) {
                var initial = ledger.newBatch();
                for (var account = 0; account < count; account++) {
                    initial.put(accounts, (long) account, 1_000L);
                }
                ledger.commit(initial);
                var stop = new AtomicBoolean();
                var failures = new ConcurrentLinkedQueue<String>();
                var transfers = new AtomicLong();
                var checkpoints = new AtomicLong();
                try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().daemon().factory())) {
                    for (var writer = 0; writer < 4; writer++) {
                        executor.execute(() -> {
                            var random = ThreadLocalRandom.current();
                            while (!stop.get()) {
                                var from = (long) random.nextInt(count);
                                var to = (long) random.nextInt(count);
                                var amount = (long) random.nextInt(1, 10);
                                ledger.update(transaction -> {
                                    transaction.put(accounts, from, transaction.get(accounts, from).orElseThrow() - amount);
                                    transaction.put(accounts, to, transaction.get(accounts, to).orElseThrow() + amount);
                                });
                                transfers.incrementAndGet();
                            }
                        });
                    }
                    for (var reader = 0; reader < 4; reader++) {
                        var useSnapshot = reader % 2 == 0;
                        executor.execute(() -> {
                            while (!stop.get()) {
                                var total = new long[1];
                                if (useSnapshot) {
                                    try (var snapshot = ledger.snapshot()) {
                                        snapshot.forEach(accounts, KeyRange.all(), (_, value) -> {
                                            total[0] += value;
                                            return true;
                                        });
                                    }
                                } else {
                                    ledger.forEach(accounts, KeyRange.all(), (_, value) -> {
                                        total[0] += value;
                                        return true;
                                    });
                                }
                                if (total[0] != expected) {
                                    failures.add("total " + total[0]);
                                }
                            }
                        });
                    }
                    executor.execute(() -> {
                        while (!stop.get()) {
                            ledger.checkpoint();
                            checkpoints.incrementAndGet();
                        }
                    });
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                    stop.set(true);
                }
                assertTrue(failures.isEmpty(), () -> failures.size() + " inconsistent reads, first: " + failures.peek());
                assertTrue(transfers.get() > 100 && checkpoints.get() > 5, () -> transfers + " transfers, " + checkpoints + " checkpoints");
            }
            try (var ledger = open(directory, accounts)) {
                var total = new long[1];
                ledger.forEach(accounts, KeyRange.all(), (_, value) -> {
                    total[0] += value;
                    return true;
                });
                assertEquals(expected, total[0]);
                assertEquals(count, ledger.size(accounts));
            }
            verifyPages(directory, true);
        }
    }

    /// Walks the data file from its newest superblock and checks that every page is used once or free.
    private static void verifyPages(Path directory, boolean encrypted) throws IOException {
        var header = StoreHeader.read(directory);
        var keys = header.open(encrypted ? EncryptionSpec.key(KEY) : EncryptionSpec.NONE);
        try (var pages = PageFile.open(directory, new PageCipher(keys.pageKey()), 1 << 20, new HeapPool(1 << 20))) {
            var superblock = pages.superblock();
            if (superblock == null) {
                return;
            }
            var used = new BitSet();
            mark(used, superblock.catalog());
            mark(used, superblock.freeList());
            var catalog = Catalog.decode(pages.read(superblock.catalog(), PageFormat.CATALOG, false));
            for (var entry : catalog.entries()) {
                if (entry.root() != null) {
                    walk(pages, entry.root().ref(), used);
                }
            }
            var free = Catalog.decodeFreeList(pages.read(superblock.freeList(), PageFormat.FREE_LIST, false), superblock.pageCount());
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < superblock.pageCount(); page++) {
                assertTrue(used.get(page) != free.get(page), "page " + page + (used.get(page) ? " is used and free" : " is neither used nor free"));
            }
            assertFalse(used.get(superblock.pageCount()), "a unit is past the end of the file");
        }
    }

    private static void mark(BitSet used, PageRef ref) {
        for (var page = ref.page(); page < ref.page() + ref.pages(); page++) {
            assertFalse(used.get(page), "page " + page + " is used twice");
            used.set(page);
        }
    }

    private static void walk(PageFile pages, PageRef ref, BitSet used) {
        mark(used, ref);
        var unit = pages.read(ref, PageFile.NODE, false);
        for (var i = 0; i < TreeNodes.count(unit); i++) {
            if (TreeNodes.isLeaf(unit)) {
                var entry = TreeNodes.entry(unit, i);
                if (TreeNodes.isBlob(unit, entry)) {
                    mark(used, TreeNodes.blobRef(unit, entry));
                }
            } else {
                walk(pages, TreeNodes.childRef(unit, i), used);
            }
        }
    }
}

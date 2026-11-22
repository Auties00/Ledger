package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The copy-on-write tree on its own, against a sorted map: random rounds of writes and range removals, with every
/// freed page reused at once; and the first tree of a key space written straight from a run, against the tree the
/// states of the run's entries write.
class TreeUpdateTest {
    private static final java.util.Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("random rounds leave the tree equal to the model, with every page reachable or free")
    void randomRounds(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 7 : 11);
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
        var pool = new HeapPool(1 << 20);
        try (var pages = PageFile.open(directory, cipher, 1 << 20, pool)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var keys = new ArrayList<byte[]>();
            for (var i = 0; i < 30_000; i++) {
                var key = new byte[1 + random.nextInt(40)];
                random.nextBytes(key);
                keys.add(key);
            }
            TreeRoot root = null;
            var deltas = DeltaTable.EMPTY;
            var deepest = 0;
            var records = 0;
            for (var round = 0; round < 40; round++) {
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                var writes = round < 3 ? 12_000 : random.nextInt(600);
                for (var i = 0; i < writes; i++) {
                    var key = keys.get(random.nextInt(keys.size()));
                    states.put(key, random.nextInt(5) == 0 ? null : value(random));
                }
                var ranges = new ArrayList<TreeUpdate.Range>();
                if (round > 3 && random.nextInt(3) == 0) {
                    var a = keys.get(random.nextInt(keys.size()));
                    var b = keys.get(random.nextInt(keys.size()));
                    var low = UNSIGNED.compare(a, b) <= 0 ? a : b;
                    var high = low == a ? b : a;
                    var open = random.nextInt(10);
                    var from = open == 0 ? null : low;
                    var to = open == 1 ? null : high;
                    if (from == null || to == null || UNSIGNED.compare(from, to) < 0) {
                        ranges.add(new TreeUpdate.Range(from == null ? null : segment(from), to == null ? null : segment(to)));
                        var removed = from == null && to == null ? model
                                : from == null ? model.headMap(to, false)
                                : to == null ? model.tailMap(from, true)
                                : model.subMap(from, true, to, false);
                        removed.clear();
                    }
                }
                var ops = new ArrayList<TreeUpdate.Op>();
                for (Map.Entry<byte[], byte[]> state : states.entrySet()) {
                    ops.add(new TreeUpdate.Op(segment(state.getKey()), state.getValue() == null ? null : segment(state.getValue())));
                    if (state.getValue() == null) {
                        model.remove(state.getKey());
                    } else {
                        model.put(state.getKey(), state.getValue());
                    }
                }
                var writer = pages.writer(random.nextLong());
                var update = new TreeUpdate(pages, writer, space);
                root = update.apply(root, ops, ranges);
                deltas = update.finishDeltas(deltas);
                writer.flush();
                for (var unit : update.freed()) {
                    space.release(unit.page(), unit.pages());
                }
                if (round % 7 == 0) {
                    // Units read back from the file, not from the cache
                    pages.cache().clearForTests();
                }
                verify(pages, root, model, random, keys);
                verifyNodes(pages, root);
                records = Math.max(records, verifyPages(pages, space, root, deltas));
                if (root != null) {
                    deepest = Math.max(deepest, TreeNodes.level(pages.read(root.ref(), PageFile.NODE, true)));
                }
            }
            assertTrue(deepest >= 2, () -> "the tree never grew past level " + 1);
            assertTrue(records > 0, "no leaf was ever written as a delta record");
        }
    }

    @ParameterizedTest(name = "cache of {0} bytes")
    @ValueSource(ints = {1 << 20, 64 << 20})
    @DisplayName("a long scan keeps the leaves it reads while the cache has room for them, and evicts no other")
    void scansFillRoomOnly(int cacheBytes, @TempDir Path directory) throws Exception {
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        try (var pages = PageFile.open(directory, cipher, cacheBytes, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 20_000; i++) {
                ops.add(new TreeUpdate.Op(segment("k%06d".formatted(i).getBytes()), segment(new byte[150])));
            }
            var writer = pages.writer(1);
            var root = new TreeUpdate(pages, writer, space).apply(null, ops, List.of());
            writer.flush();
            pages.cache().clearForTests();
            // A hot leaf, read before the scan
            var hot = segment("k000010".getBytes());
            assertTrue(TreeSearch.contains(pages, root, hot));
            var cursor = new TreeCursor(pages, root, null, null, false);
            var scanned = 0;
            while (cursor.next()) {
                cursor.value();
                scanned++;
            }
            assertEquals(20_000, scanned);
            assertEquals(TreeSearch.FOUND, TreeSearch.lookupCached(pages, root, hot), "the scan evicted the hot leaf");
            var late = TreeSearch.lookupCached(pages, root, segment("k019990".getBytes()));
            assertEquals(cacheBytes > 16 << 20 ? TreeSearch.FOUND : TreeSearch.UNKNOWN, late);
        }
    }

    @Test
    @DisplayName("a flush on a worker of a fork-join pool seals on no other worker, whose tasks may be waiting for the lock the flush holds")
    void sealsOffTheWorkers(@TempDir Path directory) throws Exception {
        var helpers = ConcurrentHashMap.<Thread>newKeySet();
        var helped = new CountDownLatch(1);
        var waited = new AtomicBoolean();
        var flusher = new AtomicReference<Thread>();
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES")) {
            @Override
            void seal(byte[] plain, int pages, byte[] out, int offset) {
                if (Thread.currentThread() == flusher.get()) {
                    // The first seal of the flushing worker waits for another thread to seal, so a seal forked to its
                    // deque is there for the idle workers to take
                    if (waited.compareAndSet(false, true)) {
                        try {
                            helped.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException exception) {
                            throw new IllegalStateException(exception);
                        }
                    }
                } else {
                    helpers.add(Thread.currentThread());
                    helped.countDown();
                }
                super.seal(plain, pages, out, offset);
            }
        };
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            workers.submit(() -> {
                flusher.set(Thread.currentThread());
                // Past the units from which a flush seals on every processor
                for (var i = 0; i < 256; i++) {
                    writer.write(PageFormat.VALUE, 0, space.allocate(1), 1, new byte[PageFormat.HEADER + 100], 100, false);
                }
                writer.flush();
                return null;
            }).get();
        }
        if (Runtime.getRuntime().availableProcessors() > 1) {
            assertFalse(helpers.isEmpty(), "no other thread sealed");
        }
        for (var helper : helpers) {
            assertTrue(helper.isVirtual(), "sealed on " + helper);
        }
    }

    @Test
    @DisplayName("subtrees updated in parallel at two levels of a tree, on as few workers as a machine of four processors has, leave the tree equal to the model")
    void parallelUpdatesOnFewWorkers(@TempDir Path directory) throws Exception {
        var random = new Random(23);
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            try {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var keys = new ArrayList<byte[]>();
                for (var i = 0; i < 200_000; i++) {
                    keys.add("k%07d".formatted(i).getBytes());
                }
                TreeRoot root = null;
                var deltas = DeltaTable.EMPTY;
                for (var round = 0; round < 12; round++) {
                    var ops = new ArrayList<TreeUpdate.Op>();
                    for (var key : keys) {
                        var value = new byte[100];
                        random.nextBytes(value);
                        model.put(key, value);
                        ops.add(new TreeUpdate.Op(segment(key), segment(value)));
                    }
                    var writer = pages.writer(random.nextLong());
                    var update = new TreeUpdate(pages, writer, space);
                    var before = root;
                    // A pool of its own, so a machine of many processors has only three workers to run into each other
                    root = assertTimeoutPreemptively(Duration.ofMinutes(2),
                            () -> workers.submit(() -> update.apply(before, ops, List.of())).get());
                    deltas = update.finishDeltas(deltas);
                    writer.flush();
                    for (var unit : update.freed()) {
                        space.release(unit.page(), unit.pages());
                    }
                }
                verify(pages, root, model, random, keys);
            } finally {
                // A close would wait for workers a failure of the timeout left stuck
                workers.shutdownNow();
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a tree written from a replay's run alone, of values in chunks and in the log, inline, at the inline bound and in units of many pages, empty values, removals and keys longer than a slab's chunk takes, is the tree the states of the run's entries write, unit for unit, past the bytes a writer queues before it flushes, and records the same settles")
    void runTreeLayout(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 53 : 59);
        var writes = runWrites(random, 30_000, false);
        var queued = 0L;
        for (var write : writes) {
            if (write.kind() != RunKind.REMOVED && !TreeNodes.LeafEntry.fitsInline(write.key().length, write.value().length)) {
                queued += (long) PageFormat.pagesFor(write.value().length) * PageFormat.SIZE;
            }
        }
        // More than the 8 MiB a writer queues before it flushes, in value units alone
        assertTrue(queued > 8 << 20, "the value units take " + queued + " bytes");
        try (var log = new Log(directory.resolve("1.log"))) {
            var streamedRun = replayRun(writes, log);
            var mergedRun = replayRun(writes, log);
            log.write();
            var streamed = layOut(directory.resolve("streamed"), encrypted, streamedRun, true, null);
            var merged = layOut(directory.resolve("merged"), encrypted, mergedRun, false, null);
            assertLaidAlike(merged, streamed);
            for (var entry = 0; entry < streamedRun.size(); entry++) {
                assertEquals(mergedRun.settledForTests(entry), streamedRun.settledForTests(entry), "the settles differ");
            }
            for (var position = 0; position < streamedRun.size(); position++) {
                assertEquals(TreeSearch.ABSENT, streamedRun.settledForTests(streamedRun.entryAt(position)), "an entry is not settled");
            }
            assertEquals(mergedRun.correction(), streamedRun.correction());
            verifyLaid(directory.resolve("streamed"), encrypted, streamed, writes, random);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a tree written from a bulk commit's run alone, of values on its homes, is the tree the states of the run's entries write, unit for unit, and records no settle")
    void committedRunTreeLayout(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 61 : 67);
        var writes = runWrites(random, 30_000, true);
        try (var log = new Log(directory.resolve("1.log"))) {
            var streamedRun = committedRun(writes, log, null);
            var mergedRun = committedRun(writes, log, null);
            var streamed = layOut(directory.resolve("streamed"), encrypted, streamedRun, true, null);
            var merged = layOut(directory.resolve("merged"), encrypted, mergedRun, false, null);
            assertLaidAlike(merged, streamed);
            for (var position = 0; position < streamedRun.size(); position++) {
                assertEquals(TreeSearch.UNKNOWN, streamedRun.settledForTests(streamedRun.entryAt(position)), "a counted run was settled");
            }
            verifyLaid(directory.resolve("streamed"), encrypted, streamed, writes, random);
        }
    }

    @Test
    @DisplayName("values a spill moves off the homes of a bulk commit's run to the log and to chunks between the two passes of its tree are written where the states of the run's entries put them, and read back")
    void spillBetweenRunPasses(@TempDir Path directory) throws Exception {
        var random = new Random(71);
        var writes = runWrites(random, 20_000, true);
        try (var log = new Log(directory.resolve("1.log"))) {
            var homes = new ArrayList<HomeValues>();
            var streamedRun = committedRun(writes, log, homes);
            var mergedRun = committedRun(writes, log, null);
            log.write();
            var moved = new int[2];
            Consumer<int[]> spill = _ -> {
                var sequence = 1L;
                for (var home : homes) {
                    var record = log.queue.record(sequence++, home.entries().size());
                    record.locate(log.view, log.places.get(home.array()), 0, 0, false);
                    var count = 0;
                    var entries = new int[home.entries().size()];
                    var offsets = new int[entries.length];
                    var ops = new int[entries.length];
                    // Every other value, so leaves hold values on homes, in chunks and in the log side by side
                    for (var i = 0; i < entries.length; i += 2) {
                        entries[count] = home.entries().get(i);
                        offsets[count] = home.offsets().get(i);
                        ops[count] = i;
                        count++;
                    }
                    var crypto = log.queue.takeCrypto();
                    try {
                        var spilled = streamedRun.spill(crypto, record, entries, offsets, ops, count);
                        moved[0] += spilled[0];
                        moved[1] += spilled[1];
                    } finally {
                        log.queue.giveBack(crypto);
                    }
                }
            };
            var streamed = layOut(directory.resolve("streamed"), true, streamedRun, true, spill);
            assertTrue(moved[0] > 0, "no value moved to the log");
            assertTrue(moved[1] > 0, "no value moved to a chunk");
            var merged = layOut(directory.resolve("merged"), true, mergedRun, false, null);
            assertLaidAlike(merged, streamed);
            verifyLaid(directory.resolve("streamed"), true, streamed, writes, random);
        }
    }

    @ParameterizedTest(name = "let go of before the first pass: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("homes a bulk commit's run let go of for their value frames, before its tree is written or between the two passes, are read back from those frames, and the tree is the one the states of the run's entries write, unit for unit")
    void framedHomesInRunPasses(boolean beforeFirstPass, @TempDir Path directory) throws Exception {
        var random = new Random(beforeFirstPass ? 73 : 79);
        var writes = runWrites(random, 20_000, true);
        try (var log = new Log(directory.resolve("1.log"))) {
            var homes = new ArrayList<HomeValues>();
            var streamedRun = committedRun(writes, log, homes, 1L);
            var mergedRun = committedRun(writes, log, null);
            log.write();
            var frames = new int[homes.size()];
            var offsets = new long[homes.size()];
            var lengths = new int[homes.size()];
            for (var home = 0; home < homes.size(); home++) {
                frames[home] = home;
                offsets[home] = log.places.get(homes.get(home).array());
                lengths[home] = homes.get(home).array().length;
            }
            var dropped = new AtomicBoolean();
            Runnable drop = () -> {
                assertTrue(dropped.compareAndSet(false, true));
                streamedRun.dropHomes(frames, log.view, offsets, lengths, 1L);
                assertEquals(homes.size(), streamedRun.framedHomesForTests(), "homes let go of");
                assertEquals(0, streamedRun.homesForTests(), "homes the run still holds");
            };
            if (beforeFirstPass) {
                drop.run();
            }
            var streamed = layOut(directory.resolve("streamed"), true, streamedRun, true, beforeFirstPass ? null : _ -> drop.run());
            assertTrue(dropped.get(), "the homes were never let go of");
            assertTrue(streamedRun.homesForTests() > 0, "no home read back from its frame");
            var merged = layOut(directory.resolve("merged"), true, mergedRun, false, null);
            assertLaidAlike(merged, streamed);
            verifyLaid(directory.resolve("streamed"), true, streamed, writes, random);
        }
    }

    @Test
    @DisplayName("an entry that takes other bytes than the first pass counted fails the tree before its leaf is written, with every page the update took listed for the checkpoint to give back")
    void runTreeSizeMismatch(@TempDir Path directory) throws Exception {
        var random = new Random(73);
        var writes = runWrites(random, 20_000, false);
        try (var log = new Log(directory.resolve("1.log"));
             var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(1 << 20))) {
            var run = replayRun(writes, log);
            log.write();
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(42);
            var update = new TreeUpdate(pages, writer, space);
            var corrupted = new AtomicInteger(-1);
            update.passHookForTests(sizes -> {
                // A put halfway through, so leaves before it are written and units queued
                var put = writes.size() / 3;
                sizes[put]++;
                corrupted.set(put);
            });
            var failure = assertThrows(IllegalStateException.class, () -> update.applyRun(run));
            assertTrue(failure.getMessage().contains("was counted as"), failure.getMessage());
            assertTrue(corrupted.get() >= 0, "the hook did not run");
            assertTrue(update.allocated().size() > 1, "no page was taken before the failure");
            writer.discard();
            for (var unit : update.allocated()) {
                space.release(unit.page(), unit.pages());
            }
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < space.pageCount(); page++) {
                assertTrue(space.isUnused(page), "page " + page + " was taken and not listed");
            }
        }
    }

    @Test
    @DisplayName("a run that only removes keys leaves a tree that held no key empty, settles every entry, and takes and writes no page")
    void runTreeOfRemovals(@TempDir Path directory) throws Exception {
        var writes = new ArrayList<RunWrite>();
        for (var i = 0; i < 500; i++) {
            writes.add(new RunWrite("k%04d".formatted(i).getBytes(), null, RunKind.REMOVED));
        }
        try (var log = new Log(directory.resolve("1.log"));
             var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(1 << 20))) {
            var run = replayRun(writes, log);
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(3);
            var update = new TreeUpdate(pages, writer, space);
            assertNull(update.applyRun(run));
            writer.flush();
            assertTrue(update.allocated().isEmpty(), "pages taken: " + update.allocated());
            assertEquals(PageFormat.FIRST_UNIT_PAGE, space.pageCount());
            for (var entry = 0; entry < run.size(); entry++) {
                assertEquals(TreeSearch.ABSENT, run.settledForTests(entry));
            }
        }
    }

    /// How a run holds the value of a write.
    private enum RunKind {
        /// In a chunk of the replay.
        CHUNK,
        /// In the log.
        LOGGED,
        /// On a home of a bulk commit.
        HOME,
        /// No value: the write removes its key.
        REMOVED
    }

    private record RunWrite(byte[] key, byte[] value, RunKind kind) {
    }

    /// The entries of a bulk commit's run whose values lie on `array`, and where.
    private record HomeValues(byte[] array, List<Integer> entries, List<Integer> offsets) {
    }

    private record Laid(TreeRoot root, List<PageRef> allocated, int pageCount, byte[] file) {
    }

    /// {@return `count` writes of distinct keys of 1 to 40 bytes, one in a hundred up to [KeyCodec#MAX_LENGTH], in an
    /// order other than the keys': empty values, values at both sides of the bound of an inline value, values of units
    /// of many pages and short ones, on the homes of a bulk commit if `committed`, else in chunks, in the log or
    /// removed}
    private static List<RunWrite> runWrites(Random random, int count, boolean committed) {
        var keys = new TreeMap<byte[], Boolean>(UNSIGNED);
        while (keys.size() < count) {
            var key = new byte[random.nextInt(100) == 0 ? 257 + random.nextInt(com.github.auties00.ledger.KeyCodec.MAX_LENGTH - 256)
                    : 1 + random.nextInt(40)];
            random.nextBytes(key);
            keys.put(key, Boolean.TRUE);
        }
        var writes = new ArrayList<RunWrite>(count);
        for (var key : keys.keySet()) {
            // The longest value a leaf holds inline under this key
            var inline = TreeNodes.MAX_ENTRY - (TreeNodes.SLOT + 2 + key.length + 1 + 2);
            var length = switch (random.nextInt(30)) {
                case 0 -> 2_500 + random.nextInt(24_000);
                case 1 -> inline;
                case 2 -> inline + 1;
                case 3 -> 0;
                case 4 -> SpillQueue.LOG_VALUE_MIN + random.nextInt(300);
                default -> random.nextInt(SpillQueue.LOG_VALUE_MIN + 200);
            };
            var value = new byte[Math.max(0, length)];
            random.nextBytes(value);
            var kind = committed ? RunKind.HOME
                    : random.nextInt(10) == 0 ? RunKind.REMOVED
                    : value.length >= SpillQueue.LOG_VALUE_MIN && random.nextBoolean() ? RunKind.LOGGED : RunKind.CHUNK;
            writes.add(new RunWrite(key, kind == RunKind.REMOVED ? null : value, kind));
        }
        Collections.shuffle(writes, random);
        return writes;
    }

    /// {@return the run of a replay of `writes`, its values in chunks of 64 KiB or placed in `log`}
    private static PackedRun replayRun(List<RunWrite> writes, Log log) {
        var builder = new PackedRun.Builder();
        byte[] chunk = null;
        MemorySegment view = null;
        var fill = 0;
        var stamp = 1L;
        for (var write : writes) {
            var key = write.key();
            var hash = Segments.hash(key, 0, key.length);
            var value = write.value();
            switch (write.kind()) {
                case REMOVED -> builder.delete(key, 0, key.length, hash, stamp);
                case LOGGED -> {
                    var place = log.logValue(value, stamp);
                    builder.putLogged(key, 0, key.length, hash, stamp, log.view, place.frameOffset(), place.bodyIndex(),
                            place.tagHigh(), place.tagLow(), value.length);
                }
                case CHUNK -> {
                    if (chunk == null || fill + value.length > chunk.length) {
                        chunk = new byte[Math.max(64 << 10, value.length)];
                        view = MemorySegment.ofArray(chunk).asReadOnly();
                        fill = 0;
                    }
                    System.arraycopy(value, 0, chunk, fill, value.length);
                    builder.put(key, 0, key.length, hash, stamp, view, fill, value.length);
                    fill += value.length;
                }
                case HOME -> throw new IllegalArgumentException("A replay puts no value on a home");
            }
            stamp++;
        }
        return builder.build(null, null);
    }

    /// {@return the run of a bulk commit of `writes`, its values on homes of 64 KiB each placed in `log` as the body of
    /// a frame of its own; the homes and their entries go to `homes` if it is not `null`}
    private static PackedRun committedRun(List<RunWrite> writes, Log log, List<HomeValues> homes) {
        return committedRun(writes, log, homes, -1);
    }

    /// {@return the run of a bulk commit of `writes`, as [#committedRun(List, Log, List)] makes it, each home the body
    /// of a value frame of the commit at `sequence` that a run can let go of it for, or of a frame of no commit if
    /// `sequence` is negative}
    private static PackedRun committedRun(List<RunWrite> writes, Log log, List<HomeValues> homes, long sequence) {
        var keyBytes = 0L;
        for (var write : writes) {
            keyBytes += write.key().length;
        }
        var builder = PackedRun.Builder.forCommit(writes.size(), keyBytes, null, 0);
        byte[] home = null;
        MemorySegment view = null;
        HomeValues values = null;
        var fill = 0;
        var stamp = 1L;
        for (var write : writes) {
            var key = MemorySegment.ofArray(write.key()).asReadOnly();
            var value = write.value();
            if (home == null || fill + value.length > home.length) {
                home = new byte[Math.max(64 << 10, value.length)];
                view = MemorySegment.ofArray(home).asReadOnly();
                fill = 0;
                if (sequence < 0) {
                    log.reserve(home);
                } else {
                    log.reserveValueFrame(home, sequence);
                }
                values = new HomeValues(home, new ArrayList<>(), new ArrayList<>());
                if (homes != null) {
                    homes.add(values);
                }
            }
            System.arraycopy(value, 0, home, fill, value.length);
            var entry = builder.putHome(key, Segments.hash(key), stamp++, view, fill, value.length);
            values.entries().add(entry);
            values.offsets().add(fill);
            fill += value.length;
        }
        return builder.buildCommitted(null);
    }

    /// {@return the states of the entries of `run` in the order of their keys, as a checkpoint makes them of a run with
    /// no chain and no range removed: the run's entry for a put, a state of no version for a removal}
    private static List<TreeUpdate.Op> runOps(PackedRun run) {
        var ops = new ArrayList<TreeUpdate.Op>(run.size());
        for (var position = 0; position < run.size(); position++) {
            var entry = run.entryAt(position);
            ops.add(run.removes(entry) ? new TreeUpdate.Op(run, entry, null) : TreeUpdate.Op.ofRun(run, entry));
        }
        return ops;
    }

    /// {@return the tree `run` leaves in a new data file in `directory`, written straight from the run if `streamed`,
    /// else from the states of its entries, with `between` run between the passes if it is not `null`}
    private static Laid layOut(Path directory, boolean encrypted, PackedRun run, boolean streamed, Consumer<int[]> between)
            throws Exception {
        Files.createDirectories(directory);
        TreeRoot root;
        List<PageRef> allocated;
        int pageCount;
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(42);
            var update = new TreeUpdate(pages, writer, space);
            if (between != null) {
                update.passHookForTests(between);
            }
            root = streamed ? update.applyRun(run) : update.apply(null, runOps(run), List.of());
            writer.flush();
            allocated = List.copyOf(update.allocated());
            pageCount = space.pageCount();
            assertTrue(update.freed().isEmpty() && update.freedExternal().isEmpty(), "a new tree freed a unit");
            verifyNodes(pages, root);
        }
        return new Laid(root, allocated, pageCount, Files.readAllBytes(directory.resolve(PageFile.FILE)));
    }

    private static void assertLaidAlike(Laid merged, Laid streamed) {
        assertEquals(merged.root(), streamed.root());
        assertEquals(merged.allocated(), streamed.allocated(), "the pages taken, in order");
        assertEquals(merged.pageCount(), streamed.pageCount());
        assertEquals(merged.file().length, streamed.file().length, "the data files differ in length");
        assertEquals(-1, Arrays.mismatch(merged.file(), streamed.file()), "the data files differ");
    }

    /// Checks the tree `laid` wrote in `directory` against the puts of `writes`.
    private static void verifyLaid(Path directory, boolean encrypted, Laid laid, List<RunWrite> writes, Random random)
            throws Exception {
        var model = new TreeMap<byte[], byte[]>(UNSIGNED);
        var keys = new ArrayList<byte[]>(writes.size());
        for (var write : writes) {
            keys.add(write.key());
            if (write.kind() != RunKind.REMOVED) {
                model.put(write.key(), write.value());
            }
        }
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
            verify(pages, laid.root(), model, random, keys);
            verifyNodes(pages, laid.root());
        }
    }

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
    }

    /// A segment of a log without encryption that holds values, or homes of values, as the bodies of frames of their
    /// own, from which runs read them back as a store's do once their values left the heap.
    private static final class Log implements AutoCloseable {
        private final Path path;
        private final SpillQueue queue;
        private final LogSegmentView view;
        private final ByteArrayOutputStream bytes;
        // Where each array's frame starts, by the array
        private final IdentityHashMap<byte[], Long> places;
        // The arrays placed whose bytes are written at write(), in the order they were placed
        private final List<byte[]> reserved;
        // The commit of each array placed as the body of a value frame, which write() gives its header and checksum
        private final IdentityHashMap<byte[], Long> valueFrames;

        Log(Path path) {
            this.path = path;
            this.queue = new SpillQueue(0, false);
            this.view = queue.segment(1, path, null);
            this.bytes = new ByteArrayOutputStream();
            this.places = new IdentityHashMap<>();
            this.reserved = new ArrayList<>();
            this.valueFrames = new IdentityHashMap<>();
        }

        /// Gives `array` a frame of its own, whose body is the bytes the array holds when the log is written.
        void reserve(byte[] array) {
            places.put(array, (long) bytes.size());
            reserved.add(array);
            bytes.writeBytes(new byte[LogWriter.FRAME_HEADER + array.length]);
        }

        /// Gives `array` a value frame of its own of the commit at `sequence`, as a split commit's frames hold the homes
        /// of its run, whose body is the bytes the array holds when the log is written, sealed by a checksum past it.
        void reserveValueFrame(byte[] array, long sequence) {
            places.put(array, (long) bytes.size());
            reserved.add(array);
            valueFrames.put(array, sequence);
            bytes.writeBytes(new byte[LogWriter.FRAME_HEADER + array.length + Integer.BYTES]);
        }

        /// {@return the place of `value`, given a frame of its own at the first call, in the log, as a write stamped
        /// `stamp` holds it}
        LogValue logValue(byte[] value, long stamp) {
            if (!places.containsKey(value)) {
                reserve(value);
            }
            var crypto = queue.takeCrypto();
            try {
                return crypto.logValue(view, places.get(value), 0, stamp, value, 0, value.length);
            } finally {
                queue.giveBack(crypto);
            }
        }

        /// Writes the frames placed to the segment's file.
        void write() throws IOException {
            var file = bytes.toByteArray();
            for (var array : reserved) {
                var start = places.get(array).intValue();
                var at = start + LogWriter.FRAME_HEADER;
                System.arraycopy(array, 0, file, at, array.length);
                var sequence = valueFrames.get(array);
                if (sequence != null) {
                    var header = ByteBuffer.wrap(file);
                    header.putInt(start, LogWriter.MAGIC);
                    header.putInt(start + 4, array.length + Integer.BYTES);
                    header.putLong(start + 8, sequence);
                    file[start + 16] = FrameBody.VALUES;
                    var crc = new CRC32C();
                    crc.update(file, start, LogWriter.FRAME_HEADER + array.length);
                    header.putInt(at + array.length, (int) crc.getValue());
                }
            }
            Files.write(path, file);
        }

        @Override
        public void close() {
            view.retire();
        }
    }

    /// Checks what each branch records of its children against the nodes themselves, and each node against its limits.
    static void verifyNodes(PageFile pages, TreeRoot root) {
        if (root != null) {
            var subtree = verifyNode(pages, root.ref());
            assertEquals(root.count(), subtree.count(), "the root counts wrong");
            assertEquals(root.blobs(), subtree.blobs(), "the root flags its blobs wrong");
        }
    }

    private static Subtree verifyNode(PageFile pages, PageRef ref) {
        var unit = pages.read(ref, PageFile.NODE, false);
        assertTrue(TreeNodes.fill(unit) <= TreeNodes.CAPACITY, "a node passes its capacity");
        assertTrue(PageFormat.fits(1, Bytes.i32(unit, PageFormat.LENGTH)), "a node passes its page");
        assertEquals(PageFormat.INDEXED, unit[PageFormat.FORMAT], "a node has no search index");
        assertTrue(TreeNodes.count(unit) > 0, "a node is empty");
        if (TreeNodes.isLeaf(unit)) {
            var blobs = false;
            byte[] previous = null;
            for (var entry : TreeNodes.leafEntries(unit)) {
                blobs |= entry.blob() != null;
                var key = bytes(entry.key());
                assertTrue(previous == null || UNSIGNED.compare(previous, key) < 0, "a leaf is out of order");
                previous = key;
            }
            return new Subtree(bytes(TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0))), TreeNodes.count(unit), blobs);
        }
        var count = 0L;
        var blobs = false;
        byte[] first = null;
        byte[] previous = null;
        for (var child : TreeNodes.children(unit)) {
            var subtree = verifyNode(pages, child.ref());
            assertArrayEquals(subtree.firstKey(), bytes(child.firstKey()), "a branch names the wrong first key");
            assertEquals(subtree.count(), child.count(), "a branch counts a child wrong");
            assertEquals(subtree.blobs(), child.blobs(), "a branch flags a child's blobs wrong");
            assertTrue(previous == null || UNSIGNED.compare(previous, subtree.firstKey()) < 0, "a branch is out of order");
            first = first == null ? subtree.firstKey() : first;
            previous = subtree.firstKey();
            count += subtree.count();
            blobs |= subtree.blobs();
        }
        return new Subtree(first, count, blobs);
    }

    private record Subtree(byte[] firstKey, long count, boolean blobs) {
    }

    private static byte[] value(Random random) {
        var size = switch (random.nextInt(20)) {
            case 0 -> 2_500 + random.nextInt(20_000);
            case 1 -> 1_900 + random.nextInt(200);
            default -> random.nextInt(300);
        };
        var value = new byte[size];
        random.nextBytes(value);
        return value;
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    static void verify(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, Random random, List<byte[]> keys) {
        assertEquals(model.size(), root == null ? 0 : root.count());
        assertScan(pages, root, model, null, null);
        for (var i = 0; i < 3; i++) {
            var a = keys.get(random.nextInt(keys.size()));
            var b = keys.get(random.nextInt(keys.size()));
            var low = UNSIGNED.compare(a, b) <= 0 ? a : b;
            var high = low == a ? b : a;
            assertScan(pages, root, model, low, high);
            assertScan(pages, root, model, low, null);
            assertScan(pages, root, model, null, high);
            var expected = UNSIGNED.compare(low, high) < 0 ? model.subMap(low, true, high, false).size() : 0;
            assertEquals(expected, TreeSearch.count(pages, root, segment(low), segment(high)));
        }
        for (var i = 0; i < 50; i++) {
            var key = keys.get(random.nextInt(keys.size()));
            var expected = model.get(key);
            if (root == null) {
                assertNull(expected);
                continue;
            }
            assertEquals(expected != null, TreeSearch.contains(pages, root, segment(key)));
            var unit = TreeSearch.leaf(pages, root, key);
            var entry = TreeSearch.entryOf(unit, key);
            if (expected == null) {
                assertEquals(-1, entry);
            } else {
                assertArrayEquals(expected, bytes(TreeSearch.value(pages, unit, entry)));
                if (TreeNodes.isInline(unit, entry)) {
                    assertArrayEquals(expected, TreeNodes.inlineValueBytes(unit, entry));
                }
            }
        }
    }

    private static void assertScan(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] from, byte[] to) {
        for (var descending : new boolean[]{false, true}) {
            var expected = from != null && to != null && UNSIGNED.compare(from, to) >= 0 ? new TreeMap<byte[], byte[]>(UNSIGNED)
                    : from == null && to == null ? model
                    : from == null ? model.headMap(to, false)
                    : to == null ? model.tailMap(from, true)
                    : model.subMap(from, true, to, false);
            var ordered = descending ? new ArrayList<>(expected.descendingMap().entrySet()) : new ArrayList<>(expected.entrySet());
            var cursor = new TreeCursor(pages, root, from, to, descending);
            for (var entry : ordered) {
                assertTrue(cursor.next(), "the cursor ended early");
                assertArrayEquals(entry.getKey(), bytes(cursor.key()));
                assertArrayEquals(entry.getValue(), bytes(cursor.value()));
            }
            assertFalse(cursor.next(), "the cursor went past the range");
        }
    }

    /// {@return the delta records the tree refers to}
    static int verifyPages(PageFile pages, FreeSpace space, TreeRoot root, DeltaTable deltas) {
        var used = new BitSet();
        var records = new TreeMap<Integer, Integer>();
        if (root != null) {
            assertFalse(root.ref().isRecord(), "the root is a delta record");
            CheckpointTest.walk(pages, root.ref(), used, new BitSet(), records);
        }
        CheckpointTest.verifyDeltas(used, records, deltas);
        for (var page = PageFormat.FIRST_UNIT_PAGE; page < space.pageCount(); page++) {
            var free = space.isUnused(page);
            assertTrue(free != used.get(page), "page " + page + (free ? " is free and in use" : " is neither free nor in use"));
        }
        return records.values().stream().mapToInt(Integer::intValue).sum();
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The log's forces on its forcing thread, while the flush seals ahead what follows them: no byte reaches a segment
/// while such a force runs, a failed force stops the log with nothing written after it, an interrupt of the pool's
/// thread or a close during a force loses nothing, and a crash at any point of such a workload recovers.
///
/// Each force of the forcing thread is held a few milliseconds by the interceptor of a [CrashFileSystem], which is
/// what gives the flush time to seal ahead, and the same interceptor records every change of a segment meanwhile.
class SealAheadTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(MESSAGES, CHATS);
    private static final String FORCER = "ledger-log-force";
    private static final long SEGMENT = 64 << 10;
    // The arrays frames are sealed ahead into, and a segment whose frames take several of them
    private static final int AHEAD_ARRAY = 256 << 10;
    private static final long LARGE_SEGMENT = 1 << 20;

    static {
        for (var i = 0; i < KEY.length; i++) {
            KEY[i] = (byte) (i * 7 + 1);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("seals ahead while the forcing thread forces, writes nothing to a segment meanwhile, and recovers every durable commit")
    void noWriteDuringForce(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        var watch = new ForceWatch(2);
        disk.intercept(watch);
        var random = new Random(11);
        var model = new TreeMap<String, String>();
        var ledger = open(disk.getPath("/store"), encryption, SEGMENT, Long.MAX_VALUE);
        try {
            for (var i = 0; i < 300; i++) {
                var batch = ledger.newBatch();
                var writes = 1 + random.nextInt(6);
                for (var j = 0; j < writes; j++) {
                    var key = "k%04d".formatted(random.nextInt(3_000));
                    var value = text(random, random.nextInt(8) == 0 ? 12_000 : 1_500);
                    batch.put(MESSAGES, key, value);
                    model.put(key, value);
                }
                ledger.commit(batch);
            }
            ledger.awaitDurable();
            assertTrue(((LiveLedger) ledger).log().sealedAhead() > 0, "nothing was sealed ahead");
        } finally {
            crash(ledger);
        }
        assertEquals(List.of(), watch.violations);
        // Every commit was durable before the crash, so a loss of power keeps them all
        try (var reopened = open(disk.afterPowerLoss(new Random(3)).getPath("/store"), encryption, SEGMENT, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("frames larger than an array sealed ahead into, than what may be held ahead and than a segment are written whole")
    void largeFrames(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        var watch = new ForceWatch(2);
        disk.intercept(watch);
        var random = new Random(23);
        var model = new TreeMap<String, String>();
        var ledger = open(disk.getPath("/store"), encryption, LARGE_SEGMENT, Long.MAX_VALUE);
        try {
            for (var i = 0; i < 36; i++) {
                // 300 KB spans two arrays sealed ahead into; 1.2 MB is more than the segment and all a flush holds ahead
                var length = switch (i % 3) {
                    case 0 -> 5_000;
                    case 1 -> 300_000;
                    default -> 1_200_000;
                };
                var value = text(random, length, length);
                ledger.commit(ledger.newBatch().put(MESSAGES, "k" + i, value));
                model.put("k" + i, value);
            }
            ledger.awaitDurable();
            assertTrue(((LiveLedger) ledger).log().sealedAhead() > 0, "nothing was sealed ahead");
        } finally {
            crash(ledger);
        }
        assertEquals(List.of(), watch.violations);
        try (var reopened = open(disk.afterPowerLoss(new Random(4)).getPath("/store"), encryption, LARGE_SEGMENT, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("frames of every length around the end of an array sealed ahead into are written whole")
    void lengthsAroundAnArray(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        var watch = new ForceWatch(4);
        disk.intercept(watch);
        var random = new Random(5);
        var model = new TreeMap<String, String>();
        var segment = 4L << 20;
        var ledger = open(disk.getPath("/store"), encryption, segment, Long.MAX_VALUE);
        try {
            var commits = 0;
            for (var length = AHEAD_ARRAY - 420; length < AHEAD_ARRAY + 40; length += 3) {
                var value = text(random, length, length);
                ledger.commit(ledger.newBatch().put(MESSAGES, "k%04d".formatted(commits), value));
                model.put("k%04d".formatted(commits), value);
                if (++commits % 7 == 0) {
                    // Moves every frame after it to another offset of its array
                    var small = text(random, 1, 300);
                    ledger.commit(ledger.newBatch().put(MESSAGES, "s%04d".formatted(commits), small));
                    model.put("s%04d".formatted(commits), small);
                }
            }
            ledger.awaitDurable();
            assertTrue(((LiveLedger) ledger).log().sealedAhead() > 0, "nothing was sealed ahead");
        } finally {
            crash(ledger);
        }
        assertEquals(List.of(), watch.violations);
        try (var reopened = open(disk.afterPowerLoss(new Random(9)).getPath("/store"), encryption, segment, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    @Test
    @DisplayName("a write of what was sealed ahead that fails gives each of its arrays back to the pool once")
    void failedHeldWrite() throws Exception {
        var disk = CrashFileSystem.create();
        var opened = new AtomicReference<LiveLedger>();
        var writes = new AtomicInteger();
        var failed = new AtomicBoolean();
        var watch = new ForceWatch(25) {
            @Override
            public void beforeChange(Path path, ChangeKind kind) throws IOException {
                // The flush's first writes once it sealed ahead are of the arrays it holds, one write an array
                if (kind == ChangeKind.WRITE && isSegment(path) && Thread.currentThread().getName().startsWith("ledger-io-")) {
                    var ledger = opened.get();
                    if (ledger != null && ledger.log().sealedAhead() > 0 && writes.incrementAndGet() == 2) {
                        failed.set(true);
                        throw new IOException("No space left on device");
                    }
                }
                super.beforeChange(path, kind);
            }
        };
        disk.intercept(watch);
        var heap = HeapPool.process();
        // Two trims with no acquire between them empty the pool, so what it hands out below is what this store gave back
        heap.trimIfIdle();
        heap.trimIfIdle();
        var random = new Random(77);
        try (var pool = IoPool.create(1)) {
            // Each frame takes at least three arrays sealed ahead into
            var ledger = (LiveLedger) open(disk.getPath("/store"), Encryption.key(KEY), 8L << 20, Long.MAX_VALUE, pool);
            opened.set(ledger);
            try {
                try {
                    for (var i = 0; i < 200 && !failed.get(); i++) {
                        ledger.commit(ledger.newBatch().put(MESSAGES, "k" + i, text(random, 600_000, 600_000)));
                    }
                } catch (LedgerIoException _) {
                    // The log stopped under this commit
                }
                assertTrue(failed.get(), "no second write of what was sealed ahead came");
                assertThrows(LedgerIoException.class, ledger::awaitDurable);
                // The pool's one thread runs this once the flush, which gives its arrays back last, has ended
                var flushEnded = new CountDownLatch(1);
                ((PlatformIoPool) pool).executor().execute(flushEnded::countDown);
                assertTrue(flushEnded.await(10, TimeUnit.SECONDS), "the flush did not end");
                var free = heap.pooledBytesForTests();
                var arrays = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
                var duplicates = 0;
                for (var i = 0L; i < free / AHEAD_ARRAY; i++) {
                    if (!arrays.add(heap.acquire(AHEAD_ARRAY))) {
                        duplicates++;
                    }
                }
                assertEquals(0, duplicates, "the pool handed out an array it had handed out already");
                assertTrue(free - heap.pooledBytesForTests() >= 2L * AHEAD_ARRAY, "the arrays held were not given back");
            } finally {
                try {
                    crash(ledger);
                } catch (RuntimeException _) {
                    // The log stopped: the close lets go of the files
                }
            }
        }
    }

    @Test
    @DisplayName("a group whose first frame does not fit after the group before starts the next segment with no force of the full one")
    void nextGroupStartsNextSegment() throws Exception {
        var disk = CrashFileSystem.create();
        var forcing = new Semaphore(0);
        var redundant = new AtomicInteger();
        // Whether each segment was written since its last force
        var dirty = new ConcurrentHashMap<String, Boolean>();
        var watch = new ForceWatch(20) {
            @Override
            public void beforeChange(Path path, ChangeKind kind) throws IOException {
                if (isSegment(path)) {
                    if (kind == ChangeKind.WRITE) {
                        dirty.put(path.toString(), true);
                    } else if (kind == ChangeKind.FORCE && dirty.put(path.toString(), false) == Boolean.FALSE) {
                        redundant.incrementAndGet();
                    }
                }
                super.beforeChange(path, kind);
            }

            @Override
            void whileForcing(Path path) throws IOException {
                forcing.release();
                super.whileForcing(path);
            }
        };
        disk.intercept(watch);
        var commits = 24;
        var values = new ArrayList<String>();
        var ledger = open(disk.getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE);
        try {
            var random = new Random(31);
            for (var i = 0; i < commits; i++) {
                // Lands while the group before is forced; two such frames never share a segment
                assertTrue(forcing.tryAcquire(10, TimeUnit.SECONDS), "no force ran on the forcing thread");
                var value = text(random, 40_000, 40_000);
                values.add(value);
                ledger.commit(ledger.newBatch().put(MESSAGES, "k" + i, value));
            }
            ledger.awaitDurable();
        } finally {
            crash(ledger);
        }
        assertEquals(List.of(), watch.violations);
        // A group that went through a rollover of its own would force the full segment once more, with nothing new in it
        assertTrue(redundant.get() <= commits / 4, () -> redundant.get() + " forces of a segment unchanged since its last force");
        try (var reopened = open(disk.afterPowerLoss(new Random(6)).getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE)) {
            for (var i = 0; i < commits; i++) {
                assertEquals(values.get(i), reopened.get(MESSAGES, "k" + i).orElseThrow());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a force that fails on the forcing thread stops the log: nothing more reaches a segment and every waiter fails")
    void forceFailure(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        var failed = new AtomicBoolean();
        var after = new CopyOnWriteArrayList<String>();
        var watch = new ForceWatch(3) {
            @Override
            public void beforeChange(Path path, ChangeKind kind) throws IOException {
                // A preparation and the mark of the segment a close keeps write a file no frame reached
                if (failed.get() && isSegment(path) && (kind == ChangeKind.WRITE || kind == ChangeKind.FORCE)
                        && Thread.currentThread().getName().startsWith("ledger-io-")) {
                    after.add(kind + " of " + path);
                }
                super.beforeChange(path, kind);
            }

            @Override
            void whileForcing(Path path) throws IOException {
                // Held first, so the flush seals ahead what it then has to drop
                super.whileForcing(path);
                if (handedOff.get() == 6 && failed.compareAndSet(false, true)) {
                    throw new IOException("The device failed the force");
                }
            }
        };
        disk.intercept(watch);
        var history = new History();
        var random = new Random(41);
        var ledger = open(disk.getPath("/store"), encryption, SEGMENT, Long.MAX_VALUE);
        try {
            for (var i = 0; i < 2_000 && !failed.get(); i++) {
                try {
                    history.commit(ledger, random, 6, 2_500);
                    if (i % 20 == 19) {
                        ledger.awaitDurable();
                        history.durable = history.visible;
                    }
                } catch (LedgerIoException _) {
                    break;
                }
            }
            assertTrue(failed.get(), "no force failed");
            assertThrows(LedgerIoException.class, ledger::awaitDurable);
            assertThrows(LedgerIoException.class, () -> ledger.commit(ledger.newBatch().put(MESSAGES, "late", "x")).awaitDurable());
        } finally {
            try {
                crash(ledger);
            } catch (RuntimeException _) {
                // The log stopped: the close lets go of the files
            }
        }
        assertEquals(List.of(), watch.violations);
        assertEquals(List.of(), after);
        for (var survivor : List.of(disk.afterProcessCrash(), disk.afterPowerLoss(new Random(7)))) {
            try (var reopened = open(survivor.getPath("/store"), encryption, SEGMENT, Long.MAX_VALUE)) {
                history.check(reopened);
            }
        }
    }

    @Test
    @DisplayName("an interrupt of the pool's thread while the forcing thread forces is taken, and the log goes on")
    void interruptedPool() throws Exception {
        var disk = CrashFileSystem.create();
        var interrupts = new AtomicInteger();
        var watch = new ForceWatch(3) {
            @Override
            void whileForcing(Path path) throws IOException {
                if (interrupts.get() < 20) {
                    for (var thread : Thread.getAllStackTraces().keySet()) {
                        if (thread.getName().startsWith("ledger-io-")) {
                            thread.interrupt();
                            interrupts.incrementAndGet();
                        }
                    }
                }
                super.whileForcing(path);
            }
        };
        disk.intercept(watch);
        var random = new Random(53);
        var model = new TreeMap<String, String>();
        try (var pool = IoPool.create(1)) {
            var ledger = open(disk.getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE, pool);
            try {
                for (var i = 0; i < 300; i++) {
                    var key = "k%04d".formatted(random.nextInt(1_000));
                    var value = text(random, 6_000);
                    ledger.commit(ledger.newBatch().put(MESSAGES, key, value));
                    model.put(key, value);
                }
                ledger.awaitDurable();
            } finally {
                crash(ledger);
            }
        }
        assertTrue(interrupts.get() > 0, "the pool's thread was never interrupted");
        assertEquals(List.of(), watch.violations);
        try (var reopened = open(disk.afterPowerLoss(new Random(8)).getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    @Test
    @DisplayName("a close while the forcing thread forces waits for it, makes everything durable, and ends the forcing thread")
    void closeDuringForce() throws Exception {
        var disk = CrashFileSystem.create();
        var forcing = new CountDownLatch(1);
        var closed = new AtomicBoolean();
        var after = new CopyOnWriteArrayList<String>();
        var watch = new ForceWatch(40) {
            @Override
            public void beforeChange(Path path, ChangeKind kind) throws IOException {
                if (closed.get() && isSegment(path) && (kind == ChangeKind.WRITE || kind == ChangeKind.FORCE)) {
                    after.add(kind + " of " + path);
                }
                super.beforeChange(path, kind);
            }

            @Override
            void whileForcing(Path path) throws IOException {
                forcing.countDown();
                super.whileForcing(path);
            }
        };
        disk.intercept(watch);
        var before = forcers();
        var random = new Random(61);
        var model = new TreeMap<String, String>();
        var ledger = open(disk.getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE);
        var queued = 0;
        // Commits until a force runs on the forcing thread, and some more behind it
        for (var i = 0; i < 10_000 && queued < 40; i++) {
            var key = "k%04d".formatted(random.nextInt(1_000));
            var value = text(random, 5_000);
            ledger.commit(ledger.newBatch().put(MESSAGES, key, value));
            model.put(key, value);
            if (forcing.getCount() == 0) {
                queued++;
            }
        }
        assertEquals(0, forcing.getCount(), "no force ran on the forcing thread");
        ledger.close();
        closed.set(true);
        awaitEnded(before);
        assertEquals(List.of(), watch.violations);
        assertEquals(List.of(), after);
        try (var reopened = open(disk.afterPowerLoss(new Random(9)).getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    @Test
    @DisplayName("an ingest seals ahead by default, solitary durable commits never start the forcing thread, and a close ends it")
    void defaultThreshold() throws Exception {
        var disk = CrashFileSystem.create();
        var watch = new ForceWatch(2);
        disk.intercept(watch);
        var before = forcers();
        try (var ledger = Ledger.builder().directory(disk.getPath("/durable")).encryption(Encryption.key(KEY)).keyspace(MESSAGES).keyspace(CHATS).build()) {
            for (var i = 0; i < 100; i++) {
                ledger.commit(ledger.newBatch().put(CHATS, "k" + i, "v" + i)).awaitDurable();
            }
            assertEquals(0, ((LiveLedger) ledger).log().sealedAhead());
            assertEquals(Set.of(), started(before), "a durable commit with nothing behind it started the forcing thread");
        }
        var random = new Random(71);
        var model = new HashMap<String, String>();
        try (var ledger = ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/ingest")).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(CHATS)).segmentSize(4 << 20).build()) {
            // Batches of a thousand messages, as a history sync stores them
            for (var i = 0; i < 24; i++) {
                var batch = ledger.newBatch();
                for (var j = 0; j < 1_000; j++) {
                    var key = "m%02d-%04d".formatted(i, j);
                    var value = text(random, 2_400);
                    batch.put(MESSAGES, key, value);
                    model.put(key, value);
                }
                ledger.commit(batch);
            }
            ledger.awaitDurable();
            assertTrue(((LiveLedger) ledger).log().sealedAhead() > 0, "the ingest sealed nothing ahead");
            assertEquals(model.size(), ledger.size(MESSAGES));
        }
        awaitEnded(before);
        assertEquals(List.of(), watch.violations);
    }

    @Test
    @DisplayName("writers committing at once beside durable commits and checkpoints lose nothing")
    void concurrentWriters(@TempDir Path directory) throws Exception {
        var model = new ConcurrentHashMap<String, String>();
        var ledger = open(directory, Encryption.key(KEY), 128 << 10, 4_096);
        try {
            try (var executor = Executors.newFixedThreadPool(5)) {
                var tasks = new ArrayList<Future<?>>();
                for (var writer = 0; writer < 4; writer++) {
                    var id = writer;
                    tasks.add(executor.submit(() -> {
                        var random = new Random(id);
                        for (var i = 0; i < 300; i++) {
                            var batch = ledger.newBatch();
                            var puts = new HashMap<String, String>();
                            var writes = 1 + random.nextInt(5);
                            for (var j = 0; j < writes; j++) {
                                // Each writer its own keys, so the last value it wrote wins
                                var key = "w" + id + "-" + random.nextInt(200);
                                var value = text(random, random.nextInt(10) == 0 ? 9_000 : 800);
                                batch.put(MESSAGES, key, value);
                                puts.put(key, value);
                            }
                            ledger.commit(batch);
                            model.putAll(puts);
                        }
                        return null;
                    }));
                }
                tasks.add(executor.submit(() -> {
                    for (var i = 0; i < 100; i++) {
                        ledger.commit(ledger.newBatch().put(CHATS, "durable" + i % 10, "v" + i)).awaitDurable();
                        if (i % 25 == 0) {
                            ledger.checkpoint();
                        }
                    }
                    return null;
                }));
                for (var task : tasks) {
                    task.get();
                }
            }
            ledger.awaitDurable();
        } finally {
            crash(ledger);
        }
        try (var reopened = open(directory, Encryption.key(KEY), 128 << 10, 4_096)) {
            assertEquals(new TreeMap<>(model), entries(reopened, MESSAGES));
            for (var i = 90; i < 100; i++) {
                assertEquals("v" + i, reopened.get(CHATS, "durable" + i % 10).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("groups written through alternate with groups sealed ahead, and a loss of power keeps every durable commit")
    void besideWritesThrough() throws Exception {
        var disk = CrashFileSystem.create();
        var watch = new ForceWatch(1);
        disk.intercept(watch);
        var random = new Random(83);
        var model = new TreeMap<String, String>();
        var ledger = open(disk.getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE, null, WriteThrough.Mode.DSYNC);
        var log = ((LiveLedger) ledger).log();
        try {
            for (var i = 0; i < 2_000 && (log.writtenThrough() == 0 || log.sealedAhead() == 0 || i < 600); i++) {
                var key = "k%04d".formatted(random.nextInt(2_000));
                var value = text(random, 400);
                ledger.commit(ledger.newBatch().put(MESSAGES, key, value)).awaitDurable();
                model.put(key, value);
                if (i % 7 == 6) {
                    // A burst the force of the next group seals ahead, into the next segment when this one has no room
                    for (var j = 0; j < 12; j++) {
                        var burstKey = "b%04d".formatted(random.nextInt(2_000));
                        var burstValue = text(random, 3_000, 9_000);
                        ledger.commit(ledger.newBatch().put(MESSAGES, burstKey, burstValue));
                        model.put(burstKey, burstValue);
                    }
                }
            }
            ledger.awaitDurable();
            assertTrue(log.writtenThrough() > 0, "no group was written through");
            assertTrue(log.sealedAhead() > 0, "nothing was sealed ahead");
        } finally {
            crash(ledger);
        }
        assertEquals(List.of(), watch.violations);
        try (var reopened = open(disk.afterPowerLoss(new Random(10)).getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    @Test
    @DisplayName("the run of small commits sealed ahead into the next segment is replayed as the commits it holds")
    void runsSealedAhead() throws Exception {
        var disk = CrashFileSystem.create();
        var watch = new ForceWatch(5);
        disk.intercept(watch);
        var random = new Random(89);
        var model = new TreeMap<String, String>();
        var ledger = open(disk.getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE);
        try {
            for (var i = 0; i < 4_000; i++) {
                var key = "k%04d".formatted(i);
                var value = text(random, 60, 160);
                ledger.commit(ledger.newBatch().put(MESSAGES, key, value));
                model.put(key, value);
            }
            ledger.awaitDurable();
            assertTrue(((LiveLedger) ledger).log().sealedAhead() > 0, "nothing was sealed ahead");
        } finally {
            crash(ledger);
        }
        assertEquals(List.of(), watch.violations);
        try (var reopened = open(disk.afterPowerLoss(new Random(12)).getPath("/store"), Encryption.key(KEY), SEGMENT, Long.MAX_VALUE)) {
            assertEquals(model, entries(reopened, MESSAGES));
        }
    }

    static Stream<Arguments> crashes() {
        return IntStream.range(0, 48).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0, seed % 3 == 0 ? "process crash" : "power loss"));
    }

    @ParameterizedTest(name = "seed {0}, encrypted: {1}, {2}")
    @MethodSource("crashes")
    @DisplayName("a crash anywhere while groups are sealed ahead recovers a state between the last durable commit and the last visible one")
    void crashes(int seed, boolean encrypted, String failure) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var dry = CrashFileSystem.create();
        dry.intercept(new ForceWatch(1));
        new Workload(seed, encryption).run(dry.getPath("/store"));
        var disk = CrashFileSystem.create();
        var random = new Random(seed * 7919L + 3);
        if (random.nextBoolean()) {
            disk.stopAfter(random.nextLong(dry.changes() + 1));
        } else {
            disk.stopAtForce(random.nextLong(dry.forces() + 1));
        }
        check(seed, encryption, disk, failure.equals("process crash"), random);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {4_000, 4_001, 4_002, 4_003})
    @DisplayName("a crash at changes spread over a workload that seals ahead and writes groups through, on a file system that journals names as NTFS does, recovers")
    void throughCrashes(int seed) throws Exception {
        var encryption = Encryption.key(KEY);
        var dry = CrashFileSystem.createNtfs();
        dry.intercept(new ForceWatch(1));
        new Workload(seed, encryption, WriteThrough.Mode.DSYNC).run(dry.getPath("/store"));
        var stride = Math.max(1, dry.changes() / 60);
        for (var change = 0L; change <= dry.changes(); change += stride) {
            var disk = CrashFileSystem.createNtfs();
            disk.stopAfter(change);
            var workload = new Workload(seed, encryption, WriteThrough.Mode.DSYNC);
            var watch = new ForceWatch(1);
            disk.intercept(watch);
            workload.run(disk.getPath("/store"));
            assertEquals(List.of(), watch.violations);
            var survivor = disk.afterPowerLoss(new Random(seed * 41L + change));
            try (var ledger = open(survivor.getPath("/store"), encryption, SEGMENT, workload.largeValues)) {
                workload.history.check(ledger);
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery").put(CHATS, "after", "recovery")).awaitDurable();
            }
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {2_000, 2_001})
    @DisplayName("a loss of power at any force of a workload sealing ahead recovers")
    void everyForce(int seed) throws Exception {
        var encryption = Encryption.key(KEY);
        var dry = CrashFileSystem.create();
        dry.intercept(new ForceWatch(1));
        new Workload(seed, encryption).run(dry.getPath("/store"));
        for (var force = 0; force <= dry.forces(); force++) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            check(seed, encryption, disk, false, new Random(seed * 31L + force));
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {3_000, 3_002})
    @DisplayName("a crash at changes spread over a workload sealing ahead, whose groups cross segments with units beside them, recovers")
    void spreadChanges(int seed) throws Exception {
        var encryption = Encryption.key(KEY);
        var dry = CrashFileSystem.create();
        dry.intercept(new ForceWatch(1));
        new Workload(seed, encryption).run(dry.getPath("/store"));
        var stride = Math.max(1, dry.changes() / 120);
        for (var change = 0L; change <= dry.changes(); change += stride) {
            var disk = CrashFileSystem.create();
            disk.stopAfter(change);
            check(seed, encryption, disk, false, new Random(seed * 37L + change));
        }
    }

    static Stream<Arguments> heldRunCrashes() {
        return IntStream.range(0, 8).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0));
    }

    @ParameterizedTest(name = "seed {0}, encrypted: {1}")
    @MethodSource("heldRunCrashes")
    @DisplayName("a crash at changes spread over frames that take several arrays sealed ahead into recovers")
    void heldRunCrashes(int seed, boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var dry = CrashFileSystem.create();
        var dryWatch = new ForceWatch(3);
        dry.intercept(dryWatch);
        runHeldRuns(seed, encryption, dry.getPath("/store"), new History());
        assertEquals(List.of(), dryWatch.violations);
        for (var stop = 1; stop <= 12; stop++) {
            var disk = CrashFileSystem.create();
            var watch = new ForceWatch(3);
            disk.intercept(watch);
            disk.stopAfter(dry.changes() * stop / 13);
            var history = new History();
            runHeldRuns(seed, encryption, disk.getPath("/store"), history);
            assertEquals(List.of(), watch.violations);
            var survivor = stop % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(seed * 100L + stop));
            try (var ledger = open(survivor.getPath("/store"), encryption, LARGE_SEGMENT, Long.MAX_VALUE)) {
                history.check(ledger);
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery")).awaitDurable();
            }
        }
    }

    /// Commits, until it ends or the store fails, bursts of frames from tens to hundreds of KiB on a store of segments
    /// that hold a few of them, with a wait for durability now and then.
    private static void runHeldRuns(int seed, Encryption encryption, Path directory, History history) {
        var random = new Random(seed);
        Ledger ledger = null;
        try {
            ledger = open(directory, encryption, LARGE_SEGMENT, Long.MAX_VALUE);
            for (var step = 0; step < 20; step++) {
                var burst = 1 + random.nextInt(4);
                for (var i = 0; i < burst; i++) {
                    var value = random.nextInt(4) == 0 ? text(random, 4_000, 12_000) : text(random, 40_000, 600_000);
                    history.put(ledger, "k%02d".formatted(random.nextInt(16)), value);
                }
                if (random.nextInt(5) == 0) {
                    ledger.awaitDurable();
                    history.durable = history.visible;
                }
            }
            ledger.close();
            history.durable = history.visible;
        } catch (RuntimeException | InterruptedException failure) {
            if (ledger != null) {
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // The file system stopped: closing only lets go of the threads
                }
            }
        }
    }

    /// Runs the workload of `seed` on `disk` until it stops, fails it, and checks what the store recovers.
    private static void check(int seed, Encryption encryption, CrashFileSystem disk, boolean processCrash, Random random) throws Exception {
        var watch = new ForceWatch(1);
        disk.intercept(watch);
        var workload = new Workload(seed, encryption);
        workload.run(disk.getPath("/store"));
        assertEquals(List.of(), watch.violations);
        var survivor = processCrash ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
        try (var ledger = open(survivor.getPath("/store"), encryption, SEGMENT, workload.largeValues)) {
            workload.history.check(ledger);
            ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery").put(CHATS, "after", "recovery")).awaitDurable();
        }
        try (var ledger = open(survivor.getPath("/store"), encryption, SEGMENT, workload.largeValues)) {
            assertEquals("recovery", ledger.get(MESSAGES, "after").orElseThrow());
        }
    }

    private static Ledger open(Path directory, Encryption encryption, long segmentSize, long largeValues) {
        return open(directory, encryption, segmentSize, largeValues, null);
    }

    /// Opens a store that seals ahead at every group of a flush of the pool, whatever is queued behind it.
    private static Ledger open(Path directory, Encryption encryption, long segmentSize, long largeValues, IoPool pool) {
        return open(directory, encryption, segmentSize, largeValues, pool, null);
    }

    /// Opens a store that seals ahead as [#open(Path, Encryption, long, long, IoPool)] does and writes groups through as
    /// `mode` says, or as the file system's default if `null`.
    private static Ledger open(Path directory, Encryption encryption, long segmentSize, long largeValues, IoPool pool, WriteThrough.Mode mode) {
        var options = Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES).keyspace(CHATS).largeValues(largeValues);
        if (pool != null) {
            options.ioPool(pool);
        }
        var builder = ((LedgerBuilder) options).segmentSize(segmentSize).sealAheadFrom(0);
        if (mode != null) {
            builder.writeThrough(mode);
        }
        return builder.build();
    }

    /// Stops `ledger` as a process would once its commits are durable, leaving the log for the next open to replay.
    private static void crash(Ledger ledger) {
        ((LiveLedger) ledger).closeWithoutCheckpoint();
    }

    private static boolean isSegment(Path path) {
        var name = path.getFileName();
        return name != null && name.toString().endsWith(".log");
    }

    private static TreeMap<String, String> entries(Ledger ledger, Keyspace<String, String> keyspace) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    private static String text(Random random, int maxLength) {
        return text(random, 0, maxLength);
    }

    private static String text(Random random, int minLength, int maxLength) {
        var length = minLength + random.nextInt(maxLength - minLength + 1);
        var builder = new StringBuilder(length);
        for (var i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return builder.toString();
    }

    /// {@return the forcing threads alive now}
    private static Set<Thread> forcers() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals(FORCER))
                .collect(Collectors.toSet());
    }

    /// {@return the forcing threads alive now that were not in `before`}
    private static Set<Thread> started(Set<Thread> before) {
        var started = new HashSet<>(forcers());
        started.removeAll(before);
        return started;
    }

    /// Waits until every forcing thread started since `before` has ended.
    private static void awaitEnded(Set<Thread> before) throws InterruptedException {
        for (var thread : started(before)) {
            thread.join(5_000);
            if (thread.isAlive()) {
                fail("a forcing thread outlived its store's close");
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    /// Holds each force of a segment on the forcing thread `hold` milliseconds, so the flush seals ahead meanwhile, and
    /// records every write to a segment while one runs: such a write would reach the file before the force returned.
    private static class ForceWatch implements ChangeInterceptor {
        final AtomicInteger handedOff = new AtomicInteger();
        final List<String> violations = new CopyOnWriteArrayList<>();
        private final AtomicInteger forcing = new AtomicInteger();
        private final long hold;

        ForceWatch(long hold) {
            this.hold = hold;
        }

        @Override
        public void beforeChange(Path path, ChangeKind kind) throws IOException {
            if (!isSegment(path)) {
                return;
            }
            // A segment's preparation writes the file nothing was written to yet, on a thread of its own
            if (kind == ChangeKind.WRITE && forcing.get() > 0 && !Thread.currentThread().getName().equals("ledger-prepare")) {
                violations.add("a write of " + path + " while a force ran");
            }
            if (kind == ChangeKind.FORCE && Thread.currentThread().getName().equals(FORCER)) {
                handedOff.incrementAndGet();
                forcing.incrementAndGet();
                try {
                    whileForcing(path);
                } finally {
                    forcing.decrementAndGet();
                }
            }
        }

        /// Runs on the forcing thread before its force of `path` starts.
        ///
        /// @throws IOException to fail the force
        void whileForcing(Path path) throws IOException {
            sleep(hold);
        }
    }

    /// The state after each commit of a store, the last commit made visible and the last one known durable.
    private static final class History {
        private final List<Map<String, TreeMap<String, String>>> states;
        private final Map<String, TreeMap<String, String>> state;
        int visible;
        int durable;

        History() {
            this.states = new ArrayList<>();
            this.state = new TreeMap<>();
            for (var keyspace : KEYSPACES) {
                state.put(keyspace.name(), new TreeMap<>());
            }
            states.add(copy(state));
        }

        /// Commits a batch of up to `maxWrites` writes, values up to `maxValue` characters and now and then far more,
        /// without waiting for it to be durable.
        void commit(Ledger ledger, Random random, int maxWrites, int maxValue) {
            var next = copy(state);
            var batch = ledger.newBatch();
            var writes = 1 + random.nextInt(maxWrites);
            for (var i = 0; i < writes; i++) {
                var keyspace = KEYSPACES.get(random.nextInt(KEYSPACES.size()));
                var entries = next.get(keyspace.name());
                var key = "k%03d".formatted(random.nextInt(150));
                if (random.nextInt(8) == 0) {
                    batch.delete(keyspace, key);
                    entries.remove(key);
                } else {
                    // A twentieth more than a segment holds, a sixth more than a value kept beside the log
                    var draw = random.nextInt(20);
                    var value = draw == 0 ? text(random, 70_000, 70_000) : draw < 4 ? text(random, 3_000, 9_000) : text(random, maxValue);
                    batch.put(keyspace, key, value);
                    entries.put(key, value);
                }
            }
            ledger.commit(batch);
            state.clear();
            state.putAll(next);
            visible++;
            states.add(copy(state));
        }

        /// Commits one write of `value` to `key` of the paged key space, without waiting for it to be durable.
        void put(Ledger ledger, String key, String value) {
            ledger.commit(ledger.newBatch().put(MESSAGES, key, value));
            state.get(MESSAGES.name()).put(key, value);
            visible++;
            states.add(copy(state));
        }

        /// Checks that `ledger` holds the state after some commit from the last durable one to the last visible one.
        void check(Ledger ledger) {
            var recovered = new TreeMap<String, TreeMap<String, String>>();
            for (var keyspace : KEYSPACES) {
                recovered.put(keyspace.name(), entries(ledger, keyspace));
            }
            for (var commit = durable; commit <= visible; commit++) {
                if (states.get(commit).equals(recovered)) {
                    for (var keyspace : KEYSPACES) {
                        assertEquals(recovered.get(keyspace.name()).size(), ledger.size(keyspace), keyspace.name() + " count");
                    }
                    return;
                }
            }
            fail("The store recovered a state no commit from " + durable + " to " + visible + " left");
        }

        private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
            var copy = new TreeMap<String, TreeMap<String, String>>();
            state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
            return copy;
        }
    }

    /// A workload drawn from a seed: bursts of commits not waited for, which the flush takes in groups of several
    /// frames across segments, now and then a wait for durability, a checkpoint or a close.
    private static final class Workload {
        private final Random random;
        private final Encryption encryption;
        private final WriteThrough.Mode mode;
        final long largeValues;
        final History history;

        Workload(int seed, Encryption encryption) {
            this(seed, encryption, null);
        }

        Workload(int seed, Encryption encryption, WriteThrough.Mode mode) {
            this.random = new Random(seed);
            this.encryption = encryption;
            this.mode = mode;
            // Half the workloads write their values of more than 4 KiB beside the log, whose force runs beside its own
            this.largeValues = seed % 4 < 2 ? 4_096 : Long.MAX_VALUE;
            this.history = new History();
        }

        /// Runs until it ends or the store fails, which a stopped file system makes it do.
        void run(Path directory) {
            Ledger ledger = null;
            try {
                ledger = open(directory, encryption, SEGMENT, largeValues, null, mode);
                for (var step = 0; step < 60; step++) {
                    var choice = random.nextInt(30);
                    if (choice == 0) {
                        ledger.checkpoint();
                        history.durable = history.visible;
                    } else if (choice == 1) {
                        ledger.close();
                        history.durable = history.visible;
                        ledger = open(directory, encryption, SEGMENT, largeValues, null, mode);
                    } else if (choice < 6) {
                        ledger.awaitDurable();
                        history.durable = history.visible;
                    } else {
                        var burst = 1 + random.nextInt(6);
                        for (var i = 0; i < burst; i++) {
                            history.commit(ledger, random, 8, 400);
                        }
                    }
                }
                ledger.close();
                history.durable = history.visible;
            } catch (RuntimeException | InterruptedException failure) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
            }
        }
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Groups written through two at a time: the second lane writes a group beside the flush's, commits are acknowledged
/// in the order of their groups, and recovery drops a group written beside a torn one, and only such a group, so a
/// crash that leaves the later of two groups on the device and the earlier torn still opens.
///
/// The simulated file system makes the writes of a channel opened with `DSYNC` durable as they return, as a write
/// then a force the interceptor sees, so holding that force holds a write made and not yet durable: a loss of power
/// then keeps all, some or none of it. The crash shapes a write leaves are also made by hand, on the state a crash of
/// the process leaves, where both groups are whole: the first frame of a group zeroed, its body damaged, or the
/// padding after it holding other bytes. Commits that must leave their group to the pool's flush, and not write it on
/// their own thread, wait for durability with a timeout.
@Timeout(300)
class OverlappedGroupsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Fifteen groups of a block each to a segment
    private static final long SEGMENT = 64 << 10;
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final int DRAWS = 12;

    static {
        for (var i = 0; i < KEY.length; i++) {
            KEY[i] = (byte) (i * 5 + 3);
        }
    }

    private static Ledger open(Path directory, LogWriter.Overlap overlap) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(WriteThrough.Mode.DSYNC)
                .overlap(overlap)
                .build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static String value(String key) {
        return "v".repeat(500) + key;
    }

    /// Commits `key` durably, on this thread, and records it in `model`.
    private static void commit(Ledger ledger, Map<String, String> model, String key, String value) throws InterruptedException {
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    /// Fills segments, makes them spares, and commits until the log writes a group through into one of them, with a
    /// few groups past it, so the next group is not the first of its segment.
    private static void reachWriteThrough(Ledger ledger, Map<String, String> model) throws InterruptedException {
        var log = log(ledger);
        var commits = 0;
        while (log.currentSegment() < 12) {
            var key = "w%04d".formatted(commits++);
            commit(ledger, model, key, value(key));
        }
        ledger.checkpoint();
        for (var attempt = 0; attempt < 200 && log.writtenThrough() == 0; attempt++) {
            var key = "w%04d".formatted(commits++);
            commit(ledger, model, key, value(key));
        }
        assertTrue(log.writtenThrough() > 0, "no group was written through");
        // Until the last group was written through, past the first of its segment, with room after it for the groups
        // of a test, which then are written through too
        for (var attempt = 0; attempt < 400; attempt++) {
            var through = log.writtenThrough();
            var key = "w%04d".formatted(commits++);
            commit(ledger, model, key, value(key));
            if (log.writtenThrough() > through && log.groupStart() > LogWriter.SEGMENT_HEADER
                    && log.groupEnd() + 8 * LogWriter.ALIGNMENT <= SEGMENT) {
                return;
            }
        }
        throw new AssertionError("no group was written through with room after it");
    }

    private static Map<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    /// Has each force of a log segment, a write through's included, take a millisecond, so the commits that arrive
    /// meanwhile make the next group.
    private static void slowForces(CrashFileSystem disk) {
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        var deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting for " + what);
            Thread.sleep(1);
        }
    }

    /// Holds the force after a write through of a log segment, the `holdMain`-th of the pool's flush and the
    /// `holdLane`-th of the second lane since it was armed, until released.
    private static final class Holds implements ChangeInterceptor {
        volatile boolean armed;
        volatile int holdMain;
        volatile int holdLane;
        final AtomicInteger mainForces = new AtomicInteger();
        final AtomicInteger laneForces = new AtomicInteger();
        final CountDownLatch mainHeld = new CountDownLatch(1);
        final CountDownLatch laneHeld = new CountDownLatch(1);
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch laneRelease = new CountDownLatch(1);

        @Override
        public void beforeChange(Path path, ChangeKind kind) {
            if (!armed || kind != ChangeKind.FORCE || !path.toString().endsWith(".log")) {
                return;
            }
            var name = Thread.currentThread().getName();
            if (name.startsWith("ledger-log-lane")) {
                if (laneForces.incrementAndGet() == holdLane) {
                    hold(laneHeld, laneRelease);
                }
            } else if (name.startsWith("ledger-io-")) {
                if (mainForces.incrementAndGet() == holdMain) {
                    hold(mainHeld, mainRelease);
                }
            }
        }

        private static void hold(CountDownLatch held, CountDownLatch release) {
            held.countDown();
            try {
                assertTrue(release.await(WAIT.toMinutes(), TimeUnit.MINUTES), "a held force was never released");
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        }

        void releaseAll() {
            armed = false;
            mainRelease.countDown();
            laneRelease.countDown();
        }
    }

    /// A commit of `key` on a thread of its own, which waits for durability with a timeout, so the pool's flush writes
    /// its group.
    private static final class Committer {
        final String key;
        final String value;
        final Thread thread;
        volatile boolean durable;
        volatile Throwable failure;

        Committer(Ledger ledger, String key) {
            this.key = key;
            this.value = value(key);
            this.thread = Thread.ofPlatform().name("committer-" + key).start(() -> {
                try {
                    durable = ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable(WAIT);
                } catch (Throwable throwable) {
                    failure = throwable;
                }
            });
        }

        void join() throws InterruptedException {
            thread.join(WAIT.toMillis());
            assertNull(failure, () -> "the commit of " + key + " failed: " + failure);
            assertTrue(durable, () -> "the commit of " + key + " is not durable");
        }
    }

    /// Starts a commit of `key` that leaves its group to the pool's flush, and waits until it waits for durability.
    private static Committer waiting(Ledger ledger, String key) throws InterruptedException {
        var log = log(ledger);
        var before = log.waitingForTests();
        var committer = new Committer(ledger, key);
        await(() -> log.waitingForTests() > before || committer.durable || committer.failure != null, key + " to wait");
        return committer;
    }

    /// The first frame of a group of the current use of a segment, at a block boundary.
    ///
    /// @param offset where it starts
    /// @param overlaps whether its group was written beside the one before it
    /// @param back the distance back to that group, in blocks
    /// @param sealedLength the length of its sealed body
    private record GroupStart(long offset, boolean overlaps, int back, int sealedLength) {
        long named() {
            return offset - (long) back * LogWriter.ALIGNMENT;
        }

        long frameEnd() {
            return offset + LogWriter.FRAME_HEADER + sealedLength;
        }
    }

    /// {@return the groups of the current use of the segment at `path`, in order}
    private static List<GroupStart> groups(Path path) throws IOException {
        var bytes = Files.readAllBytes(path);
        var buffer = ByteBuffer.wrap(bytes);
        var segmentTag = buffer.getLong(LogWriter.SALT_OFFSET);
        var useTag = buffer.getLong(LogWriter.SEGMENT_HEADER + LogWriter.USE_TAG_OFFSET);
        var groups = new ArrayList<GroupStart>();
        for (var offset = LogWriter.SEGMENT_HEADER; offset + LogWriter.FRAME_HEADER <= bytes.length; offset += LogWriter.ALIGNMENT) {
            if (buffer.getInt(offset) == LogWriter.MAGIC && (bytes[offset + 17] & LogWriter.GROUP_START) != 0
                    && buffer.getLong(offset + LogWriter.SEGMENT_TAG_OFFSET) == segmentTag
                    && buffer.getLong(offset + LogWriter.USE_TAG_OFFSET) == useTag) {
                groups.add(new GroupStart(offset, (bytes[offset + 17] & LogWriter.OVERLAPS) != 0,
                        Short.toUnsignedInt(buffer.getShort(offset + 18)), buffer.getInt(offset + 4)));
            }
        }
        return groups;
    }

    private static Path segment(CrashFileSystem disk, long number) {
        return disk.getPath("/store/log/" + LogWriter.segmentName(number));
    }

    private static void writeAt(Path path, long offset, byte[] bytes) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            var buffer = ByteBuffer.wrap(bytes);
            var at = offset;
            while (buffer.hasRemaining()) {
                at += channel.write(buffer, at);
            }
        }
    }

    /// Flips a byte of the sealed body of the frame at `offset`.
    private static void damageBody(Path path, long offset) throws IOException {
        var at = offset + LogWriter.FRAME_HEADER + 7;
        var bytes = Files.readAllBytes(path);
        writeAt(path, at, new byte[]{(byte) (bytes[(int) at] ^ 0x5A)});
    }

    private static void setVersion(Path path, int version) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            var header = ByteBuffer.allocate(LogWriter.HEADER_CRC_OFFSET + Integer.BYTES);
            channel.read(header, 0);
            header.putInt(8, version);
            var crc = new CRC32C();
            crc.update(header.array(), 0, LogWriter.HEADER_CRC_OFFSET);
            header.putInt(LogWriter.HEADER_CRC_OFFSET, (int) crc.getValue());
            channel.write(header.clear(), 0);
        }
    }

    /// {@return the contents of the store a crash of its process leaves in `disk` after `damage` changes the current
    /// segment, opened without groups beside one another}
    private static Map<String, String> reopened(CrashFileSystem disk, long segment, Damage damage) throws IOException {
        var survivor = disk.afterProcessCrash();
        var path = segment(survivor, segment);
        damage.apply(path, groups(path));
        try (var ledger = open(survivor.getPath("/store"), LogWriter.Overlap.OFF)) {
            return contents(ledger);
        }
    }

    @FunctionalInterface
    private interface Damage {
        void apply(Path path, List<GroupStart> groups) throws IOException;
    }

    /// Checks that every state a loss of power now could leave of `disk` holds `acknowledged`, and of `pending` a
    /// prefix in its order.
    private static void survivesPowerLoss(CrashFileSystem disk, Map<String, String> acknowledged, List<String> pending) {
        for (var seed = 0; seed < DRAWS; seed++) {
            var survivor = disk.afterPowerLoss(new Random(seed));
            try (var ledger = open(survivor.getPath("/store"), LogWriter.Overlap.OFF)) {
                var recovered = contents(ledger);
                for (var entry : acknowledged.entrySet()) {
                    assertEquals(Optional.of(entry.getValue()), Optional.ofNullable(recovered.get(entry.getKey())),
                            "seed " + seed + ", key " + entry.getKey());
                }
                var missing = false;
                for (var key : pending) {
                    var present = recovered.containsKey(key);
                    assertTrue(!present || !missing, "seed " + seed + ": " + key + " recovered after a commit before it was not");
                    missing |= !present;
                }
                assertEquals(acknowledged.size() + pending.stream().filter(recovered::containsKey).count(), recovered.size(),
                        "seed " + seed + ": recovered keys never committed");
            }
        }
    }

    @Nested
    @DisplayName("a group written beside a group not durable yet")
    class Beside {
        @Test
        @DisplayName("is written while the group before it is on the device, and acknowledged only once that one is durable")
        void inOrder() throws Exception {
            var disk = CrashFileSystem.create();
            var holds = new Holds();
            disk.intercept(holds);
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON)) {
                var log = log(ledger);
                reachWriteThrough(ledger, model);
                var overlapped = log.overlappedGroups();
                holds.holdMain = 1;
                holds.armed = true;
                var k = waiting(ledger, "k");
                assertTrue(holds.mainHeld.await(1, TimeUnit.MINUTES), "the flush's group never reached the device");
                var next = waiting(ledger, "k+1");
                await(() -> log.laneWritten(true), "the group beside it to be written");
                assertEquals(overlapped + 1, log.overlappedGroups());
                assertTrue(!next.durable && !k.durable, "a group was acknowledged before the one laid out ahead of it was durable");
                holds.releaseAll();
                k.join();
                next.join();
                model.put(k.key, k.value);
                model.put(next.key, next.value);
                assertEquals(model, contents(ledger));
            }
        }

        @Test
        @DisplayName("names the group it went beside, and the groups after both go on one at a time once neither is in flight")
        void namesItsPredecessor() throws Exception {
            var disk = CrashFileSystem.create();
            var holds = new Holds();
            disk.intercept(holds);
            var model = new TreeMap<String, String>();
            long segment;
            try (var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON)) {
                var log = log(ledger);
                reachWriteThrough(ledger, model);
                holds.holdMain = 1;
                holds.armed = true;
                var k = waiting(ledger, "k");
                assertTrue(holds.mainHeld.await(1, TimeUnit.MINUTES));
                var next = waiting(ledger, "k+1");
                await(() -> log.laneWritten(true), "the group beside it to be written");
                holds.releaseAll();
                k.join();
                next.join();
                model.put(k.key, k.value);
                model.put(next.key, next.value);
                commit(ledger, model, "after", value("after"));
                segment = log.currentSegment();
                var groups = groups(segment(disk, segment));
                var beside = groups.stream().filter(GroupStart::overlaps).toList();
                assertEquals(1, beside.size(), () -> "groups beside another: " + beside);
                var named = beside.getFirst().named();
                assertEquals(groups.get(groups.indexOf(beside.getFirst()) - 1).offset(), named,
                        "the group named is not the one right before it");
            }
            try (var ledger = open(disk.getPath("/store"), LogWriter.Overlap.OFF)) {
                assertEquals(model, contents(ledger));
            }
        }

        @Test
        @DisplayName("concurrent committers share writes through on two lanes, and every acknowledged commit survives a loss of power taken while they run")
        void concurrent() throws Exception {
            var disk = CrashFileSystem.create();
            slowForces(disk);
            var warm = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON)) {
                var log = log(ledger);
                reachWriteThrough(ledger, warm);
                var acknowledged = new ConcurrentHashMap<String, String>(warm);
                var committers = 8;
                var perCommitter = 60;
                var done = new AtomicInteger();
                var threads = new ArrayList<Thread>();
                var failures = new ConcurrentHashMap<String, Throwable>();
                for (var committer = 0; committer < committers; committer++) {
                    var name = "c" + committer;
                    threads.add(Thread.ofVirtual().start(() -> {
                        try {
                            for (var i = 0; i < perCommitter; i++) {
                                var key = name + "-%03d".formatted(i);
                                var value = "x".repeat(i * 7) + key;
                                ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                                acknowledged.put(key, value);
                            }
                        } catch (Throwable throwable) {
                            failures.put(name, throwable);
                        } finally {
                            done.incrementAndGet();
                        }
                    }));
                }
                // Losses of power while the committers run: each keeps what was acknowledged before it
                var snapshots = new ArrayList<CrashFileSystem>();
                var expected = new ArrayList<Map<String, String>>();
                while (done.get() < committers && snapshots.size() < 8) {
                    Thread.sleep(20);
                    expected.add(Map.copyOf(acknowledged));
                    snapshots.add(disk.afterPowerLoss(new Random(snapshots.size())));
                }
                for (var thread : threads) {
                    thread.join();
                }
                assertTrue(failures.isEmpty(), () -> "committers failed: " + failures);
                assertTrue(log.overlappedGroups() > 0, "no group of the committers was written beside another");
                for (var index = 0; index < snapshots.size(); index++) {
                    try (var recovered = open(snapshots.get(index).getPath("/store"), LogWriter.Overlap.OFF)) {
                        var entries = contents(recovered);
                        for (var entry : expected.get(index).entrySet()) {
                            assertEquals(entry.getValue(), entries.get(entry.getKey()), "snapshot " + index + ", key " + entry.getKey());
                        }
                        // A committer's commits follow on, so it recovers a prefix of them
                        for (var committer = 0; committer < committers; committer++) {
                            var name = "c" + committer;
                            var missing = false;
                            for (var i = 0; i < perCommitter; i++) {
                                var present = entries.containsKey(name + "-%03d".formatted(i));
                                assertTrue(!present || !missing, "snapshot " + index + ": " + name + " recovered a commit after one it lost");
                                missing |= !present;
                            }
                        }
                    }
                }
                survivesPowerLoss(disk, new TreeMap<>(acknowledged), List.of());
            }
        }

        @Test
        @DisplayName("whose write through fails is written again through the cache once the other lane's write returns, and the store forces from then on")
        void failedWrite() throws Exception {
            var disk = CrashFileSystem.create();
            var failed = new AtomicBoolean();
            var armed = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                if (!path.toString().endsWith(".log")) {
                    return;
                }
                if (kind == ChangeInterceptor.ChangeKind.FORCE) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    }
                } else if (kind == ChangeInterceptor.ChangeKind.WRITE && armed.get()
                        && Thread.currentThread().getName().startsWith("ledger-log-lane") && failed.compareAndSet(false, true)) {
                    throw new IOException("The device refused the write");
                }
            });
            var model = new ConcurrentHashMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON)) {
                var log = log(ledger);
                var warm = new TreeMap<String, String>();
                reachWriteThrough(ledger, warm);
                model.putAll(warm);
                armed.set(true);
                var threads = new ArrayList<Thread>();
                var failures = new ConcurrentHashMap<String, Throwable>();
                for (var committer = 0; committer < 8; committer++) {
                    var name = "f" + committer;
                    threads.add(Thread.ofVirtual().start(() -> {
                        try {
                            for (var i = 0; i < 40; i++) {
                                var key = name + "-" + i;
                                ledger.commit(ledger.newBatch().put(NAMES, key, value(key))).awaitDurable();
                                model.put(key, value(key));
                            }
                        } catch (Throwable throwable) {
                            failures.put(name, throwable);
                        }
                    }));
                }
                for (var thread : threads) {
                    thread.join();
                }
                assertTrue(failures.isEmpty(), () -> "committers failed: " + failures);
                assertTrue(failed.get(), "no write of the second lane was refused");
                assertNull(log.writeThroughKind(), "the store still writes through after a refused write");
                assertTrue(!log.overlapping(), "groups still go beside one another after a refused write");
                survivesPowerLoss(disk, new TreeMap<>(model), List.of());
            }
        }

        @Test
        @DisplayName("frames queued while the second lane's thread ends are written, and a later group beside another starts it again")
        void laneEnds() throws Exception {
            var disk = CrashFileSystem.create();
            slowForces(disk);
            var model = new ConcurrentHashMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON)) {
                var log = log(ledger);
                var warm = new TreeMap<String, String>();
                reachWriteThrough(ledger, warm);
                model.putAll(warm);
                for (var round = 0; round < 2; round++) {
                    var before = log.overlappedGroups();
                    var threads = new ArrayList<Thread>();
                    var failures = new ConcurrentHashMap<String, Throwable>();
                    var prefix = "r" + round + "c";
                    for (var committer = 0; committer < 6; committer++) {
                        var name = prefix + committer;
                        threads.add(Thread.ofVirtual().start(() -> {
                            try {
                                for (var i = 0; i < 25; i++) {
                                    var key = name + "-" + i;
                                    ledger.commit(ledger.newBatch().put(NAMES, key, value(key))).awaitDurable();
                                    model.put(key, value(key));
                                }
                            } catch (Throwable throwable) {
                                failures.put(name, throwable);
                            }
                        }));
                    }
                    for (var thread : threads) {
                        thread.join();
                    }
                    assertTrue(failures.isEmpty(), () -> "committers failed: " + failures);
                    assertTrue(log.overlappedGroups() > before, "round " + round + " wrote no group beside another");
                    // Commits at random moments while the lane's thread runs out of work and ends
                    var random = new Random(round);
                    var deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                    var single = 0;
                    while (log.laneRunning() && System.nanoTime() < deadline) {
                        Thread.sleep(random.nextInt(400));
                        var key = "r" + round + "-single-" + single++;
                        var committer = new Committer(ledger, key);
                        committer.join();
                        model.put(key, committer.value);
                    }
                    await(() -> !log.laneRunning(), "the second lane's thread to end");
                }
                assertEquals(new TreeMap<>(model), contents(ledger));
                survivesPowerLoss(disk, new TreeMap<>(model), List.of());
            }
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        /// Writes the group of `k` held at its force, the group of `k+1` beside it, durable, and nothing after them.
        private record Pair(CrashFileSystem disk, Holds holds, Ledger ledger, Committer first, Committer second, long segment) {
            void finish() throws InterruptedException {
                holds.releaseAll();
                first.join();
                second.join();
                ledger.close();
            }
        }

        private static Pair pair(Map<String, String> model, String calibrated) throws Exception {
            var disk = CrashFileSystem.create();
            var holds = new Holds();
            disk.intercept(holds);
            var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON);
            var log = log(ledger);
            reachWriteThrough(ledger, model);
            if (calibrated != null) {
                // A group whose frames end on a block, with no padding after them
                commit(ledger, model, "cal0", value("cal0"));
                var length = log.groupEnd() - log.groupStart();
                var exact = "v".repeat((int) (value("cal1").length() + LogWriter.ALIGNMENT - length));
                commit(ledger, model, "cal1", exact);
                assertEquals(LogWriter.ALIGNMENT, log.groupEnd() - log.groupStart(), "the calibrated group does not end on a block");
            }
            holds.holdMain = 1;
            holds.armed = true;
            var first = waiting(ledger, "k");
            assertTrue(holds.mainHeld.await(1, TimeUnit.MINUTES), "the flush's group never reached the device");
            var second = waiting(ledger, "k+1");
            await(() -> log.laneWritten(true), "the group beside it to be written");
            return new Pair(disk, holds, ledger, first, second, log.currentSegment());
        }

        @Test
        @DisplayName("opens after any loss of power with the earlier of two groups not durable and the later one durable")
        void earlierTorn() throws Exception {
            var model = new TreeMap<String, String>();
            var pair = pair(model, null);
            try {
                survivesPowerLoss(pair.disk(), model, List.of("k", "k+1"));
            } finally {
                pair.finish();
            }
        }

        @Test
        @DisplayName("drops both groups when the earlier one's first frame is lost, its body fails authentication, or it lost its first frame right after a group that ends on a block")
        void earlierFirstFrame() throws Exception {
            for (var calibrated : new String[]{null, "exact"}) {
                var model = new TreeMap<String, String>();
                var pair = pair(model, calibrated);
                try {
                    var zeroed = reopened(pair.disk(), pair.segment(), (path, groups) -> {
                        var beside = groups.getLast();
                        assertTrue(beside.overlaps(), "the last group was not written beside another");
                        writeAt(path, beside.named(), new byte[LogWriter.FRAME_HEADER]);
                    });
                    assertEquals(model, zeroed, "the first frame of the earlier group lost, calibrated: " + calibrated);
                    var damaged = reopened(pair.disk(), pair.segment(), (path, groups) -> damageBody(path, groups.getLast().named()));
                    assertEquals(model, damaged, "the body of the earlier group damaged, calibrated: " + calibrated);
                } finally {
                    pair.finish();
                }
            }
        }

        @Test
        @DisplayName("replays the earlier group and drops the later one when only the padding after the earlier one is torn")
        void earlierPaddingTorn() throws Exception {
            var model = new TreeMap<String, String>();
            var pair = pair(model, null);
            try {
                var recovered = reopened(pair.disk(), pair.segment(), (path, groups) -> {
                    var beside = groups.getLast();
                    var earlier = groups.get(groups.size() - 2);
                    assertEquals(beside.named(), earlier.offset());
                    assertTrue(earlier.frameEnd() % LogWriter.ALIGNMENT != 0, "the earlier group has no padding");
                    writeAt(path, earlier.frameEnd(), new byte[]{0x5A, 0x5A, 0x5A});
                });
                var expected = new TreeMap<>(model);
                expected.put("k", value("k"));
                assertEquals(expected, recovered);
            } finally {
                pair.finish();
            }
        }

        @Test
        @DisplayName("refuses the log when an acknowledged group before both is damaged")
        void acknowledgedDamaged() throws Exception {
            var model = new TreeMap<String, String>();
            var pair = pair(model, null);
            try {
                var survivor = pair.disk().afterProcessCrash();
                var path = segment(survivor, pair.segment());
                var groups = groups(path);
                damageBody(path, groups.get(groups.size() - 3).offset());
                assertThrows(LedgerCorruptedException.class, () -> open(survivor.getPath("/store"), LogWriter.Overlap.OFF).close());
            } finally {
                pair.finish();
            }
        }

        @Test
        @DisplayName("refuses the log in a segment of the version before groups beside one another, which never skips a later group")
        void olderVersion() throws Exception {
            var model = new TreeMap<String, String>();
            var pair = pair(model, null);
            try {
                var survivor = pair.disk().afterProcessCrash();
                var path = segment(survivor, pair.segment());
                var groups = groups(path);
                writeAt(path, groups.getLast().named(), new byte[LogWriter.FRAME_HEADER]);
                setVersion(path, LogWriter.OVERLAPS_VERSION - 1);
                assertThrows(LedgerCorruptedException.class, () -> open(survivor.getPath("/store"), LogWriter.Overlap.OFF).close());
            } finally {
                pair.finish();
            }
        }

        @Test
        @DisplayName("opens with the earlier group durable, the one beside it torn and the next, beside that one, durable; and refuses the log when the first of the three, acknowledged, is damaged")
        void middleTorn() throws Exception {
            var disk = CrashFileSystem.create();
            var holds = new Holds();
            disk.intercept(holds);
            var model = new TreeMap<String, String>();
            var ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON);
            Committer first = null;
            Committer second = null;
            Committer third = null;
            try {
                var log = log(ledger);
                reachWriteThrough(ledger, model);
                holds.holdMain = 1;
                holds.holdLane = 1;
                holds.armed = true;
                first = waiting(ledger, "k");
                assertTrue(holds.mainHeld.await(1, TimeUnit.MINUTES), "the flush's group never reached the device");
                second = waiting(ledger, "k+1");
                assertTrue(holds.laneHeld.await(1, TimeUnit.MINUTES), "the second lane's group never reached the device");
                holds.mainRelease.countDown();
                first.join();
                model.put(first.key, first.value);
                third = waiting(ledger, "k+2");
                await(() -> log.laneWritten(false), "the group beside the held one to be written");
                assertTrue(!second.durable && !third.durable, "a group was acknowledged before the one laid out ahead of it");
                survivesPowerLoss(disk, model, List.of("k+1", "k+2"));
                var segment = log.currentSegment();
                var zeroed = reopened(disk, segment, (path, groups) -> {
                    var last = groups.getLast();
                    var middle = groups.get(groups.size() - 2);
                    assertTrue(last.overlaps() && middle.overlaps(), "the last two groups were not each written beside the one before");
                    assertEquals(middle.offset(), last.named());
                    writeAt(path, middle.offset(), new byte[LogWriter.FRAME_HEADER]);
                });
                assertEquals(model, zeroed);
                var survivor = disk.afterProcessCrash();
                var path = segment(survivor, segment);
                var groups = groups(path);
                damageBody(path, groups.get(groups.size() - 3).offset());
                assertThrows(LedgerCorruptedException.class, () -> open(survivor.getPath("/store"), LogWriter.Overlap.OFF).close());
            } finally {
                holds.releaseAll();
                for (var committer : new Committer[]{first, second, third}) {
                    if (committer != null) {
                        committer.join();
                    }
                }
                ledger.close();
            }
        }

        @Test
        @DisplayName("recovers, from concurrent committers writing groups beside one another, every acknowledged commit and a prefix of each committer's, whichever force the process stops at")
        void everyForce() throws Exception {
            var dry = CrashFileSystem.create();
            slowForces(dry);
            var planned = new Workload(dry);
            planned.run();
            assertTrue(planned.overlapped > 0, "the workload wrote no group beside another");
            var span = dry.forces() - planned.warmForces;
            var step = Math.max(1, span / 48);
            for (var stop = planned.warmForces; stop <= dry.forces(); stop += step) {
                var disk = CrashFileSystem.create();
                slowForces(disk);
                disk.stopAtForce(stop);
                var workload = new Workload(disk);
                workload.run();
                var survivor = disk.afterPowerLoss(new Random(stop));
                try (var ledger = open(survivor.getPath("/store"), LogWriter.Overlap.ON)) {
                    var recovered = contents(ledger);
                    for (var entry : workload.acknowledged.entrySet()) {
                        assertEquals(entry.getValue(), recovered.get(entry.getKey()), "stop at force " + stop + ", key " + entry.getKey());
                    }
                    for (var committer = 0; committer < Workload.COMMITTERS; committer++) {
                        var missing = false;
                        for (var i = 0; i < Workload.COMMITS; i++) {
                            var present = recovered.containsKey(Workload.key(committer, i));
                            assertTrue(!present || !missing, "stop at force " + stop + ": committer " + committer + " recovered a commit after one it lost");
                            missing |= !present;
                        }
                    }
                    ledger.commit(ledger.newBatch().put(NAMES, "~after", "v")).awaitDurable();
                }
                try (var ledger = open(survivor.getPath("/store"), LogWriter.Overlap.OFF)) {
                    assertEquals(Optional.of("v"), ledger.get(NAMES, "~after"));
                }
            }
        }
    }

    /// Commits from four threads at once to a store that writes groups beside one another, after enough commits to
    /// make spares and reach one; and what it learned while it ran. A stopped file system ends it.
    private static final class Workload {
        static final int COMMITTERS = 4;
        static final int COMMITS = 30;
        private final CrashFileSystem disk;
        final Map<String, String> acknowledged;
        long warmForces;
        long overlapped;

        Workload(CrashFileSystem disk) {
            this.disk = disk;
            this.acknowledged = new ConcurrentHashMap<>();
        }

        static String key(int committer, int commit) {
            return "c" + committer + "-%03d".formatted(commit);
        }

        void run() {
            Ledger ledger = null;
            try {
                ledger = open(disk.getPath("/store"), LogWriter.Overlap.ON);
                var log = log(ledger);
                var warm = new TreeMap<String, String>();
                reachWriteThrough(ledger, warm);
                acknowledged.putAll(warm);
                warmForces = disk.forces();
                var store = ledger;
                var threads = new ArrayList<Thread>();
                for (var committer = 0; committer < COMMITTERS; committer++) {
                    var id = committer;
                    threads.add(Thread.ofVirtual().start(() -> {
                        try {
                            for (var i = 0; i < COMMITS; i++) {
                                var key = key(id, i);
                                var value = "x".repeat(i % 5 * 400) + key;
                                // Every other one with a timeout, which leaves its group to the pool's flush
                                var commit = store.commit(store.newBatch().put(NAMES, key, value));
                                if (i % 2 == 0) {
                                    commit.awaitDurable();
                                } else if (!commit.awaitDurable(WAIT)) {
                                    return;
                                }
                                acknowledged.put(key, value);
                            }
                        } catch (RuntimeException | InterruptedException _) {
                            // The file system stopped
                        }
                    }));
                }
                for (var thread : threads) {
                    thread.join();
                }
                overlapped = log.overlappedGroups();
                ledger.close();
            } catch (RuntimeException | InterruptedException _) {
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

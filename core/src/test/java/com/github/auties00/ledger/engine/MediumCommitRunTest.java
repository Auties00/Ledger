package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.WriteBatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The runs that commits of [LiveLedger#RUN_APPEND_WRITES] puts or more to a paged key space whose latest tree holds
/// entries of it keep their puts of new keys in when they find the store idle: uncounted runs, counted against that tree
/// from what the checkpoint that takes them settles. The expected state of every test is kept beside the store and
/// checked through point reads, counts, scans and snapshots; scans come after the appends, as a scan stops medium
/// commits appending to the run it orders.
class MediumCommitRunTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // Messages the populated tree holds
    private static final int POPULATED = 3_000;
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN, 200, 700, 1_500};

    private static LiveLedger open(Path directory, boolean encrypted, long budget) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(Duration.ZERO))
                .segmentSize(4L << 20)
                .spillBudget(budget)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(message).array();
    }

    private static byte[] value(int message, int generation) {
        var length = LENGTHS[Math.floorMod(message * 7 + generation, LENGTHS.length)];
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (message * 31 + generation * 17 + i);
        }
        return value;
    }

    private static void put(WriteBatch batch, TreeMap<Integer, byte[]> expected, int message, int generation) {
        var value = value(message, generation);
        batch.put(MESSAGES, key(message), value);
        expected.put(message, value);
    }

    // Fills the key space with POPULATED messages and checkpoints them, so its latest tree holds them
    private static TreeMap<Integer, byte[]> populate(LiveLedger ledger) throws InterruptedException {
        var expected = new TreeMap<Integer, byte[]>();
        var batch = ledger.newBatch();
        for (var message = 0; message < POPULATED; message++) {
            put(batch, expected, message, 0);
        }
        ledger.commit(batch).awaitDurable();
        ledger.checkpoint();
        assertNull(ledger.state(MESSAGES).run, "the checkpoint left the run it took");
        assertNotNull(ledger.tree().root(ledger.state(MESSAGES).id), "no tree of the key space");
        return expected;
    }

    // Point reads and the count only: a scan would order the run and stop the medium appends
    private static void verifyPoints(View view, TreeMap<Integer, byte[]> expected, int last) {
        assertEquals(expected.size(), view.size(MESSAGES), "messages counted");
        for (var message = 0; message <= last; message++) {
            var wanted = expected.get(message);
            var found = view.get(MESSAGES, key(message));
            if (wanted == null) {
                assertTrue(found.isEmpty(), "message " + message + " read present");
            } else {
                assertTrue(found.isPresent(), "message " + message + " absent");
                assertArrayEquals(wanted, found.get(), "message " + message);
            }
        }
    }

    // Every way a view reads `expected`
    private static void verify(View view, TreeMap<Integer, byte[]> expected, int last) {
        verifyPoints(view, expected, last);
        var keys = new ArrayList<Integer>();
        view.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            var message = ByteBuffer.wrap(key).getInt();
            assertArrayEquals(expected.get(message), value, "message " + message + " scanned");
            keys.add(message);
            return true;
        });
        assertEquals(new ArrayList<>(expected.keySet()), keys, "messages scanned");
        var descending = new ArrayList<Integer>();
        view.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (key, _) -> {
            descending.add(ByteBuffer.wrap(key).getInt());
            return true;
        });
        assertEquals(new ArrayList<>(expected.descendingKeySet()), descending, "messages scanned descending");
        for (var from = 0; from < last; from += 977) {
            var to = from + 400;
            var range = view.range(MESSAGES, KeyRange.between(key(from), key(to)), Integer.MAX_VALUE);
            assertEquals(expected.subMap(from, to).size(), range.size(), "messages from " + from);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("idle commits of 100 writes over a populated tree append to an uncounted run, overwriting, adding and removing keys, counted exactly at every view, through snapshots, a checkpoint and a reopen")
    void uncountedRunOverPopulatedTree(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(7);
        TreeMap<Integer, byte[]> expected;
        var last = POPULATED + 4_000;
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            expected = populate(ledger);
            var snapshots = new ArrayList<Snapshot>();
            var seen = new ArrayList<TreeMap<Integer, byte[]>>();
            var base = ledger.tree().root(ledger.state(MESSAGES).id);
            PackedRun first = null;
            for (var commit = 0; commit < 40; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 50; i++) {
                    put(batch, expected, POPULATED + commit * 100 + i, commit);
                }
                for (var i = 0; i < 40; i++) {
                    put(batch, expected, random.nextInt(POPULATED), commit + 1);
                }
                for (var i = 0; i < 5; i++) {
                    // Keys earlier commits put in the run, on chains over its entries
                    put(batch, expected, POPULATED + random.nextInt(commit * 100 + 50), commit + 2);
                }
                for (var i = 0; i < 4; i++) {
                    var message = random.nextInt(POPULATED);
                    batch.delete(MESSAGES, key(message));
                    expected.remove(message);
                }
                // A key no tree, run or chain holds
                batch.delete(MESSAGES, key(last + 1 + commit));
                ledger.commit(batch);
                var run = ledger.state(MESSAGES).run;
                assertNotNull(run, "an idle commit of 100 puts made no run");
                assertFalse(run.counted(), "a run over a populated tree counted its puts");
                assertSame(base, run.base(), "the run is counted against another tree than the latest");
                if (first == null) {
                    first = run;
                } else {
                    assertTrue(run.sharesArraysForTests(first), "a commit made a run beside the first");
                }
                assertTrue(run.size() > commit * 50, "the run took " + run.size() + " puts after " + (commit + 1) + " commits");
                snapshots.add(ledger.snapshot());
                seen.add(new TreeMap<>(expected));
                verifyPoints(ledger, expected, last);
            }
            assertFalse(first.ordered(), "a point read ordered the run");
            var chunks = ledger.state(MESSAGES).run.keyChunkLengthsForTests();
            assertEquals(1, chunks.length, "small appends took chunks of their own");
            for (var i = 0; i < snapshots.size(); i++) {
                verify(snapshots.get(i), seen.get(i), last);
            }
            for (var snapshot : snapshots) {
                snapshot.close();
            }
            verify(ledger, expected, last);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run, "the checkpoint left the run it took");
            verify(ledger, expected, last);
            // A run after the release starts from what the released one left to the count
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, last - 100 + i, 99);
            }
            ledger.commit(batch).awaitDurable();
            assertNotNull(ledger.state(MESSAGES).run);
            verify(ledger, expected, last);
            ledger.checkpoint();
            verify(ledger, expected, last);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected, last);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    @Test
    @DisplayName("a commit of 63 puts to a key space keeps them on chains, one of 64 makes an uncounted run over a populated tree and keeps them on chains over an empty one")
    void thresholds(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var batch = ledger.newBatch();
            for (var i = 0; i < LiveLedger.RUN_APPEND_WRITES - 1; i++) {
                put(batch, expected, POPULATED + i, 1);
                batch.put(TEXTS, "t" + i, "text " + i);
            }
            ledger.commit(batch).awaitDurable();
            assertNull(ledger.state(MESSAGES).run, "a commit of 63 puts made a run");
            assertNull(ledger.state(TEXTS).run, "a commit of 63 puts made a run");
            batch = ledger.newBatch();
            for (var i = 0; i < LiveLedger.RUN_APPEND_WRITES; i++) {
                put(batch, expected, POPULATED + 100 + i, 1);
                batch.put(TEXTS, "u" + i, "text " + i);
            }
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "a commit of 64 puts made no run");
            assertFalse(run.counted());
            assertEquals(LiveLedger.RUN_APPEND_WRITES, run.size());
            assertNull(ledger.state(TEXTS).run, "a commit of 64 puts to an empty key space made a run");
            assertEquals(2 * LiveLedger.RUN_APPEND_WRITES - 1, ledger.state(TEXTS).chains());
            assertEquals(2 * LiveLedger.RUN_APPEND_WRITES - 1, ledger.size(TEXTS));
            verify(ledger, expected, POPULATED + 200);
            ledger.checkpoint();
            verify(ledger, expected, POPULATED + 200);
            assertEquals(2 * LiveLedger.RUN_APPEND_WRITES - 1, ledger.size(TEXTS));
        }
    }

    @Test
    @DisplayName("a commit of 100 puts that finds another commit under way keeps them on chains, so writers that meet never run alone")
    void contendedCommitTakesChains(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var held = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var mediumSequenced = new CountDownLatch(1);
            var first = new AtomicLong(-1);
            ledger.hookSequencedForTests(sequence -> {
                if (first.compareAndSet(-1, sequence)) {
                    held.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    }
                } else {
                    mediumSequenced.countDown();
                }
            });
            var small = ledger.newBatch();
            put(small, expected, POPULATED + 500, 1);
            var smallCommit = Thread.ofPlatform().start(() -> ledger.commit(small));
            assertTrue(held.await(10, TimeUnit.SECONDS));
            var medium = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(medium, expected, POPULATED + i, 1);
            }
            var failure = new AtomicReference<Throwable>();
            var mediumCommit = Thread.ofPlatform().start(() -> {
                try {
                    ledger.commit(medium);
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            assertTrue(mediumSequenced.await(10, TimeUnit.SECONDS), "the commit of 100 puts did not take the pipeline");
            release.countDown();
            smallCommit.join();
            mediumCommit.join();
            ledger.hookSequencedForTests(null);
            assertNull(failure.get());
            assertNull(ledger.state(MESSAGES).run, "a contended commit made a run");
            assertTrue(ledger.state(MESSAGES).chains() >= 100, "the contended commit kept no chains");
            verify(ledger, expected, POPULATED + 600);
            ledger.checkpoint();
            verify(ledger, expected, POPULATED + 600);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("no uncounted run is made while a checkpoint's cut waits for its trees, a cut closes the run, and the run made after the trees publish is counted against them")
    void outstandingCutKeepsChains(boolean encrypted, @TempDir Path directory) throws Exception {
        var last = POPULATED + 2_000;
        TreeMap<Integer, byte[]> expected;
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            expected = populate(ledger);
            var current = expected;
            var ran = new AtomicBoolean();
            ledger.checkpointer().hookCutForTests(() -> {
                if (ran.compareAndSet(false, true)) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 100; i++) {
                        put(batch, current, i * 13, 2);
                    }
                    ledger.commit(batch);
                    assertNull(ledger.state(MESSAGES).run, "an uncounted run made while a cut waited for its trees");
                }
            });
            // A write after the populating checkpoint, so the cut is not empty
            var batch = ledger.newBatch();
            put(batch, expected, POPULATED + 1_999, 1);
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            assertTrue(ran.get());
            verifyPoints(ledger, expected, last);
            // A run over the trees just published, closed by the next cut while its trees wait
            batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + i, 3);
            }
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            assertSame(ledger.tree().root(ledger.state(MESSAGES).id), run.base());
            var during = new AtomicBoolean();
            ledger.checkpointer().hookCutForTests(() -> {
                if (during.compareAndSet(false, true)) {
                    assertFalse(run.appendable(), "the cut left the run open to appends");
                    var inner = ledger.newBatch();
                    for (var i = 0; i < 100; i++) {
                        put(inner, current, POPULATED + 200 + i, 4);
                    }
                    ledger.commit(inner);
                    assertSame(run, ledger.state(MESSAGES).run, "a commit appended to a closed run");
                    assertEquals(100, run.size());
                }
            });
            ledger.checkpoint();
            assertTrue(during.get());
            ledger.checkpointer().hookCutForTests(null);
            verifyPoints(ledger, expected, last);
            batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + 400 + i, 5);
            }
            ledger.commit(batch).awaitDurable();
            var next = ledger.state(MESSAGES).run;
            assertNotNull(next, "no run once the trees published and the run was released");
            assertNotSame(run, next);
            assertSame(ledger.tree().root(ledger.state(MESSAGES).id), next.base());
            verify(ledger, expected, last);
            ledger.checkpoint();
            verify(ledger, expected, last);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected, last);
        }
    }

    @Test
    @DisplayName("a bulk commit's counted run made while a cut waits takes no append once that cut's trees publish entries of its key space, and the next checkpoint merges it")
    void countedRunDuringCutStops(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var small = ledger.newBatch();
            for (var i = 0; i < 20; i++) {
                put(small, expected, 5_000 + i, 0);
            }
            ledger.commit(small).awaitDurable();
            var ran = new AtomicBoolean();
            ledger.checkpointer().hookCutForTests(() -> {
                if (ran.compareAndSet(false, true)) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < LiveLedger.BULK_RUN_WRITES; i++) {
                        put(batch, expected, i, 1);
                    }
                    ledger.commit(batch);
                    var run = ledger.state(MESSAGES).run;
                    assertNotNull(run, "no counted run beside the cut");
                    assertTrue(run.counted());
                }
            });
            ledger.checkpoint();
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "a checkpoint below the run released it");
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, LiveLedger.BULK_RUN_WRITES + i, 2);
            }
            ledger.commit(batch).awaitDurable();
            assertSame(run, ledger.state(MESSAGES).run, "a counted run took an append over a populated tree");
            assertEquals(LiveLedger.BULK_RUN_WRITES, run.size());
            verify(ledger, expected, 5_100);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected, 5_100);
        }
    }

    @Test
    @DisplayName("a commit planned into a run that a cut closes before it takes the roles writes every put blind, counted exactly, and the checkpoints' count checks pass")
    void planClosedBeforeRoles(@TempDir Path directory) throws Exception {
        var last = POPULATED + 1_000;
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + i, 1);
            }
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            var cutting = new CountDownLatch(1);
            var proceed = new CountDownLatch(1);
            var failure = new AtomicReference<Throwable>();
            var checkpoint = new AtomicReference<Thread>();
            ledger.checkpointer().hookCutForTests(() -> {
                cutting.countDown();
                try {
                    proceed.await();
                } catch (InterruptedException exception) {
                    throw new AssertionError(exception);
                }
            });
            ledger.hookPlanForTests(() -> {
                ledger.hookPlanForTests(null);
                checkpoint.set(Thread.ofPlatform().start(() -> {
                    try {
                        ledger.checkpoint();
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                }));
                try {
                    assertTrue(cutting.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException exception) {
                    throw new AssertionError(exception);
                }
            });
            var planned = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                // New keys, keys of the tree, keys of the closed run, and removals of each
                put(planned, expected, POPULATED + 200 + i, 2);
            }
            for (var i = 0; i < 30; i++) {
                put(planned, expected, i * 31, 2);
                put(planned, expected, POPULATED + i * 2, 2);
            }
            for (var i = 0; i < 10; i++) {
                planned.delete(MESSAGES, key(i * 37 + 1));
                expected.remove(i * 37 + 1);
                planned.delete(MESSAGES, key(POPULATED + 99 - i));
                expected.remove(POPULATED + 99 - i);
            }
            ledger.commit(planned);
            assertSame(run, ledger.state(MESSAGES).run, "a commit appended to a run the cut closed");
            assertEquals(100, run.size());
            assertTrue(ledger.state(MESSAGES).blind.listed() > 0, "the commit wrote nothing blind");
            assertEquals(expected.size(), ledger.size(MESSAGES));
            proceed.countDown();
            checkpoint.get().join();
            ledger.checkpointer().hookCutForTests(null);
            assertNull(failure.get());
            verify(ledger, expected, last);
            ledger.checkpoint();
            verify(ledger, expected, last);
        }
    }

    @Test
    @DisplayName("a snapshot older than a run, one taken between its appends and one pinned across hundreds of them each read and count as of their sequence")
    void snapshotsAcrossAppends(@TempDir Path directory) throws Exception {
        var last = POPULATED + 30_000;
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var before = ledger.snapshot();
            var beforeSeen = new TreeMap<>(expected);
            Snapshot middle = null;
            TreeMap<Integer, byte[]> middleSeen = null;
            for (var commit = 0; commit < 300; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 90; i++) {
                    put(batch, expected, POPULATED + commit * 90 + i, commit);
                }
                for (var i = 0; i < 10; i++) {
                    put(batch, expected, (commit * 10 + i) % POPULATED, commit + 1);
                }
                ledger.commit(batch);
                if (commit == 150) {
                    middle = ledger.snapshot();
                    middleSeen = new TreeMap<>(expected);
                }
                if (commit % 50 == 0) {
                    assertEquals(beforeSeen.size(), before.size(MESSAGES), "the snapshot below the run counted its entries");
                }
            }
            var run = ledger.state(MESSAGES).run;
            assertEquals(300 * 100, run.size(), "an append left the run");
            assertNotNull(run.previous(), "a view a snapshot reads was let go of");
            verifyPoints(before, beforeSeen, last);
            verifyPoints(middle, middleSeen, last);
            verifyPoints(ledger, expected, last);
            verify(before, beforeSeen, last);
            verify(middle, middleSeen, last);
            before.close();
            middle.close();
            verify(ledger, expected, last);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected, last);
        }
    }

    @Test
    @DisplayName("a scan of a run stops commits of fewer than 2,048 puts appending to it, which then keep their puts on chains")
    void scanStopsMediumAppends(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + i, 1);
            }
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertFalse(run.ordered());
            verify(ledger, expected, POPULATED + 300);
            assertTrue(run.ordered(), "a scan left the run unordered");
            batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + 100 + i, 1);
            }
            ledger.commit(batch).awaitDurable();
            assertSame(run, ledger.state(MESSAGES).run, "a commit appended to a run a scan ordered");
            assertTrue(ledger.state(MESSAGES).chains() >= 100);
            verify(ledger, expected, POPULATED + 300);
            ledger.checkpoint();
            verify(ledger, expected, POPULATED + 300);
        }
    }

    @Test
    @DisplayName("hundreds of commits of 100 puts stay in one run, their keys sharing chunks, until the run's medium limit, and later ones take chains")
    void appendsUpToTheLimit(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var commits = LiveLedger.MEDIUM_RUN_ENTRIES / 100 + 2;
            var taken = 0;
            for (var commit = 0; commit < commits; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 100; i++) {
                    var message = POPULATED + commit * 100 + i;
                    batch.put(MESSAGES, key(message), new byte[]{(byte) message});
                    expected.put(message, new byte[]{(byte) message});
                }
                ledger.commit(batch);
                var run = ledger.state(MESSAGES).run;
                if ((commit + 1) * 100 <= LiveLedger.MEDIUM_RUN_ENTRIES) {
                    assertEquals((commit + 1) * 100, run.size(), "commit " + commit + " left the run");
                    taken = run.size();
                } else {
                    assertEquals(taken, run.size(), "a commit appended past the limit");
                }
            }
            var run = ledger.state(MESSAGES).run;
            var chunks = run.keyChunkLengthsForTests();
            // 4-byte keys: every chunk is whole but the last
            assertEquals((taken * 4 + PackedRun.KEY_CHUNK - 1) / PackedRun.KEY_CHUNK, chunks.length);
            assertTrue(run.slotsForTests() <= 1 << 17, "the hash table grew past half a 2 MiB region");
            assertEquals(expected.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            assertEquals(expected.size(), ledger.size(MESSAGES));
            for (var message = POPULATED; message < POPULATED + commits * 100; message += 37) {
                assertArrayEquals(expected.get(message), ledger.get(MESSAGES, key(message)).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("an idle commit of more puts than the run's medium limit over a populated tree keeps them on chains")
    void commitPastTheLimitTakesChains(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false, Long.MAX_VALUE)) {
            var expected = populate(ledger);
            var batch = ledger.newBatch();
            for (var i = 0; i <= LiveLedger.MEDIUM_RUN_ENTRIES; i++) {
                var message = POPULATED + i;
                batch.put(MESSAGES, key(message), new byte[]{(byte) message});
                expected.put(message, new byte[]{(byte) message});
            }
            ledger.commit(batch).awaitDurable();
            assertNull(ledger.state(MESSAGES).run, "a commit past the medium limit made a run");
            assertEquals(expected.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            assertEquals(expected.size(), ledger.size(MESSAGES));
            for (var message = 0; message <= POPULATED + LiveLedger.MEDIUM_RUN_ENTRIES; message += 101) {
                assertArrayEquals(expected.get(message), ledger.get(MESSAGES, key(message)).orElseThrow());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store that crashes after appends to an uncounted run recovers them from the log, counted exactly")
    void crashAfterAppends(boolean encrypted, @TempDir Path directory) throws Exception {
        var last = POPULATED + 3_000;
        TreeMap<Integer, byte[]> expected;
        try (var ledger = open(directory, encrypted, 0)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            expected = populate(ledger);
            for (var commit = 0; commit < 20; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 80; i++) {
                    put(batch, expected, POPULATED + commit * 80 + i, commit);
                }
                for (var i = 0; i < 20; i++) {
                    put(batch, expected, commit * 20 + i, commit);
                }
                batch.delete(MESSAGES, key(2_900 + commit));
                expected.remove(2_900 + commit);
                ledger.commit(batch).awaitDurable();
            }
            assertNotNull(ledger.state(MESSAGES).run);
            verifyPoints(ledger, expected, last);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected, last);
            ledger.checkpoint();
            verify(ledger, expected, last);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected, last);
        }
    }

    @Test
    @DisplayName("an append to an uncounted run taken back by a decoder that throws leaves the run as its newest view holds it, and the next append continues its keys")
    void rollbackOfAppend(@TempDir Path directory) throws Exception {
        ValueCodec<String> refusing = ValueCodec.of(value -> value.getBytes(StandardCharsets.UTF_8).length,
                (value, target) -> MemorySegment.copy(MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)), 0,
                        target, 0, target.byteSize()),
                source -> {
                    var text = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
                    if (text.equals("fail")) {
                        throw new IllegalArgumentException("refused");
                    }
                    return text;
                });
        var failing = Keyspace.resident("failing", KeyCodec.utf8(), refusing);
        try (var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(Encryption.key(KEY)).keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).keyspace(failing))
                .writeThrough(WriteThrough.Mode.OFF)
                .build()) {
            var expected = populate(ledger);
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + i, 1);
            }
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            var count = ledger.size(MESSAGES);
            var taken = ledger.newBatch();
            for (var i = 0; i < 500; i++) {
                taken.put(MESSAGES, key(POPULATED + 100 + i), value(i, 9));
            }
            for (var i = 0; i < 50; i++) {
                taken.put(MESSAGES, key(i), value(i, 9));
            }
            taken.putEncoded(failing, MemorySegment.ofArray("k".getBytes(StandardCharsets.UTF_8)),
                    MemorySegment.ofArray("fail".getBytes(StandardCharsets.UTF_8)));
            assertThrows(IllegalArgumentException.class, () -> ledger.commit(taken));
            assertSame(run, ledger.state(MESSAGES).run, "a commit taken back published a view");
            assertEquals(100, run.size());
            assertEquals(count, ledger.size(MESSAGES));
            verifyPoints(ledger, expected, POPULATED + 700);
            batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                put(batch, expected, POPULATED + 300 + i, 2);
            }
            ledger.commit(batch).awaitDurable();
            var appended = ledger.state(MESSAGES).run;
            assertTrue(appended.sharesArraysForTests(run), "the append after a commit taken back made a run beside it");
            assertEquals(200, appended.size());
            assertEquals(1, appended.keyChunkLengthsForTests().length);
            verify(ledger, expected, POPULATED + 700);
            ledger.checkpoint();
            verify(ledger, expected, POPULATED + 700);
        }
    }

    @ParameterizedTest(name = "round {0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("medium commits beside pipelined commits, checkpoints, releases and counting readers keep every snapshot's count and reads in agreement and the counts exact")
    void racingMediumCommits(int round, @TempDir Path directory) throws Exception {
        var commits = new ConcurrentHashMap<Long, List<Object[]>>();
        try (var ledger = open(directory, true, round == 2 ? 0 : Long.MAX_VALUE)) {
            var populated = populate(ledger);
            commits.put(0L, populated.entrySet().stream().map(entry -> new Object[]{entry.getKey(), entry.getValue()}).toList());
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            // Held for reading by each step of the other threads, and for writing by the commits that must find the
            // store idle
            var quiet = new ReentrantReadWriteLock(true);
            var threads = new ArrayList<Thread>();
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(round);
                try {
                    while (!stop.get()) {
                        var batch = ledger.newBatch();
                        var writes = new ArrayList<Object[]>();
                        for (var i = 0; i < 1 + random.nextInt(3); i++) {
                            var message = random.nextInt(POPULATED + 20_000);
                            if (random.nextInt(3) == 0) {
                                batch.delete(MESSAGES, key(message));
                                writes.add(new Object[]{message, null});
                            } else {
                                var value = value(message, 100 + random.nextInt(50));
                                batch.put(MESSAGES, key(message), value);
                                writes.add(new Object[]{message, value});
                            }
                        }
                        quiet.readLock().lock();
                        try {
                            commits.put(ledger.commit(batch).sequence(), writes);
                        } finally {
                            quiet.readLock().unlock();
                        }
                        Thread.sleep(0, 200_000);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            // Counts at every view against point reads of a sample; a scan now and then, which orders the run
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    var iteration = 0;
                    while (!stop.get()) {
                        quiet.readLock().lock();
                        try (var snapshot = ledger.snapshot()) {
                            var size = snapshot.size(MESSAGES);
                            if (++iteration % 25 == 0) {
                                var scanned = snapshot.forEach(MESSAGES, KeyRange.all(), (_, _) -> true);
                                assertEquals(size, scanned, "a snapshot's count and scan disagree");
                            }
                            assertEquals(size, snapshot.size(MESSAGES), "a snapshot counted twice differently");
                        } finally {
                            quiet.readLock().unlock();
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            var checkpoints = Thread.ofPlatform().start(() -> {
                try {
                    while (round > 0 && !stop.get()) {
                        quiet.readLock().lock();
                        try {
                            ledger.checkpoint();
                        } finally {
                            quiet.readLock().unlock();
                        }
                        Thread.sleep(3);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
            var random = new Random(round + 100);
            var quietRuns = 0;
            for (var commit = 0; commit < 150; commit++) {
                // Now and then with the other threads held between their steps and a checkpoint published, so the
                // commit finds the store idle and no cut waiting for its trees whatever the timing of the machine
                var still = commit % 30 == 29;
                if (still) {
                    quiet.writeLock().lock();
                }
                try {
                    if (still) {
                        ledger.checkpoint();
                    }
                    var batch = ledger.newBatch();
                    var writes = new ArrayList<Object[]>();
                    for (var i = 0; i < 100; i++) {
                        var message = random.nextInt(4) == 0 ? random.nextInt(POPULATED) : POPULATED + commit * 100 + i;
                        var value = value(message, commit);
                        batch.put(MESSAGES, key(message), value);
                        writes.add(new Object[]{message, value});
                    }
                    for (var i = 0; i < 5; i++) {
                        var message = random.nextInt(POPULATED + commit * 100);
                        batch.delete(MESSAGES, key(message));
                        writes.add(new Object[]{message, null});
                    }
                    commits.put(ledger.commit(batch).sequence(), writes);
                    var run = ledger.state(MESSAGES).run;
                    if (still && run != null && run.appended() && !run.counted() && run.appendable()) {
                        quietRuns++;
                    }
                } finally {
                    if (still) {
                        quiet.writeLock().unlock();
                    }
                }
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            checkpoints.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertTrue(quietRuns > 0, "no medium commit that found the store idle made an uncounted run");
            var expected = new TreeMap<Integer, byte[]>();
            for (var sequence : commits.keySet().stream().sorted().toList()) {
                for (var write : commits.get(sequence)) {
                    if (write[1] == null) {
                        expected.remove((Integer) write[0]);
                    } else {
                        expected.put((Integer) write[0], (byte[]) write[1]);
                    }
                }
            }
            verify(ledger, expected, POPULATED + 20_000);
            ledger.checkpoint();
            verify(ledger, expected, POPULATED + 20_000);
        }
    }
}

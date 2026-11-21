package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.auties00.ledger.engine.BulkCommitRunTest.BULK;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.CHATS;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.Expected;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.KEY;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.LENGTHS;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.MESSAGES;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.TEXTS;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.key;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.open;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.puts;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.value;
import static com.github.auties00.ledger.engine.BulkCommitRunTest.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The run the bulk commits start beside the one a checkpoint's cut closed ([PackedRun#older()]), while the latest tree
/// holds none of the key space's entries: every read, count and scan reads both while its tree lacks them, a key lives
/// in one run at most, a reader of the trees before the cut keeps the closed run through the next checkpoint, a
/// checkpoint that failed leaves both runs to the next one, which merges both, and the runs start, append and roll back
/// as the commits that make them do. The cut's hook stalls a checkpoint between its cut and its trees, where the bulk
/// commits that start the run beside run.
class BesideRunTest {
    /// Commits `count` puts of new messages from `first` in one bulk commit, recording them in `expected`.
    private static void bulk(LiveLedger ledger, Expected expected, int first, int count, int generation) {
        var batch = ledger.newBatch();
        puts(batch, expected, first, count, generation, LENGTHS);
        ledger.commit(batch);
    }

    /// Commits the puts of messages `first` to `first + count`, excluded, one small commit each, on chains.
    private static void smallPuts(LiveLedger ledger, Expected expected, int first, int count, int generation) {
        for (var message = first; message < first + count; message++) {
            var value = value(message, generation, LENGTHS);
            ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
            expected.messages.put(message, value);
        }
    }

    /// Runs `hook` at the first cut of a checkpoint from now on, and none at the later ones.
    private static AtomicBoolean atFirstCut(LiveLedger ledger, Runnable hook) {
        var ran = new AtomicBoolean();
        ledger.checkpointer().hookCutForTests(() -> {
            if (ran.compareAndSet(false, true)) {
                hook.run();
            }
        });
        return ran;
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("bulk commits during a checkpoint start a run beside the one its cut closed and append to it, keeping their new keys off chains, and the next checkpoint takes it into the tree the first one published")
    void bulkCommitsDuringCheckpoint(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            bulk(ledger, expected, 0, BULK, 0);
            var closed = ledger.state(MESSAGES).run;
            var ran = atFirstCut(ledger, () -> {
                var chainsBefore = ledger.state(MESSAGES).chains();
                for (var commit = 0; commit < 3; commit++) {
                    bulk(ledger, expected, (commit + 1) * BULK, BULK, 1);
                }
                var beside = ledger.state(MESSAGES).run;
                assertSame(closed, beside.older());
                assertEquals(3 * BULK, beside.size(), "a bulk commit after the cut did not append to the run beside");
                assertEquals(chainsBefore, ledger.state(MESSAGES).chains(), "bulk commits after the cut kept chains");
                verify(ledger, expected);
            });
            ledger.checkpoint();
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            assertNull(ledger.state(MESSAGES).run.older(), "the checkpoint kept the run it took");
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a snapshot of the trees before a cut keeps the run that cut closed beside the younger run through the next checkpoint, which takes the younger alone, and both go once the snapshot closes")
    void pinnedReaderAcrossTwoCuts(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            bulk(ledger, expected, 0, BULK, 0);
            var closed = ledger.state(MESSAGES).run;
            var pinned = ledger.snapshot();
            var atPin = expected.copy();
            try {
                var ran = atFirstCut(ledger, () -> bulk(ledger, expected, BULK, BULK, 1));
                ledger.checkpoint();
                assertTrue(ran.get());
                ledger.checkpointer().hookCutForTests(null);
                var beside = ledger.state(MESSAGES).run;
                assertNotSame(closed, beside);
                assertSame(closed, beside.older(), "the run a snapshot reads was released");
                verify(ledger, expected);
                verify(pinned, atPin);
                // The latest tree holds the key space: a bulk commit goes to chains, over keys of both runs too
                var batch = ledger.newBatch();
                puts(batch, expected, 2 * BULK, BULK, 2, LENGTHS);
                for (var message = 0; message < 2 * BULK; message += 97) {
                    var value = value(message, 3, LENGTHS);
                    batch.put(MESSAGES, key(message), value);
                    expected.messages.put(message, value);
                }
                ledger.commit(batch).awaitDurable();
                assertSame(beside, ledger.state(MESSAGES).run, "a bulk commit appended over a tree of the key space");
                verify(ledger, expected);
                // The next checkpoint takes the younger run alone, as its trees hold the older
                ledger.checkpoint();
                var held = ledger.state(MESSAGES).run;
                assertSame(beside, held, "a run a snapshot reads was released");
                assertSame(closed, held.older(), "the run a snapshot reads was released");
                verify(ledger, expected);
                verify(pinned, atPin);
            } finally {
                pinned.close();
            }
            ledger.commit(ledger.newBatch().put(TEXTS, "after", "pin")).awaitDurable();
            expected.texts.put("after", "pin");
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run, "the runs outlived every reader of a tree that lacks them");
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a checkpoint that fails after a bulk commit started a run beside the one its cut closed leaves both runs, the younger taking appends, and the next checkpoint merges both into one tree, counted exactly")
    void failedCheckpointMergesBothRuns(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var failing = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null
                    && path.getFileName().toString().equals(PageFile.FILE)) {
                throw new IOException("No space left on device");
            }
        });
        var directory = disk.getPath("/store");
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            bulk(ledger, expected, 0, BULK, 0);
            // Chains the failed checkpoint gives back, some over the closed run's keys
            smallPuts(ledger, expected, 10 * BULK, 30, 0);
            smallPuts(ledger, expected, 5, 10, 4);
            var closed = ledger.state(MESSAGES).run;
            var ran = atFirstCut(ledger, () -> {
                // Interleaved with the closed run's keys, which go to chains
                var batch = ledger.newBatch();
                for (var message = BULK / 2; message < BULK / 2 + 2 * BULK; message++) {
                    var value = value(message, 1, LENGTHS);
                    batch.put(MESSAGES, key(message), value);
                    expected.messages.put(message, value);
                }
                ledger.commit(batch);
                failing.set(true);
            });
            assertThrows(RuntimeException.class, ledger::checkpoint);
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            failing.set(false);
            var beside = ledger.state(MESSAGES).run;
            assertSame(closed, beside.older());
            assertEquals(BULK + BULK / 2, beside.size(), "keys of the closed run went to the run beside it");
            verify(ledger, expected);
            // The failed checkpoint published no tree, so the run beside still takes appends
            bulk(ledger, expected, 4 * BULK, BULK, 2);
            assertEquals(2 * BULK + BULK / 2, ledger.state(MESSAGES).run.size());
            for (var message = 0; message < 3 * BULK; message += 41) {
                ledger.commit(ledger.newBatch().delete(MESSAGES, key(message)));
                expected.messages.remove(message);
            }
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run, "the checkpoint left a run it took");
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a commit that would start a run beside the closed one, taken back by a decoder that throws, leaves the key space's runs as they were, and the next bulk commit starts it")
    void rollbackLeavesRunsInPlace(@TempDir Path directory) throws Exception {
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
        var expected = new Expected();
        try (var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(Encryption.key(KEY)).keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).keyspace(failing))
                .writeThrough(WriteThrough.Mode.OFF)
                .build()) {
            bulk(ledger, expected, 0, BULK, 0);
            var closed = ledger.state(MESSAGES).run;
            var ran = atFirstCut(ledger, () -> {
                var batch = ledger.newBatch();
                puts(batch, new Expected(), BULK, BULK, 1, LENGTHS);
                // Decoded as the batch applies, after the run took every put
                batch.putEncoded(failing, MemorySegment.ofArray("k".getBytes(StandardCharsets.UTF_8)),
                        MemorySegment.ofArray("fail".getBytes(StandardCharsets.UTF_8)));
                assertThrows(IllegalArgumentException.class, () -> ledger.commit(batch));
                assertSame(closed, ledger.state(MESSAGES).run, "a run of a commit taken back");
                assertNull(closed.older());
                verify(ledger, expected);
                bulk(ledger, expected, BULK, BULK, 2);
                assertSame(closed, ledger.state(MESSAGES).run.older());
                verify(ledger, expected);
            });
            ledger.checkpoint();
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "round {0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("pipelined commits over both runs' keys and readers racing the start of a run beside the closed one keep every snapshot's count, scan and point reads in agreement and the counts exact")
    void racingStart(int round, @TempDir Path directory) throws Exception {
        var commits = new ConcurrentHashMap<Long, List<Object[]>>();
        try (var ledger = open(directory, true, round == 2 ? 0 : Long.MAX_VALUE)) {
            var first = ledger.newBatch();
            var firstWrites = new ArrayList<Object[]>();
            for (var message = 0; message < BULK; message++) {
                var value = value(message, 0, LENGTHS);
                first.put(MESSAGES, key(message), value);
                firstWrites.add(new Object[]{message, value});
            }
            commits.put(ledger.commit(first).sequence(), firstWrites);
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 2; t++) {
                var seed = round * 10 + t;
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed);
                    try {
                        while (!stop.get()) {
                            var batch = ledger.newBatch();
                            var writes = new ArrayList<Object[]>();
                            for (var i = 0; i < 1 + random.nextInt(4); i++) {
                                // The closed run's keys, the run beside's and new ones
                                var message = random.nextInt(4 * BULK);
                                if (random.nextInt(3) == 0) {
                                    batch.delete(MESSAGES, key(message));
                                    writes.add(new Object[]{message, null});
                                } else {
                                    var value = value(message, 100 + random.nextInt(50), LENGTHS);
                                    batch.put(MESSAGES, key(message), value);
                                    writes.add(new Object[]{message, value});
                                }
                            }
                            commits.put(ledger.commit(batch).sequence(), writes);
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    while (!stop.get()) {
                        try (var snapshot = ledger.snapshot()) {
                            var size = snapshot.size(MESSAGES);
                            var scanned = new ArrayList<byte[]>();
                            snapshot.forEach(MESSAGES, KeyRange.all(), (key, _) -> {
                                scanned.add(key);
                                return true;
                            });
                            assertEquals(size, scanned.size(), "a snapshot's count and scan disagree");
                            for (var i = 0; i < scanned.size(); i += 53) {
                                assertTrue(snapshot.get(MESSAGES, scanned.get(i)).isPresent(), "a scanned key reads absent");
                            }
                            var descending = new AtomicInteger();
                            snapshot.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (_, _) -> {
                                descending.incrementAndGet();
                                return true;
                            });
                            assertEquals(size, descending.get(), "a snapshot's count and descending scan disagree");
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            var started = new AtomicInteger();
            ledger.checkpointer().hookCutForTests(() -> {
                var batch = ledger.newBatch();
                var writes = new ArrayList<Object[]>();
                // Half the keys of the run before, half new
                var from = (1 + started.get()) * BULK / 2;
                for (var message = from; message < from + BULK; message++) {
                    var value = value(message, 1 + started.get(), LENGTHS);
                    batch.put(MESSAGES, key(message), value);
                    writes.add(new Object[]{message, value});
                }
                commits.put(ledger.commit(batch).sequence(), writes);
                var run = ledger.state(MESSAGES).run;
                if (run != null && run.older() != null) {
                    started.incrementAndGet();
                }
            });
            for (var checkpoint = 0; checkpoint < 3; checkpoint++) {
                ledger.checkpoint();
            }
            ledger.checkpointer().hookCutForTests(null);
            assertTrue(started.get() > 0, "no bulk commit started a run beside the closed one");
            Thread.sleep(50);
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            var expected = new Expected();
            for (var sequence : commits.keySet().stream().sorted().toList()) {
                for (var write : commits.get(sequence)) {
                    if (write[1] == null) {
                        expected.messages.remove((Integer) write[0]);
                    } else {
                        expected.messages.put((Integer) write[0], (byte[]) write[1]);
                    }
                }
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("counts racing the checkpoint that publishes a tree holding the closed run and lets go of it count each entry of both runs once")
    void countsRacingRelease(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            bulk(ledger, expected, 0, BULK, 0);
            smallPuts(ledger, expected, 3 * BULK, 20, 0);
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var counts = new AtomicInteger();
            var counters = new ArrayList<Thread>();
            var ran = atFirstCut(ledger, () -> {
                bulk(ledger, expected, BULK, BULK, 1);
                assertNotNull(ledger.state(MESSAGES).run.older());
                var size = expected.messages.size();
                for (var t = 0; t < 2; t++) {
                    counters.add(Thread.ofPlatform().start(() -> {
                        try {
                            while (!stop.get()) {
                                assertEquals(size, ledger.size(MESSAGES), "a count during the release");
                                assertEquals(size, ledger.range(MESSAGES, KeyRange.all(), Integer.MAX_VALUE).size(),
                                        "a range during the release");
                                counts.incrementAndGet();
                            }
                        } catch (Throwable throwable) {
                            failure.compareAndSet(null, throwable);
                        }
                    }));
                }
            });
            ledger.checkpoint();
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            var after = counts.get();
            while (counts.get() < after + 4 && failure.get() == null) {
                Thread.onSpinWait();
            }
            stop.set(true);
            for (var thread : counters) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            verify(ledger, expected);
            // A counter that read the trees before kept the closed run past the first prune; the next one lets it go
            ledger.commit(ledger.newBatch().put(TEXTS, "after", "counts")).awaitDurable();
            expected.texts.put("after", "counts");
            ledger.checkpoint();
            var run = ledger.state(MESSAGES).run;
            assertTrue(run == null || run.older() == null, "a run outlived every reader of a tree that lacks it");
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store closed without a checkpoint while a run beside the closed one held appends replays both runs' writes into one run, counted against the trees it opens from")
    void replayAfterBeside(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            bulk(ledger, expected, 0, BULK, 0);
            var ran = atFirstCut(ledger, () -> {
                bulk(ledger, expected, BULK, BULK, 1);
                bulk(ledger, expected, 2 * BULK, BULK, 2);
                assertNotNull(ledger.state(MESSAGES).run.older());
            });
            ledger.checkpoint();
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            for (var message = 0; message < 3 * BULK; message += 31) {
                ledger.commit(ledger.newBatch().delete(MESSAGES, key(message)));
                expected.messages.remove(message);
            }
            ledger.awaitDurable();
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        // A spill budget of 0 replays the whole log into a run
        try (var ledger = open(directory, encrypted, 0)) {
            var replayed = ledger.state(MESSAGES).run;
            assertNotNull(replayed, "the replay built no run");
            assertNull(replayed.older());
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a bulk commit after the checkpoint that closed a run published a tree of the key space puts keys only that tree holds on chains, counted as overwrites, never into the run beside")
    void besideRefusesOverTree(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            bulk(ledger, expected, 0, BULK, 0);
            // Keys on chains the checkpoint takes into its tree, which its prune then drops
            smallPuts(ledger, expected, 5 * BULK, 64, 0);
            var ran = atFirstCut(ledger, () -> bulk(ledger, expected, BULK, BULK, 1));
            ledger.checkpoint();
            assertTrue(ran.get());
            ledger.checkpointer().hookCutForTests(null);
            var beside = ledger.state(MESSAGES).run;
            assertNotNull(beside);
            var before = beside.size();
            // Keys only the tree holds now, with new ones
            var batch = ledger.newBatch();
            puts(batch, expected, 5 * BULK, 64, 2, LENGTHS);
            puts(batch, expected, 2 * BULK, BULK, 2, LENGTHS);
            ledger.commit(batch).awaitDurable();
            assertEquals(before, ledger.state(MESSAGES).run.size(), "a bulk commit appended to the run beside over a tree of the key space");
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a run beside the closed one is a bulk commits' run a cut closed, never the run of an open's replay")
    void besideNeedsClosedBulkRun() {
        var replay = new PackedRun.Builder();
        assertInstanceOf(IllegalArgumentException.class,
                assertThrows(RuntimeException.class, () -> PackedRun.Builder.beside(replay.build(null, null), 10, 10)));
        var committed = PackedRun.Builder.forCommit(1, 4);
        var segment = MemorySegment.ofArray(new byte[]{1, 2, 3, 4});
        committed.putHome(segment, Segments.hash(segment), Reads.stamp(1, 0), null, 0, 0);
        var open = committed.buildCommitted(null);
        assertThrows(IllegalArgumentException.class, () -> PackedRun.Builder.beside(open, 10, 10));
        open.closeToAppends();
        var beside = PackedRun.Builder.beside(open, 10, 10);
        assertTrue(beside.isEmpty());
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The freezes that move the published chains of a paged key space into a run between checkpoints ([RunFreezer]):
/// every read, scan, count, later write, range removal, snapshot, checkpoint, reopen and crash sees what the chains
/// showed, a count stays exact through blind writes the freeze hands over, the freezes that cannot be made are not, and
/// the segments a frozen value lies in stay until no reader needs them. The stores move every durable value off the
/// heap (a budget of 0) and freeze only when a test asks, but for the test of the automatic ones.
class RunFreezerTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // Empty, short, at the log's threshold and above it
    private static final int[] LENGTHS = {0, 40, 200, SpillQueue.LOG_VALUE_MIN, 700, 1_500};

    private static LiveLedger open(Path directory, boolean encrypted) {
        return open(directory, encrypted, KeyspaceState.NO_FREEZE);
    }

    private static LiveLedger open(Path directory, boolean encrypted, int freezeMin) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(Duration.ZERO)
                .checkpointEvery(1L << 40))
                .segmentSize(64 << 10)
                .spillBudget(0)
                .writeThrough(WriteThrough.Mode.OFF)
                .freezeMin(freezeMin)
                .build();
        ledger.valueSlab().forceColdForTests();
        ledger.state(MESSAGES).overwrites.forceSpillsForTests();
        ledger.state(TEXTS).overwrites.forceSpillsForTests();
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

    /// {@return a text key longer than a chain holds in its words}
    private static String textKey(int id) {
        return "text-key-longer-than-thirty-two-bytes-" + id;
    }

    private static String text(int id, int generation) {
        return "t" + id + "/" + generation + "x".repeat(LENGTHS[Math.floorMod(id + generation, LENGTHS.length)]);
    }

    /// What a store should hold.
    private static final class Expected {
        final TreeMap<Integer, byte[]> messages = new TreeMap<>();
        final TreeMap<String, String> texts = new TreeMap<>();

        Expected copy() {
            var copy = new Expected();
            copy.messages.putAll(messages);
            copy.texts.putAll(texts);
            return copy;
        }
    }

    /// Commits puts of the messages from `first` to `last`, excluded, every `step`, with a text each, and removals of
    /// those of `removed`; durable, with every value moved off the heap.
    private static void write(LiveLedger ledger, Expected expected, int first, int last, int step, int generation,
                              int... removed) throws InterruptedException {
        var batch = ledger.newBatch();
        for (var id = first; id < last; id += step) {
            var value = value(id, generation);
            batch.put(MESSAGES, key(id), value);
            expected.messages.put(id, value);
            var text = text(id, generation);
            batch.put(TEXTS, textKey(id), text);
            expected.texts.put(textKey(id), text);
        }
        for (var id : removed) {
            batch.delete(MESSAGES, key(id));
            expected.messages.remove(id);
            batch.delete(TEXTS, textKey(id));
            expected.texts.remove(textKey(id));
        }
        batch.put(CHATS, "c" + generation, "chat " + generation);
        ledger.commit(batch).awaitDurable();
        spillNow(ledger);
    }

    /// Moves every value of the durable commits off the heap, on this thread and the queue's.
    private static void spillNow(LiveLedger ledger) throws InterruptedException {
        var spills = ledger.spills();
        spills.drain(ledger.log()::durableSequence);
        spills.awaitIdleForTests();
    }

    private static int[] range(int from, int to) {
        var ids = new int[to - from];
        for (var i = 0; i < ids.length; i++) {
            ids[i] = from + i;
        }
        return ids;
    }

    /// Freezes both paged key spaces, checking that each freeze ran.
    private static RunFreezer.Outcome freeze(LiveLedger ledger) {
        var texts = ledger.freezeForTests(TEXTS);
        assertNotNull(texts);
        assertTrue(texts.started(), "the freeze of the texts did not start");
        var messages = ledger.freezeForTests(MESSAGES);
        assertNotNull(messages);
        assertTrue(messages.started(), "the freeze of the messages did not start");
        return messages;
    }

    /// Checks every way a view reads `expected`: point reads, encoded reads, scans both ways, pages and counts.
    private static void verify(View view, Expected expected) {
        for (var entry : expected.messages.entrySet()) {
            assertArrayEquals(entry.getValue(), view.get(MESSAGES, key(entry.getKey())).orElseThrow(),
                    () -> "message " + entry.getKey());
            assertArrayEquals(entry.getValue(), view.readEncoded(MESSAGES, key(entry.getKey()),
                    segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
        }
        for (var entry : expected.texts.entrySet()) {
            assertEquals(entry.getValue(), view.get(TEXTS, entry.getKey()).orElseThrow(), "text " + entry.getKey());
        }
        assertEquals(expected.messages.size(), view.size(MESSAGES), "messages counted");
        assertEquals(expected.texts.size(), view.size(TEXTS), "texts counted");
        var ascending = new ArrayList<Integer>();
        view.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            var id = ByteBuffer.wrap(key).getInt();
            assertArrayEquals(expected.messages.get(id), value, "message " + id + " scanned");
            ascending.add(id);
            return true;
        });
        assertEquals(new ArrayList<>(expected.messages.keySet()), ascending, "messages scanned");
        var descending = new ArrayList<Integer>();
        view.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (key, _) -> descending.add(ByteBuffer.wrap(key).getInt()));
        assertEquals(new ArrayList<>(expected.messages.descendingKeySet()), descending, "messages scanned descending");
        var page = view.range(MESSAGES, KeyRange.between(key(100), key(200)).reversed(), 13);
        var wanted = new ArrayList<>(expected.messages.subMap(100, true, 200, false).descendingKeySet());
        assertEquals(Math.min(13, wanted.size()), page.size());
        for (var i = 0; i < page.size(); i++) {
            assertEquals(wanted.get(i), ByteBuffer.wrap(page.get(i).key()).getInt());
        }
        var texts = new TreeMap<String, String>();
        view.forEach(TEXTS, KeyRange.<String>all().reversed(), (key, value) -> {
            texts.put(key, value);
            return true;
        });
        assertEquals(expected.texts, texts, "texts scanned");
    }

    /// Writes a checkpointed history of messages 0 to 300 and texts, so later writes of keys memory lacks are blind.
    private static Expected history(LiveLedger ledger) throws InterruptedException {
        var expected = new Expected();
        for (var first = 0; first < 300; first += 60) {
            write(ledger, expected, first, first + 60, 1, 0);
        }
        ledger.checkpoint();
        assertEquals(0, ledger.state(MESSAGES).chains(), "the checkpoint left chains");
        return expected;
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("frozen writes, blind ones among them, read and count as their chains did, through overwrites, removals, more freezes, a checkpoint and a reopen")
    void freezesAndReads(boolean encrypted, @TempDir Path directory) throws Exception {
        Expected expected;
        try (var ledger = open(directory, encrypted)) {
            expected = history(ledger);
            // Overwrites and removals of keys the tree holds, removals of keys it lacks, and new keys, all blind
            write(ledger, expected, 100, 400, 2, 1, range(20, 40));
            write(ledger, expected, 400, 450, 1, 1, range(500, 520));
            var state = ledger.state(MESSAGES);
            var chains = state.chains();
            var listed = state.blind.listed();
            assertTrue(listed > 0, "no write was blind");
            var outcome = freeze(ledger);
            assertTrue(outcome.dropped() > 0 && outcome.dropped() == outcome.candidates(), outcome.toString());
            assertEquals(outcome.dropped(), outcome.handed(), "every frozen chain was a blind write");
            assertTrue(state.run.frozen());
            assertEquals(chains - outcome.dropped(), state.chains());
            assertTrue(state.blind.listed() < listed, "the blind writes stayed listed");
            verify(ledger, expected);
            // Writes of frozen keys find their entries: overwrites, removals, and keys of the run written again
            write(ledger, expected, 101, 160, 2, 2, range(100, 110));
            write(ledger, expected, 600, 640, 1, 2, 401, 403, 405);
            verify(ledger, expected);
            var second = freeze(ledger);
            assertTrue(second.dropped() > 0, second.toString());
            verify(ledger, expected);
            try (var snapshot = ledger.snapshot()) {
                var seen = expected.copy();
                write(ledger, expected, 700, 720, 1, 3, range(130, 150));
                verify(snapshot, seen);
                ledger.checkpoint();
                verify(snapshot, seen);
            }
            ledger.commit(ledger.newBatch().put(CHATS, "after", "close")).awaitDurable();
            verify(ledger, expected);
            assertNull(state.run, "the checkpoint kept the frozen run");
        }
        try (var ledger = open(directory, encrypted)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a count made once the frozen run is published, before any chain is dropped, and those made after drops with and without blind writes handed over, are exact; and the checkpoint's own count agrees")
    void countsAcrossThePublication(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            write(ledger, expected, 150, 450, 1, 1, range(0, 30));
            write(ledger, expected, 450, 460, 1, 1, range(460, 480));
            var counted = new AtomicReference<Long>();
            ledger.freezerForTests().publishedHookForTests(() -> counted.set(ledger.size(MESSAGES)));
            var outcome = ledger.freezeForTests(MESSAGES);
            ledger.freezerForTests().publishedHookForTests(null);
            assertEquals(outcome.candidates(), outcome.dropped(), outcome.toString());
            // The count settled every blind write, which stays listed
            assertEquals(0, outcome.handed(), outcome.toString());
            assertEquals(expected.messages.size(), counted.get(), "counted between the publication and the drops");
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            // Blind writes no count settled, handed over: counted once the drops are published
            write(ledger, expected, 1_000, 1_100, 1, 2, range(30, 60));
            var second = ledger.freezeForTests(MESSAGES);
            assertEquals(second.dropped(), second.handed(), second.toString());
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("blind writes a count settled before the freeze stay listed, and their chains' entries count nothing more")
    void settledBlindWritesStay(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false)) {
            var expected = history(ledger);
            write(ledger, expected, 200, 350, 1, 1, range(50, 80));
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            var outcome = ledger.freezeForTests(MESSAGES);
            assertTrue(outcome.dropped() > 0, outcome.toString());
            assertEquals(0, outcome.handed(), "a settled blind write was handed over");
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a blind put of a key the tree lacks counts once through its frozen entry, after a removal frozen over it, and after the checkpoint")
    void blindPutUnderLaterRemoval(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            write(ledger, expected, 1_000, 1_100, 1, 1);
            ledger.freezeForTests(TEXTS);
            var first = ledger.freezeForTests(MESSAGES);
            assertEquals(100, first.handed(), first.toString());
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            // Exact against the run's entries: the removals are counted as they commit
            write(ledger, expected, 0, 0, 1, 2, range(1_000, 1_050));
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            var second = ledger.freezeForTests(MESSAGES);
            assertEquals(50, second.dropped(), second.toString());
            assertEquals(0, second.handed());
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a chain whose blind first write a checkpoint took, cut by its prune, freezes with its later version alone, and the write's slot keeps its count")
    void pruneCutBlindFirstWrite() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new Gate();
        disk.intercept(gate);
        try (var ledger = open(disk.getPath("/store"), true)) {
            var expected = history(ledger);
            // Blind: memory holds none of these keys
            write(ledger, expected, 250, 350, 1, 1, range(0, 20));
            gate.arm();
            var checkpoint = Thread.ofPlatform().start(ledger::checkpoint);
            gate.awaitBlocked();
            // After the cut, before the prune: the chains keep these, and the prune cuts the blind first writes
            write(ledger, expected, 250, 350, 1, 2, range(5, 15));
            gate.release();
            checkpoint.join();
            var state = ledger.state(MESSAGES);
            assertTrue(state.chains() > 0, "the prune dropped the chains written after the cut");
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            var outcome = ledger.freezeForTests(MESSAGES);
            assertTrue(outcome.dropped() > 0, outcome.toString());
            assertEquals(0, outcome.handed(), "a blind write a checkpoint took was handed over");
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a chain a commit writes again between the publication and the drops stays, shadows its entry, and is frozen with its blind write by the next freeze")
    void lostDropRace(boolean encrypted, @TempDir Path directory) throws Exception {
        Expected reopened;
        try (var ledger = open(directory, encrypted)) {
            var expected = history(ledger);
            write(ledger, expected, 280, 380, 1, 1, range(30, 40));
            var failure = new AtomicReference<Throwable>();
            ledger.freezerForTests().publishedHookForTests(() -> {
                try {
                    // Writes of 20 kept keys and 2 removed ones the freeze took, whose chains then stay
                    write(ledger, expected, 300, 320, 1, 5);
                    write(ledger, expected, 35, 37, 1, 5);
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            var outcome = ledger.freezeForTests(MESSAGES);
            ledger.freezerForTests().publishedHookForTests(null);
            assertNull(failure.get());
            assertEquals(outcome.candidates() - 22, outcome.dropped(), outcome.toString());
            assertEquals(outcome.dropped(), outcome.handed());
            // No count before the next freeze, which would settle the blind writes the chains that stayed keep
            var second = ledger.freezeForTests(MESSAGES);
            assertEquals(22, second.dropped(), second.toString());
            assertEquals(22, second.handed(), "the blind writes of the chains that stayed were not handed over");
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
            reopened = expected;
        }
        try (var ledger = open(directory, encrypted)) {
            verify(ledger, reopened);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a count and the folds of the publisher between two holds of the drops, once blind writes were handed over, count each write once, and the checkpoint agrees")
    void foldBetweenDrops(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            // Keys memory and the tree lack, all blind: three holds of drops, the first handing its writes over
            write(ledger, expected, 1_000, 1_768, 1, 1);
            var counted = new ArrayList<Long>();
            var failure = new AtomicReference<Throwable>();
            var holds = new int[1];
            ledger.freezerForTests().droppedHookForTests(() -> {
                if (holds[0]++ != 0) {
                    return;
                }
                try {
                    // Settles every listed write, those the first hold handed over among them
                    counted.add(ledger.size(MESSAGES));
                    // Enough passes of the publisher for it to maintain the store twice, folding what it may
                    for (var i = 0; i < 40; i++) {
                        ledger.commit(ledger.newBatch().put(CHATS, "fold", "pass " + i));
                    }
                    counted.add(ledger.size(MESSAGES));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            var outcome = ledger.freezeForTests(MESSAGES);
            ledger.freezerForTests().droppedHookForTests(null);
            assertNull(failure.get());
            assertEquals(768, outcome.dropped(), outcome.toString());
            assertEquals(256, outcome.handed(), "the first hold did not hand its writes over, or a later one did");
            var size = (long) expected.messages.size();
            assertEquals(List.of(size, size), counted, "counted between the drops");
            assertEquals(size, ledger.size(MESSAGES));
            ledger.commit(ledger.newBatch().put(CHATS, "fold", "after"));
            assertEquals(size, ledger.size(MESSAGES));
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("frozen runs whose long keys fill several chunks of the keys, one merged into the next, read, scan, count, checkpoint and reopen as the chains did")
    void keysOverChunks(@TempDir Path directory) throws Exception {
        Expected reopened;
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            // Keys of 700 to 1,023 bytes, so most chunks end with a key that did not fit their rest
            writeLongTexts(ledger, expected, 0, 400, 1);
            var first = ledger.freezeForTests(TEXTS);
            assertEquals(400, first.dropped(), first.toString());
            assertTrue(ledger.state(TEXTS).run.keyChunkLengthsForTests().length >= 2, "the keys took one chunk");
            verify(ledger, expected);
            // New keys between and after the frozen ones, and overwrites of frozen ones, merged with the run
            writeLongTexts(ledger, expected, 200, 700, 2);
            var second = ledger.freezeForTests(TEXTS);
            assertEquals(500, second.dropped(), second.toString());
            var lengths = ledger.state(TEXTS).run.keyChunkLengthsForTests();
            assertTrue(lengths.length >= 3, "the merged keys took fewer than three chunks");
            for (var i = 0; i < lengths.length - 1; i++) {
                assertEquals(PackedRun.KEY_CHUNK, lengths[i], "a chunk before the last is not whole");
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
            reopened = expected;
        }
        try (var ledger = open(directory, true)) {
            verify(ledger, reopened);
        }
    }

    /// Commits texts from `first` to `last`, excluded, under keys of 700 to 1,023 bytes; durable, with every value moved
    /// off the heap.
    private static void writeLongTexts(LiveLedger ledger, Expected expected, int first, int last, int generation)
            throws InterruptedException {
        var batch = ledger.newBatch();
        for (var id = first; id < last; id++) {
            var key = "%05d".formatted(id) + "k".repeat(695 + (id * 37) % 324);
            var text = text(id, generation);
            batch.put(TEXTS, key, text);
            expected.texts.put(key, text);
        }
        ledger.commit(batch).awaitDurable();
        spillNow(ledger);
    }

    @ParameterizedTest(name = "after the publication: {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    @DisplayName("a checkpoint that waits for the lock while a freeze holds it stops the drops, takes the chains the freeze left beside their entries, and every read and count stays exact")
    void checkpointStopsTheDrops(boolean published, @TempDir Path directory) throws Exception {
        Expected reopened;
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            // Keys memory and the tree lack, all blind: three holds of drops
            write(ledger, expected, 1_000, 1_768, 1, 1);
            var state = ledger.state(MESSAGES);
            var chains = state.chains();
            var failure = new AtomicReference<Throwable>();
            var checkpoint = new AtomicReference<Thread>();
            Runnable waitingCheckpoint = () -> {
                if (checkpoint.get() != null) {
                    return;
                }
                checkpoint.set(Thread.ofPlatform().start(() -> {
                    try {
                        ledger.checkpoint();
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                }));
                // Until the checkpoint waits for the lock the freeze holds
                while (!ledger.checkpointer().wanted()) {
                    Thread.onSpinWait();
                }
            };
            if (published) {
                ledger.freezerForTests().publishedHookForTests(waitingCheckpoint);
            } else {
                ledger.freezerForTests().droppedHookForTests(waitingCheckpoint);
            }
            var outcome = ledger.freezeForTests(MESSAGES);
            ledger.freezerForTests().publishedHookForTests(null);
            ledger.freezerForTests().droppedHookForTests(null);
            checkpoint.get().join();
            assertNull(failure.get());
            assertTrue(outcome.started(), outcome.toString());
            assertEquals(768, outcome.candidates(), outcome.toString());
            // Published: no drop; one hold: its 256 chains and their blind writes, and no more
            assertEquals(published ? 0 : 256, outcome.dropped(), outcome.toString());
            assertEquals(outcome.dropped(), outcome.handed(), outcome.toString());
            assertEquals(0, state.chains(), "the checkpoint left chains, of " + chains);
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.commit(ledger.newBatch().put(CHATS, "after", "checkpoint")).awaitDurable();
            assertNull(state.run, "the checkpoint kept the frozen run");
            verify(ledger, expected);
            reopened = expected;
        }
        try (var ledger = open(directory, true)) {
            verify(ledger, reopened);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a blind put queued behind a pipelined blind removal of the same key that is taken back freezes as a put, and the count stays exact")
    void rolledBackBlindRemoval(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false)) {
            var expected = history(ledger);
            var abort = new IllegalStateException("abort");
            var removing = new AtomicReference<Thread>();
            var removal = new AtomicLong(-1);
            var queued = new CountDownLatch(1);
            var putting = new AtomicReference<Thread>();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var put = value(10, 7);
            ledger.hookSequencedForTests(sequence -> {
                if (Thread.currentThread() != removing.get()) {
                    queued.countDown();
                    return;
                }
                removal.set(sequence);
                // Sequenced behind the removal on the key, it applies once the removal was taken back
                putting.set(Thread.ofPlatform().start(() -> {
                    try {
                        ledger.commit(ledger.newBatch().put(MESSAGES, key(10), put)).awaitDurable();
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                }));
                try {
                    assertTrue(queued.await(30, TimeUnit.SECONDS), "the put was not sequenced");
                } catch (InterruptedException exception) {
                    throw new IllegalStateException(exception);
                }
            });
            ledger.hookAppliedForTests(sequence -> {
                if (sequence == removal.get()) {
                    throw abort;
                }
            });
            var thrown = new AtomicReference<Throwable>();
            var thread = Thread.ofPlatform().unstarted(() -> {
                try {
                    ledger.commit(ledger.newBatch().delete(MESSAGES, key(10)));
                } catch (Throwable throwable) {
                    thrown.set(throwable);
                }
            });
            removing.set(thread);
            thread.start();
            thread.join();
            while (putting.get() == null) {
                Thread.onSpinWait();
            }
            putting.get().join();
            ledger.hookSequencedForTests(null);
            ledger.hookAppliedForTests(null);
            assertSame(abort, thrown.get(), "the removal was not taken back");
            assertTrue(failures.isEmpty(), failures.toString());
            expected.messages.put(10, put);
            spillNow(ledger);
            // No count before the freeze, which would settle the put
            var outcome = ledger.freezeForTests(MESSAGES);
            assertEquals(1, outcome.dropped(), outcome.toString());
            assertEquals(1, outcome.handed(), outcome.toString());
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("range removals hide the frozen entries older than them, a key written after one is not frozen until the next checkpoint, and every count of a range is exact")
    void rangeRemovals(@TempDir Path directory) throws Exception {
        Expected expected;
        try (var ledger = open(directory, true)) {
            expected = history(ledger);
            write(ledger, expected, 200, 400, 1, 1);
            freeze(ledger);
            // A removal over frozen entries, the tree's keys and chains' keys
            write(ledger, expected, 400, 420, 1, 2);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(150), key(410)))).awaitDurable();
            expected.messages.subMap(150, 410).clear();
            verify(ledger, expected);
            // Keys of the removed range written again: newer than the removal, so not frozen
            write(ledger, expected, 160, 240, 4, 3);
            var state = ledger.state(MESSAGES);
            var before = state.chains();
            var outcome = ledger.freezeForTests(MESSAGES);
            assertTrue(outcome.started());
            assertEquals(before, state.chains() + outcome.dropped());
            for (var id = 160; id < 240; id += 4) {
                assertNotNull(state.chain(MemorySegment.ofArray(key(id))), "a key written after a range removal was frozen");
            }
            verify(ledger, expected);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(100), key(170)))).awaitDurable();
            expected.messages.subMap(100, 170).clear();
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, true)) {
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a scan that holds the run before a freeze keeps the freeze from dropping its chains, and returns every key")
    void scanDuringFreeze(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false)) {
            var expected = history(ledger);
            write(ledger, expected, 150, 450, 1, 1);
            var outcome = new AtomicReference<RunFreezer.Outcome>();
            var seen = new ArrayList<Integer>();
            ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                var id = ByteBuffer.wrap(key).getInt();
                if (seen.isEmpty()) {
                    var thread = Thread.ofPlatform().start(() -> outcome.set(ledger.freezeForTests(MESSAGES)));
                    try {
                        thread.join();
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    }
                }
                assertArrayEquals(expected.messages.get(id), value);
                seen.add(id);
                return true;
            });
            assertEquals(new ArrayList<>(expected.messages.keySet()), seen);
            assertTrue(outcome.get().candidates() > 0);
            assertEquals(0, outcome.get().dropped(), "a freeze dropped chains a scan had yet to reach");
            verify(ledger, expected);
            // Once no scan holds it, the next freeze drops them
            var next = ledger.freezeForTests(MESSAGES);
            assertEquals(next.candidates(), next.dropped(), next.toString());
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a scan that pauses briefly as a freeze publishes its run returns every key, and the freeze drops its chains once it ends")
    void briefScanDuringFreeze(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            write(ledger, expected, 150, 450, 1, 1);
            var published = new CountDownLatch(1);
            ledger.freezerForTests().publishedHookForTests(published::countDown);
            var outcome = new AtomicReference<RunFreezer.Outcome>();
            var seen = new ArrayList<Integer>();
            var freezer = new AtomicReference<Thread>();
            ledger.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (key, _) -> {
                if (seen.isEmpty()) {
                    freezer.set(Thread.ofPlatform().start(() -> outcome.set(ledger.freezeForTests(MESSAGES))));
                    try {
                        assertTrue(published.await(10, TimeUnit.SECONDS));
                        // Well within the time a freeze waits for the scans
                        Thread.sleep(5);
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    }
                }
                seen.add(ByteBuffer.wrap(key).getInt());
                return true;
            });
            freezer.get().join();
            assertEquals(new ArrayList<>(expected.messages.descendingKeySet()), seen);
            assertEquals(outcome.get().candidates(), outcome.get().dropped(), outcome.get().toString());
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("the count of a range removal deferred past a freeze counts the frozen keys as the chains it registered against held them")
    void deferredRangeCount(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            write(ledger, expected, 150, 450, 1, 1);
            var counts = new ConcurrentLinkedQueue<Runnable>();
            ledger.rangeCountsForTests(counts::add);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(100), key(330)))).awaitDurable();
            expected.messages.subMap(100, 330).clear();
            assertEquals(1, counts.size(), "the count of the removal was not deferred");
            var outcome = ledger.freezeForTests(MESSAGES);
            assertTrue(outcome.dropped() > 0, outcome.toString());
            counts.poll().run();
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a snapshot older than the writes keeps them from freezing, and sees none of them; once it closes they freeze")
    void snapshotOlderThanTheWrites(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false)) {
            var expected = history(ledger);
            // Past the checkpoint's publication: a reader at its very sequence may read the tree before
            ledger.commit(ledger.newBatch().put(CHATS, "after", "checkpoint")).awaitDurable();
            var seen = expected.copy();
            var snapshot = ledger.snapshot();
            write(ledger, expected, 300, 400, 1, 1);
            var outcome = ledger.freezeForTests(MESSAGES);
            assertTrue(outcome.started());
            assertEquals(0, outcome.candidates(), "a write newer than a reader froze");
            verify(snapshot, seen);
            verify(ledger, expected);
            snapshot.close();
            var next = ledger.freezeForTests(MESSAGES);
            assertEquals(100, next.dropped(), next.toString());
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("no freeze runs while a reader holds a tree older than the latest, even one whose bound is above the writes and a range removal between the two trees; nor while a run the latest tree took waits for such a reader")
    void readersOfOlderTrees() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new Gate();
        disk.intercept(gate);
        try (var ledger = open(disk.getPath("/store"), true)) {
            var expected = history(ledger);
            write(ledger, expected, 300, 400, 1, 1);
            ledger.freezeForTests(TEXTS);
            assertTrue(ledger.freezeForTests(MESSAGES).dropped() > 0);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(350), key(380)))).awaitDurable();
            expected.messages.subMap(350, 380).clear();
            gate.arm();
            var checkpoint = Thread.ofPlatform().start(ledger::checkpoint);
            gate.awaitBlocked();
            // After the cut, before the publication: the snapshot reads the tree before, above these writes
            write(ledger, expected, 355, 375, 1, 2);
            var snapshot = ledger.snapshot();
            var seen = expected.copy();
            gate.release();
            checkpoint.join();
            var outcome = ledger.freezeForTests(MESSAGES);
            assertFalse(outcome.started(), "a freeze ran under a reader of the tree before");
            verify(snapshot, seen);
            verify(ledger, expected);
            snapshot.close();
            ledger.commit(ledger.newBatch().put(CHATS, "next", "commit")).awaitDurable();
            assertNull(ledger.state(MESSAGES).run, "the taken run stayed");
            var next = ledger.freezeForTests(MESSAGES);
            assertTrue(next.started());
            assertEquals(20, next.dropped(), next.toString());
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a transaction reads, writes and removes frozen keys as it would their chains")
    void transactionsOverFrozenKeys(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            write(ledger, expected, 200, 400, 1, 1);
            freeze(ledger);
            ledger.update(transaction -> {
                assertArrayEquals(expected.messages.get(250), transaction.get(MESSAGES, key(250)).orElseThrow());
                transaction.put(MESSAGES, key(251), new byte[]{1, 2, 3});
                transaction.delete(MESSAGES, key(252));
                transaction.delete(MESSAGES, key(9_999));
                var keys = new ArrayList<Integer>();
                transaction.forEach(MESSAGES, KeyRange.between(key(240), key(260)), (key, _) -> keys.add(ByteBuffer.wrap(key).getInt()));
                var seen = new ArrayList<>(expected.messages.subMap(240, true, 260, false).keySet());
                // The scan reads the transaction's own writes
                seen.remove(Integer.valueOf(252));
                assertEquals(seen, keys);
            }).awaitDurable();
            expected.messages.put(251, new byte[]{1, 2, 3});
            expected.messages.remove(252);
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("counts of snapshots taken while commits write blind and freezes run each equal the entries at their sequence")
    void concurrentCounts(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            var counts = new TreeMap<Long, Integer>();
            try (var snapshot = ledger.snapshot()) {
                counts.put(snapshot.sequence(), expected.messages.size());
            }
            var observed = new ConcurrentLinkedQueue<long[]>();
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var readers = new ArrayList<Thread>();
            for (var reader = 0; reader < 3; reader++) {
                readers.add(Thread.ofPlatform().start(() -> {
                    while (!stop.get()) {
                        try (var snapshot = ledger.snapshot()) {
                            observed.add(new long[]{snapshot.sequence(), snapshot.size(MESSAGES)});
                        } catch (Throwable throwable) {
                            failure.set(throwable);
                            return;
                        }
                    }
                }));
            }
            var freezer = Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    try {
                        ledger.freezeForTests(MESSAGES);
                        Thread.sleep(1);
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                        return;
                    }
                }
            });
            var random = new Random(7);
            for (var round = 0; round < 60; round++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 20; i++) {
                    var id = random.nextInt(600);
                    if (random.nextInt(3) == 0) {
                        batch.delete(MESSAGES, key(id));
                        expected.messages.remove(id);
                    } else {
                        var value = value(id, round);
                        batch.put(MESSAGES, key(id), value);
                        expected.messages.put(id, value);
                    }
                }
                var sequence = ledger.commit(batch).sequence();
                synchronized (counts) {
                    counts.put(sequence, expected.messages.size());
                }
                spillNow(ledger);
            }
            stop.set(true);
            freezer.join();
            for (var reader : readers) {
                reader.join();
            }
            assertNull(failure.get());
            for (var seen : observed) {
                var at = counts.floorEntry(seen[0]);
                assertNotNull(at);
                assertEquals((long) at.getValue(), seen[1], "the count at sequence " + seen[0]);
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a store that freezes on its own, as its key spaces list chains, reads, counts, checkpoints and reopens as one that never froze")
    void automaticFreezes(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, 64)) {
            for (var round = 0; round < 3; round++) {
                for (var first = 0; first < 600; first += 50) {
                    write(ledger, expected, first + round * 1_000, first + round * 1_000 + 50, 1, round, round * 1_000 + first / 2);
                }
                assertTrue(awaitFreezes(ledger, round + 1), "the store froze nothing on its own");
                verify(ledger, expected);
                if (round == 1) {
                    ledger.checkpoint();
                    verify(ledger, expected);
                }
            }
        }
        try (var ledger = open(directory, true, 64)) {
            verify(ledger, expected);
        }
    }

    private static boolean awaitFreezes(LiveLedger ledger, long freezes) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (ledger.freezerForTests().freezesForTests() < freezes) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            ledger.commit(ledger.newBatch().put(CHATS, "tick", "x")).awaitDurable();
            Thread.sleep(5);
        }
        return true;
    }

    @Test
    @DisplayName("a store that crashed after freezes recovers every write from its log")
    void crashAfterFreezes() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Expected expected;
        try (var ledger = open(directory, true)) {
            expected = history(ledger);
            write(ledger, expected, 150, 450, 1, 1, range(0, 40));
            freeze(ledger);
            write(ledger, expected, 200, 260, 1, 2, range(400, 420));
            freeze(ledger);
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        var crashed = disk.afterPowerLoss(new Random(3)).getPath("/store");
        try (var ledger = open(crashed, true)) {
            verify(ledger, expected);
            // The runs of the replay lie under no freeze until the checkpoint that takes them
            write(ledger, expected, 500, 520, 1, 3);
            assertFalse(ledger.freezeForTests(MESSAGES).started(), "a freeze ran over the run of the replay");
            ledger.checkpoint();
            ledger.commit(ledger.newBatch().put(CHATS, "after", "checkpoint")).awaitDurable();
            write(ledger, expected, 520, 540, 1, 4, 501);
            // 20 new keys and the removal of one the checkpoint took
            assertEquals(21, freeze(ledger).dropped());
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "frozen: {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(120)
    @DisplayName("values moved to segments after a checkpoint, left pending through idle ticks that retire what it held back, are read by the closing checkpoint and after a reopen")
    void idleRetirementKeepsPendingSegments(boolean frozen, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true)) {
            for (var first = 0; first < 1_200; first += 40) {
                write(ledger, expected, first, first + 40, 1, 0);
            }
            var snapshot = ledger.snapshot();
            write(ledger, expected, 2_000, 2_040, 1, 0);
            ledger.checkpoint();
            // Pending: written after the checkpoint's cut, their values in the segments after it
            for (var first = 3_000; first < 3_200; first += 40) {
                write(ledger, expected, first, first + 40, 1, 1, first - 3_000 + 7);
            }
            snapshot.close();
            if (frozen) {
                assertTrue(freeze(ledger).dropped() > 0);
            }
            for (var tick = 0; tick < 5; tick++) {
                ledger.checkpointer().tick();
            }
            assertFalse(ledger.checkpointer().startedForTests(), "an idle tick checkpointed the pending writes");
            assertPendingSegmentsExist(ledger, directory);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, true)) {
            verify(ledger, expected);
        }
    }

    /// Checks that every log segment a value of a pending chain, one no tree holds yet, or of a frozen run lies in is
    /// still there, unretired; the chains the sweep has yet to drop for the trees that hold them may read the trees.
    private static void assertPendingSegmentsExist(LiveLedger ledger, Path directory) {
        var tree = ledger.tree();
        var treeBound = tree == null ? -1 : Reads.boundOf(tree.sequence);
        for (var keyspace : List.of(MESSAGES, TEXTS)) {
            var state = ledger.state(keyspace);
            var walk = state.walk(null, null, false);
            while (walk.next()) {
                var head = walk.chain().head;
                var logged = head == null || head.stamp <= treeBound ? null : head.logValue();
                if (logged != null) {
                    assertSegment(directory, logged.segment());
                }
            }
            var run = state.run;
            for (var entry = 0; run != null && entry < run.size(); entry++) {
                if (run.sourceForTests(entry) instanceof LogValue logged) {
                    assertSegment(directory, logged.segment());
                }
            }
        }
    }

    private static void assertSegment(Path directory, LogSegmentView segment) {
        assertFalse(segment.retired(), "a pending value's segment " + segment.number() + " retired");
        assertTrue(Files.exists(directory.resolve("log").resolve(LogFiles.segmentName(segment.number()))),
                "a pending value's segment " + segment.number() + " is gone");
    }

    @Test
    @DisplayName("a freeze of chains whose values lie in many segments reads them back after the segments of older checkpoints retire")
    void frozenValuesAcrossRetirements(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true)) {
            var expected = history(ledger);
            for (var round = 0; round < 4; round++) {
                for (var first = 300; first < 900; first += 100) {
                    write(ledger, expected, first, first + 100, 1, round + 1);
                }
                freeze(ledger);
                verify(ledger, expected);
                ledger.checkpoint();
                ledger.checkpointer().tick();
                verify(ledger, expected);
            }
        }
    }

    @Test
    @DisplayName("the merge of a frozen run keeps the entries of the run before, takes the chains' in their place, and shares the run's chunks")
    void mergeKeepsTheRunBefore(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, false)) {
            var expected = history(ledger);
            write(ledger, expected, 300, 400, 1, 1);
            ledger.freezeForTests(MESSAGES);
            var state = ledger.state(MESSAGES);
            var first = state.run;
            assertEquals(100, first.size());
            write(ledger, expected, 350, 450, 1, 2);
            ledger.freezeForTests(MESSAGES);
            var second = state.run;
            assertEquals(150, second.size());
            for (var position = 1; position < second.size(); position++) {
                assertTrue(Segments.compare(second.key(second.entryAt(position - 1)), second.key(second.entryAt(position))) < 0);
            }
            for (var id = 300; id < 450; id++) {
                var entry = second.find(MemorySegment.ofArray(key(id)));
                assertTrue(entry >= 0);
                if (expected.messages.get(id).length > 0) {
                    assertArrayEquals(expected.messages.get(id), second.valueBytes(entry), "message " + id);
                }
            }
            var kept = second.find(MemorySegment.ofArray(key(310)));
            var before = first.find(MemorySegment.ofArray(key(310)));
            if (first.sourceForTests(before) instanceof MemorySegment chunk) {
                assertSame(chunk, second.sourceForTests(kept), "the merge copied a value of the run before");
            }
            verify(ledger, expected);
        }
    }

    /// Blocks the first write of the data file once armed, until released: a checkpoint then waits between its cut and
    /// the publication of its trees.
    private static final class Gate implements ChangeInterceptor {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void arm() {
            armed.set(true);
        }

        void awaitBlocked() throws InterruptedException {
            assertTrue(blocked.await(30, TimeUnit.SECONDS), "the checkpoint wrote no page");
        }

        void release() {
            released.countDown();
        }

        @Override
        public void beforeChange(Path path, ChangeKind kind) {
            if (kind == ChangeKind.WRITE && path.getFileName() != null && path.getFileName().toString().equals(PageFile.FILE)
                    && armed.compareAndSet(true, false)) {
                blocked.countDown();
                try {
                    released.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

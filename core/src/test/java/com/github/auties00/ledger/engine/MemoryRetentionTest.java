package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// What memory a store keeps reachable: the arrays of the batches resident values came in, and how keys that differ
/// only where a hash could be made to cancel spread over the index.
class MemoryRetentionTest {
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());

    @Test
    @DisplayName("a resident byte array is held once; another resident value takes an array of its own beside large values, and shares its batch's alone")
    void residentValueOwnsItsBytes(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).keyspace(NAMES).build()) {
            var session = new byte[1_500];
            session[7] = 7;
            var name = "n".repeat(1_500);
            var batch = ledger.newBatch().put(SESSIONS, new byte[]{1}, session).put(NAMES, new byte[]{1}, name);
            for (var i = 0; i < 10; i++) {
                batch.put(MESSAGES, new byte[]{2, (byte) i}, new byte[4_096]);
            }
            ledger.commit(batch).awaitDurable();
            ledger.commit(ledger.newBatch().put(NAMES, new byte[]{9}, name)).awaitDurable();
            // A byte array is its own encoding: the store keeps one copy, which reads return and checkpoints write
            var held = head(ledger, SESSIONS, new byte[]{1});
            assertEquals(1_500, held.sourceBytesForTests());
            assertTrue(held.decoded != session, "the store holds the caller's array");
            assertEquals(7, ledger.get(SESSIONS, new byte[]{1}).orElseThrow()[7]);
            // Another value would keep the 40 KiB of messages reachable on the batch's array
            assertEquals(1_500, head(ledger, NAMES, new byte[]{1}).sourceBytesForTests());
            // Alone in its batch, it shares the batch's array, which holds little else
            var alone = head(ledger, NAMES, new byte[]{9}).sourceBytesForTests();
            assertTrue(alone > 1_500 && alone <= 2 * 1_500 + 64, "a value alone in its batch keeps " + alone + " bytes");
        }
    }

    @Test
    @DisplayName("a paged value is copied, alone, off a batch that holds other bytes, into arrays below half a G1 region; one alone in its batch shares it")
    void pagedValueLeavesItsBatch(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).build()) {
            ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, new byte[1_500]).put(MESSAGES, new byte[]{1}, new byte[1_500]))
                    .awaitDurable();
            // The session's bytes, which the store holds a copy of, are not kept for the message
            assertEquals(1_500, head(ledger, MESSAGES, new byte[]{1}).sourceBytesForTests());
            ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{2}, new byte[1_500])).awaitDurable();
            assertEquals(1_501, head(ledger, MESSAGES, new byte[]{2}).sourceBytesForTests());
            var batch = ledger.newBatch().put(SESSIONS, new byte[]{3}, new byte[64]);
            for (var i = 0; i < 400; i++) {
                batch.put(MESSAGES, new byte[]{3, (byte) (i >>> 8), (byte) i}, new byte[2_000]);
            }
            batch.put(MESSAGES, new byte[]{4}, new byte[ArrayWriteBatch.CHUNK_BYTES]);
            ledger.commit(batch).awaitDurable();
            var chunks = new HashSet<Long>();
            for (var i = 0; i < 400; i++) {
                var bytes = head(ledger, MESSAGES, new byte[]{3, (byte) (i >>> 8), (byte) i}).sourceBytesForTests();
                assertTrue(bytes <= ArrayWriteBatch.CHUNK_BYTES, "a value of a large batch keeps " + bytes + " bytes");
                chunks.add(bytes);
            }
            assertTrue(chunks.size() >= 2, "800,000 bytes of values took one array");
            // A value of half a chunk or more takes an array of its own
            assertEquals(ArrayWriteBatch.CHUNK_BYTES, head(ledger, MESSAGES, new byte[]{4}).sourceBytesForTests());
        }
    }

    @Test
    @DisplayName("the values a commit keeps, and the bounds of its range removal, survive the pool handing its batch's array to another batch")
    void reusedBatchArray(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).build()) {
            for (var i = 0; i < 10; i++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{5, (byte) i}, new byte[]{(byte) i})).awaitDurable();
            }
            // In the tree, so reads of them consult the removal's bounds rather than versions it cut
            ledger.checkpoint();
            // Large enough for the pool's arrays, with a resident value so the paged ones are copied off it
            var first = ledger.newBatch().put(SESSIONS, new byte[]{1}, filled(3_000, 1))
                    .deleteRange(MESSAGES, com.github.auties00.ledger.KeyRange.between(new byte[]{5, 2}, new byte[]{5, 6}));
            for (var i = 0; i < 4; i++) {
                first.put(MESSAGES, new byte[]{6, (byte) i}, filled(2_000, 10 + i));
            }
            ledger.commit(first).awaitDurable();
            for (var round = 0; round < 8; round++) {
                // Each batch takes an array of the same size from the pool and fills it with other bytes
                var other = ledger.newBatch().put(SESSIONS, new byte[]{2}, filled(3_000, 0x7F));
                for (var i = 0; i < 4; i++) {
                    other.put(MESSAGES, new byte[]{7, (byte) round, (byte) i}, filled(2_000, 0x55));
                }
                ledger.commit(other).awaitDurable();
            }
            assertEquals(3_000, ledger.get(SESSIONS, new byte[]{1}).orElseThrow().length);
            assertEquals(1, ledger.get(SESSIONS, new byte[]{1}).orElseThrow()[2_999]);
            for (var i = 0; i < 4; i++) {
                var value = ledger.get(MESSAGES, new byte[]{6, (byte) i}).orElseThrow();
                assertEquals(2_000, value.length);
                assertEquals(10 + i, value[0]);
                assertEquals(10 + i, value[1_999]);
            }
            for (var i = 0; i < 10; i++) {
                assertEquals(i < 2 || i >= 6, ledger.get(MESSAGES, new byte[]{5, (byte) i}).isPresent(), "key 5," + i);
            }
        }
    }

    @Test
    @DisplayName("a store in memory keeps each paged value on an array of its own, not on its batch's")
    void memoryStoreOwnsItsValues() throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().memory().keyspace(MESSAGES).build()) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 50; i++) {
                batch.put(MESSAGES, new byte[]{8, (byte) i}, new byte[200]);
            }
            ledger.commit(batch);
            assertEquals(200, head(ledger, MESSAGES, new byte[]{8, 0}).sourceBytesForTests());
        }
    }

    @Test
    @DisplayName("the chains the sweep drops after a checkpoint, which only commits take out of the ordered map, stay reachable through the links of a descending scan within a thousand")
    void sweptChainsLeaveTheLinks(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 30).build()) {
            var count = 8_000;
            for (var batch = 0; batch < count / 1_000; batch++) {
                var writes = ledger.newBatch();
                for (var i = 0; i < 1_000; i++) {
                    writes.put(MESSAGES, ByteBuffer.allocate(4).putInt(batch * 1_000 + i).array(), new byte[16]);
                }
                ledger.commit(writes).awaitDurable();
            }
            // Orders the chains and links them
            assertEquals(count, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
            var references = new java.util.ArrayList<java.lang.ref.WeakReference<Chain>>();
            var state = ledger.state(MESSAGES);
            for (var i = 0; i < count; i++) {
                references.add(new java.lang.ref.WeakReference<>(state.chain(MemorySegment.ofArray(ByteBuffer.allocate(4).putInt(i).array()))));
            }
            // A reader of the tree before the checkpoint keeps every chain through its prune; the sweep drops them after
            try (var _ = ledger.snapshot()) {
                ledger.checkpoint();
            }
            for (var i = 0; i < 400; i++) {
                ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, new byte[]{(byte) i})).awaitDurable();
            }
            assertEquals(0, state.chains(), "the sweep left chains in memory");
            var retained = references.size();
            for (var attempt = 0; attempt < 10 && retained > 1_300; attempt++) {
                System.gc();
                Thread.sleep(20);
                retained = 0;
                for (var reference : references) {
                    if (reference.get() != null) {
                        retained++;
                    }
                }
            }
            assertTrue(retained <= 1_300, retained + " of the " + count + " chains the sweep dropped stayed reachable");
            assertEquals(count, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
        }
    }

    @Test
    @DisplayName("while checkpoints fail, the chains a range removal empties, which only commits take out of the ordered map, stay on the links within a thousand")
    void removedChainsLeaveTheLinksWhileCheckpointsFail() throws Exception {
        var disk = CrashFileSystem.create();
        var failing = new java.util.concurrent.atomic.AtomicBoolean(true);
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null
                    && path.getFileName().toString().equals(PageFile.FILE)) {
                throw new java.io.IOException("No space left on device");
            }
        });
        var failures = new java.util.concurrent.ConcurrentLinkedQueue<LedgerException>();
        try (var ledger = (LiveLedger) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 30).onBackgroundFailure(failures::add).build()) {
            var count = 8_000;
            for (var batch = 0; batch < count / 1_000; batch++) {
                var writes = ledger.newBatch();
                for (var i = 0; i < 1_000; i++) {
                    writes.put(MESSAGES, ByteBuffer.allocate(4).putInt(batch * 1_000 + i).array(), new byte[16]);
                }
                ledger.commit(writes).awaitDurable();
            }
            var above = ByteBuffer.allocate(4).putInt(1_000_000).array();
            ledger.commit(ledger.newBatch().put(MESSAGES, above, new byte[16])).awaitDurable();
            // Orders the chains and links them
            assertEquals(count + 1, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
            var state = ledger.state(MESSAGES);
            assertThrows(LedgerException.class, ledger::checkpoint);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, com.github.auties00.ledger.KeyRange.between(
                    ByteBuffer.allocate(4).putInt(0).array(), ByteBuffer.allocate(4).putInt(count).array()))).awaitDurable();
            for (var i = 0; i < 400; i++) {
                ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, new byte[]{(byte) i})).awaitDurable();
            }
            assertEquals(1, state.chains(), "the range removal left chains in memory");
            // The failed checkpoint's keys hold the chains as well: what the links add is what they reach
            var onLinks = 0;
            for (var chain = state.chain(MemorySegment.ofArray(above)).below(); chain != null && chain != Chain.BOTTOM; chain = chain.below()) {
                onLinks++;
            }
            assertTrue(onLinks <= 1_300, onLinks + " of the " + count + " chains the removal emptied stayed on the links");
            assertEquals(1, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
            failing.set(false);
        }
    }

    private static byte[] filled(int length, int value) {
        var bytes = new byte[length];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        return ledger.state(keyspace).chain(MemorySegment.ofArray(key)).head;
    }

    @Test
    @DisplayName("keys whose words differ only in bits a seeded multiply-and-rotate hash cancels get different hashes")
    void craftedKeysSpread() {
        // Flipping the top bit of one word and bit 28 of the next left a multiply-and-rotate state unchanged for every
        // seed: 16 keys that collided under any seed
        var hashes = new HashSet<Integer>();
        for (var pattern = 0; pattern < 16; pattern++) {
            // Word p carries the flip of pair p, word p + 1 the bit that cancels it
            var words = new long[5];
            for (var pair = 0; pair < 4; pair++) {
                if ((pattern >>> pair & 1) != 0) {
                    words[pair] ^= Long.MIN_VALUE;
                    words[pair + 1] ^= 1L << 28;
                }
            }
            var key = ByteBuffer.allocate(40).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (var word : words) {
                key.putLong(word);
            }
            var bytes = key.array();
            hashes.add(Segments.hash(MemorySegment.ofArray(bytes)));
        }
        assertTrue(hashes.size() > 12, "the crafted keys share " + (16 - hashes.size() + 1) + " hashes");
    }
}

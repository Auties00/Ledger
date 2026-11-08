package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            var chunks = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Object, Boolean>());
            for (var i = 0; i < 400; i++) {
                var version = head(ledger, MESSAGES, new byte[]{3, (byte) (i >>> 8), (byte) i});
                var bytes = version.sourceBytesForTests();
                assertTrue(bytes <= ArrayWriteBatch.CHUNK_BYTES, "a value of a large batch keeps " + bytes + " bytes");
                chunks.add(version.sourceForTests());
            }
            assertTrue(chunks.size() >= 4, "800,000 bytes of values took " + chunks.size() + " arrays");
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

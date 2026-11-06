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

    @Test
    @DisplayName("a resident value written beside large paged values takes an array of its own; one written alone shares its batch's")
    void residentValueOwnsItsBytes(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).build()) {
            var session = new byte[1_500];
            session[7] = 7;
            var batch = ledger.newBatch().put(SESSIONS, new byte[]{1}, session);
            for (var i = 0; i < 10; i++) {
                batch.put(MESSAGES, new byte[]{2, (byte) i}, new byte[4_096]);
            }
            ledger.commit(batch).awaitDurable();
            ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{9}, session)).awaitDurable();
            // The first session's version would keep the 40 KiB of messages reachable on the batch's array
            assertEquals(1_500, head(ledger, new byte[]{1}).sourceBytesForTests());
            // Alone in its batch, the value shares the batch's array, which holds little else
            var alone = head(ledger, new byte[]{9}).sourceBytesForTests();
            assertTrue(alone > 1_500 && alone <= 2 * 1_500 + 64, "a session alone in its batch keeps " + alone + " bytes");
            assertEquals(7, ledger.get(SESSIONS, new byte[]{1}).orElseThrow()[7]);
        }
    }

    private static Version head(LiveLedger ledger, byte[] key) {
        return ledger.state(SESSIONS).chain(MemorySegment.ofArray(key)).head;
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

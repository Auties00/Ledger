package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// The resident loader installing byte-array keys of every length class (in the chain's own words up to 8, 16, 24 and
/// 32 bytes, in a slab chunk, in an array of their own) with byte-array values from its leaves, which go back to the
/// pool once a run is installed: after a reopen every key reads its value, a scan lists them in byte order from any
/// key, and values written after the load and a reopen after that read the last value.
class ResidentLoadKeysTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> RAW = Keyspace.resident("raw", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> PAGED = Keyspace.paged("paged", KeyCodec.bytes(), ValueCodec.bytes());

    private static Ledger open(Path directory, boolean encrypted) {
        return Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(RAW).keyspace(PAGED).loadResidentAtOpen(false).build();
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("installs keys of every length class from its leaves, which read, scan and take writes as written")
    void everyLength(boolean encrypted, @TempDir Path directory) {
        var random = new SplittableRandom(0x4E5DL);
        var model = new TreeMap<byte[], byte[]>(Arrays::compareUnsigned);
        try (var ledger = open(directory, encrypted)) {
            var batch = ledger.newBatch();
            for (var length = 1; length <= 300; length++) {
                for (var copy = 0; copy < 12; copy++) {
                    var key = new byte[length];
                    random.nextBytes(key);
                    var value = new byte[random.nextInt(200)];
                    random.nextBytes(value);
                    batch.put(RAW, key, value);
                    model.put(key, value);
                }
                if (length % 50 == 0) {
                    ledger.commit(batch);
                    batch = ledger.newBatch();
                }
            }
            ledger.commit(batch.put(PAGED, new byte[]{1}, new byte[]{1}));
        }
        try (var ledger = open(directory, encrypted)) {
            // A scan from a key in the middle starts the load and reads the keys after it in order
            var middle = new ArrayList<>(model.keySet()).get(model.size() / 2);
            assertScan(ledger, model.tailMap(middle, true), KeyRange.atLeast(middle));
            assertScan(ledger, model, KeyRange.all());
            for (var entry : model.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(RAW, entry.getKey()).orElseThrow());
            }
            assertEquals(model.size(), ledger.size(RAW));
            // Writes over loaded keys, of the same length and of others
            var batch = ledger.newBatch();
            var index = 0;
            for (var entry : model.entrySet()) {
                if (index++ % 3 == 0) {
                    var value = entry.getValue().clone();
                    if (index % 2 == 0 && value.length > 0) {
                        value[0] ^= 0x7F;
                    } else {
                        value = Arrays.copyOf(value, value.length + 5);
                    }
                    entry.setValue(value);
                    batch.put(RAW, entry.getKey(), value);
                }
            }
            ledger.commit(batch);
            assertScan(ledger, model, KeyRange.all());
        }
        try (var ledger = open(directory, encrypted)) {
            assertScan(ledger, model, KeyRange.all());
            for (var entry : model.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(RAW, entry.getKey()).orElseThrow());
            }
        }
    }

    private static void assertScan(Ledger ledger, Map<byte[], byte[]> expected, KeyRange<byte[]> range) {
        var keys = new ArrayList<byte[]>();
        var values = new ArrayList<byte[]>();
        ledger.forEach(RAW, range, (key, value) -> {
            keys.add(key);
            values.add(value);
            return true;
        });
        assertEquals(expected.size(), keys.size());
        var at = 0;
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getKey(), keys.get(at));
            assertArrayEquals(entry.getValue(), values.get(at));
            at++;
        }
    }
}

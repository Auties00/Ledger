package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// Concurrent committers that reuse their batches, so the pool hands each batch's array to another, checked against
/// a model after every commit, after checkpoints and after a reopen.
class BatchArrayReuseStressTest {
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());
    private static final Keyspace<byte[], byte[]> BLOBS = Keyspace.paged("blobs", KeyCodec.bytes(), ValueCodec.of(
            value -> value.length, (value, target) -> MemorySegment.copy(value, 0, target, java.lang.foreign.ValueLayout.JAVA_BYTE, 0, value.length),
            source -> source.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE)));
    private static final int THREADS = 6;
    private static final int ROUNDS = Integer.getInteger("stress.rounds", 400);

    @Test
    @DisplayName("disk store: values survive batches reusing pooled arrays")
    void disk(@TempDir Path directory) throws Exception {
        List<Map<String, byte[]>> models;
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).keyspace(NAMES).keyspace(BLOBS)
                .checkpointEvery(1L << 20).largeValues(64 << 10).build()) {
            models = run(ledger, true);
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).keyspace(NAMES).keyspace(BLOBS).build()) {
            for (var model : models) {
                verifyAll(ledger, model, "after reopen");
            }
        }
    }

    @Test
    @DisplayName("memory store: values survive batches reusing pooled arrays")
    void memory() throws Exception {
        try (var ledger = Ledger.builder().memory().keyspace(SESSIONS).keyspace(MESSAGES).keyspace(NAMES).keyspace(BLOBS).build()) {
            run(ledger, false);
        }
    }

    private static List<Map<String, byte[]>> run(Ledger ledger, boolean checkpoints) throws Exception {
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var stop = new AtomicBoolean();
        var models = new ArrayList<Map<String, byte[]>>();
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < THREADS; t++) {
            var model = new TreeMap<String, byte[]>();
            models.add(model);
            var id = t;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    work(ledger, id, model);
                } catch (Throwable failure) {
                    failures.add(failure);
                }
            }));
        }
        Thread checkpointer = null;
        if (checkpoints) {
            checkpointer = Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    try {
                        ledger.checkpoint();
                        Thread.sleep(5);
                    } catch (Throwable failure) {
                        failures.add(failure);
                        return;
                    }
                }
            });
        }
        for (var thread : threads) {
            thread.join();
        }
        stop.set(true);
        if (checkpointer != null) {
            checkpointer.join();
        }
        if (!failures.isEmpty()) {
            var first = failures.peek();
            throw new AssertionError(failures.size() + " failures, first: " + first, first);
        }
        for (var model : models) {
            verifyAll(ledger, model, "at the end");
        }
        return models;
    }

    // Model keys: "<keyspace letter>:<hex key>"
    private static void work(Ledger ledger, int id, Map<String, byte[]> model) {
        var random = new Random(id * 7919L + 17);
        WriteBatch batch = ledger.newBatch();
        for (var round = 0; round < ROUNDS; round++) {
            if (random.nextInt(4) == 0) {
                batch = ledger.newBatch();
            }
            var writes = 1 + random.nextInt(random.nextBoolean() ? 3 : 60);
            var pending = new TreeMap<String, byte[]>();
            var rangeRemoval = random.nextInt(25) == 0;
            for (var w = 0; w < writes; w++) {
                var kind = random.nextInt(10);
                var keyNumber = random.nextInt(200);
                var key = new byte[]{(byte) id, (byte) (keyNumber >>> 8), (byte) keyNumber};
                var seed = random.nextInt();
                if (kind < 4) {
                    var value = pattern(sizeFor(random), seed);
                    batch.put(MESSAGES, key, value);
                    pending.put("m:" + hex(key), value);
                } else if (kind < 5) {
                    var value = pattern(sizeFor(random), seed);
                    batch.putEncoded(BLOBS, MemorySegment.ofArray(key), MemorySegment.ofArray(value));
                    pending.put("b:" + hex(key), value);
                } else if (kind < 7) {
                    var value = pattern(random.nextInt(3) == 0 ? 0 : random.nextInt(3_000), seed);
                    batch.put(SESSIONS, key, value);
                    pending.put("s:" + hex(key), value);
                } else if (kind < 8) {
                    var text = new String(ascii(pattern(random.nextInt(2_000), seed)), java.nio.charset.StandardCharsets.US_ASCII);
                    batch.put(NAMES, key, text);
                    pending.put("n:" + hex(key), text.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                } else {
                    var keyspace = random.nextBoolean() ? "m" : "s";
                    batch.delete(keyspace.equals("m") ? MESSAGES : SESSIONS, key);
                    pending.put(keyspace + ":" + hex(key), null);
                }
            }
            byte[] from = null;
            byte[] to = null;
            if (rangeRemoval) {
                var a = random.nextInt(200);
                var b = a + random.nextInt(30);
                from = new byte[]{(byte) id, (byte) (a >>> 8), (byte) a};
                to = new byte[]{(byte) id, (byte) (b >>> 8), (byte) b};
                batch.deleteRange(MESSAGES, KeyRange.between(from, to));
            }
            ledger.commit(batch);
            // The batch's writes apply in order; a range removal applies last within its batch's order position,
            // which here is after every other write
            for (var entry : pending.entrySet()) {
                if (entry.getValue() == null) {
                    model.remove(entry.getKey());
                } else {
                    model.put(entry.getKey(), entry.getValue());
                }
            }
            if (rangeRemoval) {
                var fromHex = "m:" + hex(from);
                var toHex = "m:" + hex(to);
                model.keySet().removeIf(key -> key.startsWith("m:") && key.compareTo(fromHex) >= 0 && key.compareTo(toHex) < 0);
            }
            // Read back a sample of the thread's own keys, which no other thread writes
            var keys = new ArrayList<>(model.keySet());
            for (var i = 0; i < Math.min(8, keys.size()); i++) {
                var key = keys.get(random.nextInt(keys.size()));
                verify(ledger, key, model.get(key), "round " + round);
            }
        }
        verifyAll(ledger, model, "thread " + id + " end");
    }

    private static int sizeFor(Random random) {
        return switch (random.nextInt(12)) {
            case 0 -> 0;
            case 1 -> 140_000 + random.nextInt(20_000);
            case 2 -> 70_000 + random.nextInt(10_000);
            default -> 1 + random.nextInt(2_500);
        };
    }

    private static void verifyAll(Ledger ledger, Map<String, byte[]> model, String when) {
        for (var entry : model.entrySet()) {
            verify(ledger, entry.getKey(), entry.getValue(), when);
        }
    }

    private static void verify(Ledger ledger, String modelKey, byte[] expected, String when) {
        var key = unhex(modelKey.substring(2));
        byte[] actual = switch (modelKey.charAt(0)) {
            case 'm' -> ledger.get(MESSAGES, key).orElse(null);
            case 'b' -> ledger.get(BLOBS, key).orElse(null);
            case 's' -> ledger.get(SESSIONS, key).orElse(null);
            case 'n' -> ledger.get(NAMES, key).map(text -> text.getBytes(java.nio.charset.StandardCharsets.US_ASCII)).orElse(null);
            default -> throw new IllegalStateException(modelKey);
        };
        assertTrue(Arrays.equals(expected, actual), modelKey + " " + when + ": expected "
                + (expected == null ? "absent" : expected.length + " bytes") + ", found "
                + (actual == null ? "absent" : actual.length + " bytes" + firstDifference(expected, actual)));
    }

    private static String firstDifference(byte[] expected, byte[] actual) {
        if (expected == null) {
            return "";
        }
        for (var i = 0; i < Math.min(expected.length, actual.length); i++) {
            if (expected[i] != actual[i]) {
                return " differing at " + i;
            }
        }
        return "";
    }

    private static byte[] pattern(int length, int seed) {
        var bytes = new byte[length];
        var state = seed;
        for (var i = 0; i < length; i++) {
            state = state * 1_103_515_245 + 12_345;
            bytes[i] = (byte) (state >>> 16);
        }
        return bytes;
    }

    private static byte[] ascii(byte[] bytes) {
        for (var i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ('a' + (bytes[i] & 0x0F));
        }
        return bytes;
    }

    private static String hex(byte[] bytes) {
        var builder = new StringBuilder();
        for (var b : bytes) {
            builder.append("%02x".formatted(b & 0xFF));
        }
        return builder.toString();
    }

    private static byte[] unhex(String hex) {
        var bytes = new byte[hex.length() / 2];
        for (var i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }
}

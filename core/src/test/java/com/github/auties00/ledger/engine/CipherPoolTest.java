package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The process's pool of idle ciphers ([CipherPool]) and the owners that seed their own pools from it and give their
/// ciphers back as they close: a cipher given back holds no store's key and nonce, so a store that seals under the
/// key and nonce another store last used gets no refusal from the JDK's AES-GCM; the pool keeps at most its bound; a
/// file's cipher gives back only what is idle, and a records opener once; a key derived at the first seal is derived
/// once and seals what a key given at construction seals; HMACs of cloned instances are those of new ones; a store
/// reopened after another closed makes no cipher; and stores under keys of their own that open, scan, checkpoint and
/// close at once read back every value whole.
class CipherPoolTest {
    private static final byte[] MASTER = new byte[32];
    // The messages of the store the readers of concurrentStores scan
    private static final int CHURNED = 300;
    // The lengths of its values, from none past the threshold of large values
    private static final int[] CHURNED_LENGTHS = {0, 12, 40, 120, 700, 3_000, 6_000, 20_000};

    @Test
    @DisplayName("a cipher taken again after it was given back seals under the key and nonce of its last seal")
    void givenBackForgetsTheLastNonce() throws Exception {
        CipherPool.GCM.clearForTests();
        var key = new SecretKeySpec(new byte[32], "AES");
        var nonce = new byte[12];
        var cipher = CipherPool.GCM.take();
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        var first = cipher.doFinal(new byte[100]);
        CipherPool.GCM.giveBack(cipher);
        var again = CipherPool.GCM.take();
        assertSame(cipher, again);
        // The JDK refuses an encryption under the key and nonce of the cipher's last one; the reset replaced them
        again.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        assertArrayEquals(first, again.doFinal(new byte[100]));
        CipherPool.GCM.giveBack(again);
    }

    @Test
    @DisplayName("a counter mode cipher given back decrypts as a new one does once initialized again")
    void counterModeGivenBack() throws Exception {
        CipherPool.CTR.clearForTests();
        var key = new SecretKeySpec(new byte[32], "AES");
        var counter = new javax.crypto.spec.IvParameterSpec(new byte[16]);
        var cipher = CipherPool.CTR.take();
        cipher.init(Cipher.ENCRYPT_MODE, key, counter);
        var sealed = cipher.doFinal(new byte[64]);
        CipherPool.CTR.giveBack(cipher);
        var again = CipherPool.CTR.take();
        assertSame(cipher, again);
        again.init(Cipher.DECRYPT_MODE, key, counter);
        assertArrayEquals(new byte[64], again.doFinal(sealed));
    }

    @Test
    @DisplayName("keeps at most its bound of idle ciphers")
    void bounded() {
        CipherPool.GCM.clearForTests();
        var taken = new ArrayList<Cipher>();
        for (var i = 0; i < CipherPool.GCM.bound() + 5; i++) {
            taken.add(CipherPool.GCM.take());
        }
        taken.forEach(CipherPool.GCM::giveBack);
        assertEquals(CipherPool.GCM.bound(), CipherPool.GCM.idleForTests());
        CipherPool.GCM.giveBack(null);
        assertEquals(CipherPool.GCM.bound(), CipherPool.GCM.idleForTests());
    }

    @Test
    @DisplayName("a file's cipher gives its idle ciphers to the process's pool as it closes, and a records opener gives its own back once")
    void closeGivesBackIdle() throws Exception {
        CipherPool.GCM.clearForTests();
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        var unit = unit(1);
        cipher.seal(unit, 1);
        var plain = new byte[PageFormat.SIZE];
        assertTrue(cipher.open(unit, 0, 1, plain));
        assertEquals(1, cipher.idleForTests());
        var records = cipher.records();
        assertEquals(0, cipher.idleForTests());
        records.close();
        records.close();
        assertEquals(1, cipher.idleForTests());
        cipher.close();
        assertEquals(0, cipher.idleForTests());
        assertEquals(1, CipherPool.GCM.idleForTests());
        // A call after the close takes from the process's pool and gives back to the file's own
        unit = unit(1);
        cipher.seal(unit, 1);
        assertEquals(0, CipherPool.GCM.idleForTests());
        assertTrue(cipher.open(unit, 0, 1, plain));
    }

    @Test
    @DisplayName("a key derived at the first seal is derived once, by any number of threads, and seals as the same key given at construction")
    void derivedOnce() throws Exception {
        var derivations = new AtomicInteger();
        var key = new SecretKeySpec(new byte[32], "AES");
        var lazy = new PageCipher(true, () -> {
            derivations.incrementAndGet();
            return new SecretKeySpec(new byte[32], "AES");
        });
        assertTrue(lazy.encrypts());
        assertEquals(0, derivations.get());
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var futures = new ArrayList<Future<byte[]>>();
            for (var i = 0; i < 8; i++) {
                var index = i;
                futures.add(executor.submit(() -> {
                    start.await();
                    var unit = unit(index);
                    lazy.seal(unit, 1);
                    return unit;
                }));
            }
            start.countDown();
            var eager = new PageCipher(key);
            for (var i = 0; i < futures.size(); i++) {
                var sealed = futures.get(i).get();
                var expected = unit(i);
                eager.seal(expected, 1);
                assertArrayEquals(expected, sealed);
                var plain = new byte[PageFormat.SIZE];
                assertTrue(eager.open(sealed, 0, 1, plain));
            }
        }
        assertEquals(1, derivations.get());
        var plain = new PageCipher(false, () -> {
            throw new AssertionError("A store without encryption derives no key");
        });
        assertFalse(plain.encrypts());
        var unit = unit(3);
        plain.seal(unit, 1);
        assertTrue(plain.open(unit, 0, 1, new byte[PageFormat.SIZE]));
    }

    @Test
    @DisplayName("keys derived through clones of one HMAC instance are those of HKDF over new instances, from many threads at once")
    void clonedHmacs() throws Exception {
        var salt = "salt of the store, thirty-two by".getBytes(StandardCharsets.US_ASCII);
        var master = new byte[32];
        new Random(7).nextBytes(master);
        var prk = hmac(salt, master);
        var expectedCheck = hmac(prk, "ledger key check".getBytes(StandardCharsets.US_ASCII), new byte[]{1});
        var expectedPage = hmac(prk, "ledger data file".getBytes(StandardCharsets.US_ASCII), new byte[]{1});
        var expectedBlob = hmac(prk, "ledger blob file".getBytes(StandardCharsets.US_ASCII), new byte[]{1});
        try (var executor = Executors.newFixedThreadPool(8)) {
            var futures = new ArrayList<Future<?>>();
            for (var i = 0; i < 64; i++) {
                futures.add(executor.submit(() -> {
                    var keys = StoreKeys.derive(EncryptionSpec.key(master), salt, 0);
                    assertArrayEquals(expectedCheck, keys.check());
                    assertArrayEquals(expectedPage, keys.pageKey().getEncoded());
                    assertArrayEquals(expectedBlob, keys.blobKey().getEncoded());
                    return null;
                }));
            }
            for (var future : futures) {
                future.get();
            }
        }
        assertNotEquals(HexFormat.of().formatHex(expectedPage), HexFormat.of().formatHex(expectedBlob));
    }

    @Test
    @DisplayName("copies of a store with a large value, opened one after the other, each seal a large value under the key and first nonce of the other")
    void copiesSealLargeValues(@TempDir Path root) throws IOException {
        var keyspace = Keyspace.paged("values", KeyCodec.utf8(), ValueCodec.bytes());
        var template = root.resolve("template");
        var large = new byte[96 << 10];
        new Random(11).nextBytes(large);
        try (var ledger = store(template, keyspace)) {
            ledger.commit(ledger.newBatch().put(keyspace, "first", large));
        }
        var first = root.resolve("first");
        var second = root.resolve("second");
        copy(template, first);
        copy(template, second);
        // The pool holds only the ciphers the first copy gave back, among them the one that sealed its large value
        // under the key and at the epoch and counter the second copy seals its own
        CipherPool.GCM.clearForTests();
        for (var copy : new Path[]{first, second}) {
            try (var ledger = store(copy, keyspace)) {
                ledger.commit(ledger.newBatch().put(keyspace, "second", large));
                assertArrayEquals(large, ledger.get(keyspace, "second").orElseThrow());
            }
            try (var ledger = store(copy, keyspace)) {
                assertArrayEquals(large, ledger.get(keyspace, "first").orElseThrow());
                assertArrayEquals(large, ledger.get(keyspace, "second").orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("a store reopened after another closed makes no cipher")
    void reopenMakesNoCipher(@TempDir Path directory) {
        var keyspace = Keyspace.resident("values", KeyCodec.utf8(), ValueCodec.bytes());
        try (var ledger = store(directory, keyspace)) {
            for (var i = 0; i < 200; i++) {
                ledger.commit(ledger.newBatch().put(keyspace, "k" + i, new byte[100]));
            }
        }
        // A cold open and close leaves the process's pool with what an open of this store takes
        try (var ledger = store(directory, keyspace)) {
            ledger.get(keyspace, "k1");
        }
        var made = CipherPool.GCM.madeForTests();
        try (var ledger = store(directory, keyspace)) {
            assertArrayEquals(new byte[100], ledger.get(keyspace, "k1").orElseThrow());
            ledger.commit(ledger.newBatch().put(keyspace, "k1", new byte[3]));
        }
        assertEquals(made, CipherPool.GCM.madeForTests());
    }

    @Test
    @DisplayName("ciphers moving through the pool between encrypted stores that commit, scan, checkpoint, open and close at once never carry a key or a nonce across")
    void concurrentStores(@TempDir Path root) throws Exception {
        var paged = Keyspace.paged("paged", KeyCodec.bytes(), ValueCodec.bytes());
        var resident = Keyspace.resident("resident", KeyCodec.bytes(), ValueCodec.bytes());
        var errors = new ConcurrentLinkedQueue<String>();
        var stop = new AtomicBoolean();
        var generations = new AtomicInteger();
        var threads = new ArrayList<Thread>();
        var main = root.resolve("main");
        try (var ledger = churned(main, 1, paged, resident)) {
            var batch = ledger.newBatch();
            for (var m = 0; m < CHURNED; m++) {
                batch.put(paged, key(m), churnedValue(m, 0)).put(resident, key(m), churnedValue(m, 0));
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            // Every page read decrypts, so the readers take ciphers as often as the writer and the checkpoints do
            ((LiveLedger) ledger).pagesForTests().forceColdReadsForTests(true);
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    for (var g = 1; !stop.get(); g++) {
                        var writes = ledger.newBatch();
                        for (var m = 0; m < CHURNED; m++) {
                            writes.put(paged, key(m), churnedValue(m, g)).put(resident, key(m), churnedValue(m, g));
                        }
                        ledger.commit(writes);
                        generations.set(g);
                        if (g % 7 == 0) {
                            ledger.checkpoint();
                        }
                    }
                } catch (Throwable throwable) {
                    errors.add("writer: " + throwable);
                }
            }));
            for (var r = 0; r < 2; r++) {
                var snapshot = r == 1;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        while (!stop.get()) {
                            if (snapshot) {
                                try (var view = ledger.snapshot()) {
                                    scanChurned(view, paged, CHURNED, errors);
                                    scanChurned(view, resident, CHURNED, errors);
                                }
                            } else {
                                scanChurned(ledger, paged, CHURNED, errors);
                                scanChurned(ledger, resident, CHURNED, errors);
                            }
                        }
                    } catch (Throwable throwable) {
                        errors.add("reader: " + throwable);
                    }
                }));
            }
            // Stores under keys of their own, opened and closed over and over, so ciphers they give back reach the
            // others through the pool
            for (var c = 0; c < 2; c++) {
                var directory = root.resolve("churn" + c);
                var seed = 2 + c;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var round = 0; !stop.get(); round++) {
                            try (var other = churned(directory, seed, paged, resident)) {
                                var writes = other.newBatch();
                                for (var m = 0; m < 40; m++) {
                                    writes.put(paged, key(m), churnedValue(m, round));
                                }
                                other.commit(writes).awaitDurable();
                                if (round % 3 == 0) {
                                    other.checkpoint();
                                }
                                scanChurned(other, paged, 40, errors);
                            }
                        }
                    } catch (Throwable throwable) {
                        errors.add("churn: " + throwable);
                    }
                }));
            }
            Thread.sleep(4_000);
            stop.set(true);
            for (var thread : threads) {
                thread.join(60_000);
                assertFalse(thread.isAlive(), "a thread did not stop");
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        // The tail of the log, recovered
        try (var ledger = churned(main, 1, paged, resident)) {
            scanChurned(ledger, paged, CHURNED, errors);
            scanChurned(ledger, resident, CHURNED, errors);
        }
        var found = new ArrayList<>(errors);
        assertEquals(List.of(), found.subList(0, Math.min(20, found.size())), "after " + generations.get() + " generations");
    }

    private static Ledger churned(Path directory, int seed, Keyspace<?, ?> paged, Keyspace<?, ?> resident) {
        var key = new byte[32];
        Arrays.fill(key, (byte) seed);
        return ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(Encryption.key(key))
                .keyspace(paged).keyspace(resident)
                .pageCache(1 << 20)
                .largeValues(12 << 10)
                .commitWindow(Duration.ZERO))
                .segmentSize(256 << 10)
                .spillBudget(64 << 10)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(4).putInt(message).array();
    }

    // The value of `message` at `generation`: the generation, the message, then bytes of both
    private static byte[] churnedValue(int message, int generation) {
        var length = Math.max(8, CHURNED_LENGTHS[Math.floorMod(message * 3 + generation, CHURNED_LENGTHS.length)]);
        var value = new byte[length];
        ByteBuffer.wrap(value).putInt(generation).putInt(message);
        for (var i = 8; i < length; i++) {
            value[i] = (byte) (message * 31 + generation * 17 + i * 7);
        }
        return value;
    }

    /// Scans `keyspace` of `view` forward and checks it holds the `count` messages of one generation, each whole.
    private static void scanChurned(View view, Keyspace<byte[], byte[]> keyspace, int count, Queue<String> errors) {
        var seen = new int[1];
        var generation = new int[]{-1};
        view.forEach(keyspace, KeyRange.all(), (key, value) -> {
            var message = ByteBuffer.wrap(key).getInt();
            if (message != seen[0]) {
                errors.add(keyspace.name() + ": message " + message + " in place of " + seen[0]);
            }
            if (generation[0] < 0) {
                generation[0] = ByteBuffer.wrap(value).getInt();
            }
            if (!Arrays.equals(churnedValue(message, generation[0]), value)) {
                errors.add(keyspace.name() + ": message " + message + " is not that of generation " + generation[0]);
            }
            seen[0]++;
            return true;
        });
        if (seen[0] != count) {
            errors.add(keyspace.name() + ": " + seen[0] + " messages of " + count);
        }
    }

    private static Ledger store(Path directory, Keyspace<?, ?> keyspace) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(MASTER)).keyspace(keyspace)
                .largeValues(4 << 10).build();
    }

    private static void copy(Path from, Path to) throws IOException {
        try (var files = Files.walk(from)) {
            for (var path : files.toList()) {
                var target = to.resolve(from.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(target);
                } else if (!path.getFileName().toString().endsWith(".lock")) {
                    Files.copy(path, target);
                }
            }
        }
    }

    // A unit of one page whose content is 100 bytes of `seed`, its nonce made of `seed` too
    private static byte[] unit(int seed) {
        var unit = new byte[PageFormat.SIZE];
        Arrays.fill(unit, PageFormat.HEADER, PageFormat.HEADER + 100, (byte) seed);
        Bytes.i32(unit, PageFormat.LENGTH, 100);
        Bytes.i64(unit, PageFormat.NONCE, seed + 1);
        return unit;
    }

    private static byte[] hmac(byte[] key, byte[]... parts) throws Exception {
        var mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        for (var part : parts) {
            mac.update(part);
        }
        return mac.doFinal();
    }
}

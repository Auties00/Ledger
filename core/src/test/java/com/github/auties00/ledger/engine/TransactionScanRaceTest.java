package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// A stress of a transaction's scan beside what runs without the roles a transaction holds: the prunes
/// of the commits published just before it, on their committing threads, readers that come and go, checkpoints and
/// the loader of a resident key space.
class TransactionScanRaceTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> RES = Keyspace.resident("res", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAG = Keyspace.paged("pag", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int SPACE = 400;

    private static String key(int i) {
        return "k%04d".formatted(i);
    }

    private static Ledger open(Path dir) {
        if (dir == null) {
            return Ledger.builder().memory().keyspace(RES).keyspace(PAG).build();
        }
        return Ledger.builder().directory(dir).encryption(Encryption.key(KEY)).keyspace(RES).keyspace(PAG)
                .loadResidentAtOpen(false).build();
    }

    @ParameterizedTest(name = "directory={0}")
    @CsvSource({"false", "true"})
    @DisplayName("a transaction's scan reads the state at its call beside commits, prunes, readers and checkpoints")
    void race(boolean directory, @TempDir Path temp) throws Exception {
        var dir = directory ? temp : null;
        var current = new AtomicReference<>(open(dir));
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var seed = current.get().newBatch();
        for (var i = 0; i < SPACE; i += 2) {
            seed.put(RES, key(i), "seed" + i).put(PAG, key(i), "seed" + i);
        }
        current.get().commit(seed);
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 3; t++) {
            var id = t;
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(id);
                try {
                    while (!stop.get()) {
                        var ledger = current.get();
                        try {
                            var batch = ledger.newBatch();
                            var n = 1 + random.nextInt(4);
                            for (var i = 0; i < n; i++) {
                                var ks = random.nextBoolean() ? RES : PAG;
                                var k = key(random.nextInt(SPACE));
                                if (random.nextInt(3) == 0) {
                                    batch.delete(ks, k);
                                } else {
                                    batch.put(ks, k, "w" + id + "-" + random.nextInt(1000));
                                }
                            }
                            ledger.commit(batch);
                        } catch (IllegalStateException closed) {
                            Thread.onSpinWait();
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
        }
        threads.add(Thread.ofPlatform().start(() -> {
            var random = new Random(99);
            try {
                while (!stop.get()) {
                    var ledger = current.get();
                    try (var snapshot = ledger.snapshot()) {
                        var ks = random.nextBoolean() ? RES : PAG;
                        var first = new ArrayList<String>();
                        snapshot.forEach(ks, KeyRange.all(), (k, v) -> first.add(k + "=" + v));
                        if (directory && random.nextInt(20) == 0) {
                            ledger.checkpoint();
                        }
                        var second = new ArrayList<String>();
                        snapshot.forEach(ks, KeyRange.<String>all().reversed(), (k, v) -> second.add(k + "=" + v));
                        assertEquals(first, second.reversed(), "a snapshot reads one state");
                    } catch (IllegalStateException closed) {
                        Thread.onSpinWait();
                    }
                }
            } catch (Throwable throwable) {
                failure.compareAndSet(null, throwable);
            }
        }));
        try {
            var random = new Random(7);
            for (var round = 0; round < 400 && failure.get() == null; round++) {
                if (directory && round % 100 == 99) {
                    // A reopen: the transactions that follow run beside the loader of the resident key space
                    var old = current.get();
                    old.close();
                    current.set(open(dir));
                }
                var ks = random.nextBoolean() ? RES : PAG;
                var descending = random.nextBoolean();
                var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
                var step = descending ? -1 : 1;
                var kind = random.nextInt(4);
                var r = round;
                current.get().update(tx -> {
                    if (random.nextBoolean()) {
                        tx.put(ks, key(random.nextInt(SPACE)), "pre" + r);
                    }
                    var before = tx.range(ks, range, Integer.MAX_VALUE).stream().map(e -> e.key() + "=" + e.value()).toList();
                    var seen = new ArrayList<String>();
                    tx.forEach(ks, range, (k, v) -> {
                        seen.add(k + "=" + v);
                        var p = Integer.parseInt(k.substring(1));
                        for (var d : List.of(1, 2, 7)) {
                            var target = p + d * step;
                            if (target < 0 || target >= SPACE) {
                                continue;
                            }
                            switch (kind) {
                                case 0 -> tx.put(ks, key(target), "tx" + r);
                                case 1 -> tx.delete(ks, key(target));
                                case 2 -> {
                                    tx.put(ks, key(target), "a" + r);
                                    tx.delete(ks, key(target));
                                    tx.put(ks, key(target), "b" + r);
                                }
                                default -> {
                                    if (d == 7) {
                                        tx.deleteRange(ks, KeyRange.between(key(Math.min(p + step, target)), key(Math.max(p + step, target) + 1)));
                                    }
                                }
                            }
                        }
                        return true;
                    });
                    assertEquals(before, seen, () -> "round " + r + " kind " + kind + " descending " + descending + " " + ks.name());
                });
            }
        } finally {
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            current.get().close();
        }
        if (failure.get() != null) {
            fail(failure.get());
        }
    }
}

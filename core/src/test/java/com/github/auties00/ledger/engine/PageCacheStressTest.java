package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Readers without a lock against writers that fill, replace, remove, evict, grow and shrink the slots under them. Every
/// unit is a new array stamped with the key and nonce it is cached under, so a hit that returns another entry's array,
/// the one thing a slot's race must never do, shows in the stamp; the stamp also covers delta records' leaves, whose
/// header names their base.
class PageCacheStressTest {
    private static final long RUN_MILLIS = 1_500;
    private static final int LENGTH = 64;
    // Past the header
    private static final int STAMP = PageFormat.HEADER;
    private static final int READERS = 3;
    private static final int PAGES = 3_000;
    private static final int HOT_PAGE = 2;

    /// What a thread names: a key of the cache and the reference a reader asks for it with.
    private record Target(long key, PageRef ref) {
        static Target random(ThreadLocalRandom random) {
            var page = random.nextInt(4) == 0 ? HOT_PAGE : HOT_PAGE + 1 + random.nextInt(PAGES);
            var nonce = random.nextInt(1, 4);
            return switch (random.nextInt(3)) {
                case 0 -> new Target(PageCache.dataKey(page, -1), new PageRef(page, 1, nonce, ~nonce));
                case 1 -> new Target(PageCache.key(PageCache.BLOBS, page), new PageRef(page, 1 + page % 3, nonce, ~nonce));
                default -> {
                    var slot = page % 4;
                    yield new Target(PageCache.dataKey(page, slot), new PageRef(page, 1, nonce, ~nonce, slot));
                }
            };
        }

        /// {@return a new array for this target, with the header of its own or, for a delta record, of another unit}
        byte[] unit() {
            return unit(null);
        }

        /// {@return a new array for this target, with the header of its own or, for a delta record, of `base` if not
        /// `null` and of another unit otherwise}
        byte[] unit(PageRef base) {
            var unit = new byte[LENGTH];
            if (ref.isRecord()) {
                TreeNodes.deltaHeader(unit, base != null ? base : new PageRef(ref.page() + 1, 1, 99, 99), 0);
            } else {
                PageFile.header(unit, PageFormat.LEAF, 0, ref.page(), ref.pages(), 0, ref.nonceHigh(), ref.nonceLow());
            }
            Bytes.i64(unit, STAMP, key);
            Bytes.i64(unit, STAMP + 8, ref.nonceHigh());
            Bytes.i32(unit, STAMP + 16, ref.nonceLow());
            Bytes.i32(unit, STAMP + 20, ref.pages());
            return unit;
        }

        /// {@return what is wrong with `unit` as the hit of this target, or `null`}
        String check(byte[] unit) {
            if (unit.length != LENGTH) {
                return "a hit of " + unit.length + " bytes";
            }
            if (Bytes.i64(unit, STAMP) != key || Bytes.i64(unit, STAMP + 8) != ref.nonceHigh()
                    || Bytes.i32(unit, STAMP + 16) != ref.nonceLow() || Bytes.i32(unit, STAMP + 20) != ref.pages()) {
                return "the array of key " + Bytes.i64(unit, STAMP) + " nonce " + Bytes.i64(unit, STAMP + 8) + " for " + this;
            }
            if (!ref.isRecord() && !ref.describes(unit)) {
                return "a header that does not name " + this;
            }
            return null;
        }
    }

    /// Runs `tasks` on threads of their own until the time is up or one fails.
    private static void run(List<Task> tasks) throws InterruptedException {
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var threads = new ArrayList<Thread>();
        for (var task : tasks) {
            threads.add(Thread.ofPlatform().daemon().start(() -> {
                try {
                    while (!stop.get()) {
                        task.step();
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                    stop.set(true);
                }
            }));
        }
        var end = System.nanoTime() + Duration.ofMillis(RUN_MILLIS).toNanos();
        while (!stop.get() && System.nanoTime() < end) {
            Thread.sleep(10);
        }
        stop.set(true);
        for (var thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }

    /// One iteration of a racing thread.
    @FunctionalInterface
    private interface Task {
        void step() throws Exception;
    }

    @Test
    @DisplayName("a hit is always the array cached under the key and nonce asked for, through fills, replacements, removals, evictions, ticks, growth and shrinking")
    void hitsAreCorrect() throws InterruptedException {
        var budget = new PageCache.Budget();
        // Fewer units than the keys, so puts evict, and enough for the table to grow well past its first size
        var cache = PageCache.in(budget, 1024L * LENGTH);
        IdleTicker.unwatch(cache);
        var hits = new AtomicLong();
        var tasks = new ArrayList<Task>();
        for (var i = 0; i < READERS; i++) {
            var peeking = i == 0;
            tasks.add(() -> {
                var target = Target.random(ThreadLocalRandom.current());
                var hit = peeking ? peekOrGet(cache, target) : cache.get(target.key(), target.ref());
                if (hit != null) {
                    hits.getAndIncrement();
                    assertNull(target.check(hit));
                }
            });
        }
        tasks.add(() -> {
            var random = ThreadLocalRandom.current();
            var target = Target.random(random);
            if (random.nextInt(8) == 0) {
                cache.remove(target.key());
            } else {
                cache.put(target.key(), target.ref(), target.unit(), random.nextBoolean());
            }
        });
        // One slot's array and metadata again and again, alternating two nonces
        var round = new long[1];
        tasks.add(() -> {
            var nonce = (int) (round[0]++ % 2) + 1;
            var hot = new Target(PageCache.dataKey(HOT_PAGE, -1), new PageRef(HOT_PAGE, 1, nonce, ~nonce));
            cache.put(hot.key(), hot.ref(), hot.unit(), true);
        });
        // Empties the table now and then, so a tick shrinks it and the puts grow it again
        tasks.add(() -> {
            Thread.sleep(50);
            cache.tick();
            cache.tick();
        });
        run(tasks);
        assertTrue(hits.get() > 0, "no read hit");
        assertEquals(cache.size(), budget.total());
        assertTrue(cache.size() <= budget.limit(), "the cache holds " + cache.size() + " bytes");
        assertEquals(cache.size(), (long) cache.clockLengthForTests() * LENGTH, "the clock lists every entry once");
        cache.close();
        assertEquals(0, budget.total());
    }

    @ParameterizedTest(name = "delta record: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a read of one slot a writer keeps replacing under other nonces gets the array of the nonce it asked for or a miss")
    void replacementsOfOneSlot(boolean record) throws InterruptedException {
        var cache = PageCache.isolated(1L << 20);
        IdleTicker.unwatch(cache);
        var targets = new Target[8];
        for (var nonce = 0; nonce < targets.length; nonce++) {
            targets[nonce] = record
                    ? new Target(PageCache.dataKey(HOT_PAGE, 1), new PageRef(HOT_PAGE, 1, nonce, ~nonce, 1))
                    : new Target(PageCache.dataKey(HOT_PAGE, -1), new PageRef(HOT_PAGE, 1, nonce, ~nonce));
        }
        var hits = new AtomicLong();
        var tasks = new ArrayList<Task>();
        for (var i = 0; i < READERS; i++) {
            tasks.add(() -> {
                var target = targets[ThreadLocalRandom.current().nextInt(targets.length)];
                var hit = cache.get(target.key(), target.ref());
                if (hit != null) {
                    hits.getAndIncrement();
                    assertNull(target.check(hit));
                }
            });
        }
        var round = new int[1];
        tasks.add(() -> {
            var target = targets[round[0]++ & (targets.length - 1)];
            cache.put(target.key(), target.ref(), target.unit(), true);
        });
        run(tasks);
        assertTrue(hits.get() > 0, "no read hit");
        assertEquals(1, cache.clockLengthForTests());
        assertEquals(LENGTH, cache.size());
        cache.close();
    }

    @Test
    @DisplayName("a read of a base leaf gets the base or a miss when the leaf of a delta record on it takes the slot the base left")
    void recordLeafInTheSlotOfItsBase() throws InterruptedException {
        var cache = PageCache.isolated(1L << 20);
        IdleTicker.unwatch(cache);
        var baseRef = new PageRef(HOT_PAGE, 1, 5, ~5);
        var base = new Target(PageCache.dataKey(HOT_PAGE, -1), baseRef);
        var record = recordInSameSlot(base.key());
        // The premise: the record's leaf reuses the very slot the base left, as a table of the fewest slots has
        cache.put(base.key(), base.ref(), base.unit(), true);
        cache.remove(base.key());
        assertEquals(1, cache.tombstonesForTests());
        cache.put(record.key(), record.ref(), record.unit(baseRef), true);
        assertEquals(0, cache.tombstonesForTests());
        cache.remove(record.key());
        var tasks = new ArrayList<Task>();
        // More readers than processors: a reader the scheduler stops between its probe and its read of the slot's array
        // finds whatever the writer left there by then
        for (var i = 0; i < 2 * Runtime.getRuntime().availableProcessors(); i++) {
            tasks.add(() -> {
                var hit = cache.get(base.key(), base.ref());
                if (hit != null) {
                    assertNull(base.check(hit));
                }
            });
        }
        tasks.add(() -> {
            cache.put(base.key(), base.ref(), base.unit(), true);
            cache.remove(base.key());
            cache.put(record.key(), record.ref(), record.unit(baseRef), true);
            cache.remove(record.key());
        });
        run(tasks);
        cache.close();
    }

    /// {@return a delta record whose key has the same first slot as `key` in a table of 64 slots}
    private static Target recordInSameSlot(long key) {
        for (var page = HOT_PAGE + 1; ; page++) {
            var candidate = PageCache.dataKey(page, 0);
            if (PageCache.homeForTests(candidate, 64) == PageCache.homeForTests(key, 64)) {
                return new Target(candidate, new PageRef(page, 1, 7, ~7, 0));
            }
        }
    }

    private static byte[] peekOrGet(PageCache cache, Target target) {
        return PageCache.dataKey(target.ref().page(), target.ref().slot()) == target.key()
                ? cache.peek(target.ref())
                : cache.get(target.key(), target.ref());
    }

    @Test
    @DisplayName("a read of a key no writer touches hits throughout every rebuild of the table, as the table it finds holds the key")
    void rebuildsHideNothing() throws InterruptedException {
        var cache = PageCache.isolated(1L << 30);
        IdleTicker.unwatch(cache);
        var stable = new ArrayList<Target>();
        for (var page = 2; page < 66; page++) {
            var target = new Target(PageCache.dataKey(page, -1), new PageRef(page, 1, 5, ~5));
            stable.add(target);
            cache.put(target.key(), target.ref(), target.unit(), true);
        }
        var next = new int[]{1_000};
        var tasks = new ArrayList<Task>();
        for (var i = 0; i < READERS; i++) {
            tasks.add(() -> {
                var target = stable.get(ThreadLocalRandom.current().nextInt(stable.size()));
                var hit = cache.get(target.key(), target.ref());
                assertTrue(hit != null, () -> "a read missed " + target);
                assertNull(target.check(hit));
            });
        }
        tasks.add(() -> {
            // Stops at 300,000 entries: enough rebuilds, and the heap of a test
            if (next[0] < 300_000) {
                var page = next[0]++;
                var target = new Target(PageCache.dataKey(page, -1), new PageRef(page, 1, 5, ~5));
                cache.put(target.key(), target.ref(), target.unit(), true);
            } else {
                Thread.sleep(1);
            }
        });
        run(tasks);
        assertTrue(cache.capacityForTests() > 64, "the table never grew");
        cache.close();
    }

    @Test
    @DisplayName("two caches of a budget that take room from each other never wait for each other, and account every byte")
    void crossEviction() {
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            var budget = new PageCache.Budget();
            var caches = List.of(PageCache.in(budget, 256L * LENGTH), PageCache.in(budget, 256L * LENGTH));
            var tasks = new ArrayList<Task>();
            for (var index = 0; index < 4; index++) {
                var cache = caches.get(index % 2);
                tasks.add(() -> {
                    var target = Target.random(ThreadLocalRandom.current());
                    cache.put(target.key(), target.ref(), target.unit(), ThreadLocalRandom.current().nextBoolean());
                });
            }
            run(tasks);
            assertEquals(caches.getFirst().size() + caches.getLast().size(), budget.total());
            assertTrue(budget.total() <= budget.limit(), "the caches hold " + budget.total() + " bytes");
            caches.getFirst().close();
            assertEquals(caches.getLast().size(), budget.total());
            caches.getLast().close();
            assertEquals(0, budget.total());
        });
    }

    @Test
    @DisplayName("a put that races the close of its cache leaves no byte in the budget")
    void putRacingClose() throws InterruptedException {
        var budget = new PageCache.Budget();
        for (var round = 0; round < 200; round++) {
            var cache = PageCache.in(budget, 1024L * LENGTH);
            var stop = new AtomicBoolean();
            var putters = new ArrayList<Thread>();
            for (var i = 0; i < 2; i++) {
                putters.add(Thread.ofPlatform().daemon().start(() -> {
                    while (!stop.get()) {
                        var target = Target.random(ThreadLocalRandom.current());
                        cache.put(target.key(), target.ref(), target.unit(), true);
                    }
                }));
            }
            Thread.sleep(round % 3);
            cache.close();
            stop.set(true);
            for (var putter : putters) {
                putter.join();
            }
            assertEquals(0, cache.size(), "round " + round);
            assertEquals(0, budget.total(), "round " + round);
        }
    }
}

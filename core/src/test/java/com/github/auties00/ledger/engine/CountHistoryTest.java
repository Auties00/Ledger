package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The counts of a key space by commit, against a sorted map, alone and under a reader at a fixed sequence.
class CountHistoryTest {
    @Test
    @DisplayName("answers from a reset with its one count, then grows at the first commits and drops back to one")
    void reset() {
        var history = new CountHistory(5, 7);
        assertEquals(7, history.last());
        assertEquals(7, history.at(5));
        assertEquals(0, history.at(4));
        for (var sequence = 6L; sequence < 100; sequence++) {
            history.append(sequence, sequence);
            assertEquals(sequence, history.at(sequence));
            assertEquals(7, history.at(5));
        }
        assertEquals(95, history.sizeForTests());
        history.dropBelow(99);
        assertEquals(1, history.sizeForTests());
        assertEquals(99, history.last());
        history.reset(200, 3);
        assertEquals(1, history.sizeForTests());
        history.append(201, 4);
        assertEquals(3, history.at(200));
        assertEquals(4, history.at(201));
    }

    @Test
    @DisplayName("answers as the floor of a sorted map of the counts, through growth and drops")
    void model() {
        var history = new CountHistory(0, 0);
        var model = new TreeMap<Long, Long>();
        model.put(0L, 0L);
        var random = new SplittableRandom(3);
        var sequence = 0L;
        var count = 0L;
        for (var step = 0; step < 200_000; step++) {
            switch (random.nextInt(4)) {
                case 0, 1 -> {
                    sequence += 1 + random.nextInt(3);
                    count += random.nextInt(5) - 2;
                    history.append(sequence, count);
                    model.put(sequence, count);
                }
                case 2 -> {
                    var oldest = model.firstKey() + random.nextLong(sequence - model.firstKey() + 1);
                    history.dropBelow(oldest);
                    var floor = model.floorKey(oldest);
                    model.headMap(floor, false).clear();
                }
                default -> {
                    var at = random.nextLong(sequence + 2);
                    var entry = model.floorEntry(at);
                    // Below what the history still holds a reader is never asked about; the model answers 0 there
                    if (at >= model.firstKey()) {
                        assertEquals(entry.getValue(), history.at(at), "at " + at);
                    }
                }
            }
            assertEquals(model.lastEntry().getValue(), history.last());
            assertEquals(model.size(), history.sizeForTests());
        }
    }

    @Test
    @DisplayName("gives a reader the count as of its sequence while the publisher appends and drops below it")
    void concurrentReader() throws Exception {
        var history = new CountHistory(0, 0);
        // The count as of sequence s is s: a reader at a sequence it read knows the answer
        var published = new AtomicLong();
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var reader = Thread.ofPlatform().start(() -> {
            var random = new SplittableRandom(1);
            while (!stop.get()) {
                var latest = published.get();
                var at = latest - random.nextInt(4);
                if (at < 0) {
                    continue;
                }
                var count = history.at(at);
                if (count != at && count != 0) {
                    failure.compareAndSet(null, new AssertionError("count " + count + " at " + at));
                }
            }
        });
        for (var sequence = 1L; sequence <= 2_000_000; sequence++) {
            history.append(sequence, sequence);
            published.set(sequence);
            if (sequence % 64 == 0) {
                // The reader reads at most four below the latest
                history.dropBelow(sequence - 8);
            }
        }
        stop.set(true);
        reader.join();
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertTrue(history.sizeForTests() < 128, history.sizeForTests() + " counts kept");
    }
}

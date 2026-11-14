package com.github.auties00.ledger.engine;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// The publisher's scan of the reads in progress: every busy slot and registration is found however many there are,
/// past the scratch the table starts with and in tables larger than the shared all-zero one.
class ReaderTableTest {
    @ParameterizedTest(name = "slots {0}, reads {1}, snapshots {2}")
    @CsvSource({"256, 3, 0", "256, 200, 0", "256, 40, 40", "512, 400, 30", "16, 16, 50"})
    @DisplayName("finds every read and snapshot at every scan, as active() does")
    void scanFindsAll(int slots, int reads, int snapshots) {
        var latest = new AtomicLong(1);
        var table = new ReaderTable(slots, latest::get);
        var expected = new TreeSet<Long>();
        var leases = new ArrayList<ReaderTable.Lease>();
        for (var round = 0; round < 3; round++) {
            for (var i = 0; i < reads; i++) {
                leases.add(table.enter());
                expected.add(latest.get());
                latest.incrementAndGet();
            }
            var registered = new ArrayList<Long>();
            for (var i = 0; i < snapshots; i++) {
                registered.add(table.register());
                expected.add(latest.get());
                latest.incrementAndGet();
            }
            var scan = table.scan();
            var current = latest.get();
            var active = new long[expected.size() + 1];
            active[0] = current;
            var index = 1;
            for (var sequence : expected.descendingSet()) {
                active[index++] = sequence;
            }
            assertArrayEquals(active, scan.active());
            assertArrayEquals(table.active(), scan.active());
            assertEquals(expected.isEmpty() ? Long.MAX_VALUE : expected.first(), scan.oldest());
            for (var lease : leases) {
                table.exit(lease);
            }
            for (var sequence : registered) {
                table.unregister(sequence);
            }
            leases.clear();
            expected.clear();
            assertArrayEquals(new long[]{current}, table.scan().active());
            assertEquals(Long.MAX_VALUE, table.scan().oldest());
        }
    }
}

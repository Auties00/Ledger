package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// The writer's maintenance: what it prunes and when, seen from inside the engine.
class LiveLedgerMaintenanceTest {
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    private static int chainLength(LiveLedger ledger, String key) {
        var version = ledger.state(NAMES).entries.get(Segments.encodeKey(KeyCodec.utf8(), key));
        var length = 0;
        while (version != null) {
            length++;
            version = version.older;
        }
        return length;
    }

    @Test
    @DisplayName("overwrites keep one version when no reader needs more")
    void overwrites() {
        var ledger = new LiveLedger(List.of(NAMES), new HeapPool(1 << 20));
        for (var i = 0; i < 10_000; i++) {
            ledger.commit(ledger.newBatch().put(NAMES, "k", "v" + i));
        }
        assertEquals(1, chainLength(ledger, "k"));
        assertEquals(Optional.of("v9999"), ledger.get(NAMES, "k"));
    }

    @Test
    @DisplayName("a snapshot keeps the version it reads, and only that one, until it closes")
    void snapshotPins() {
        var ledger = new LiveLedger(List.of(NAMES), new HeapPool(1 << 20));
        ledger.commit(ledger.newBatch().put(NAMES, "k", "pinned"));
        var snapshot = ledger.snapshot();
        for (var i = 0; i < 100; i++) {
            ledger.commit(ledger.newBatch().put(NAMES, "k", "v" + i));
        }
        assertEquals(2, chainLength(ledger, "k"));
        assertEquals(Optional.of("pinned"), snapshot.get(NAMES, "k"));
        snapshot.close();
        ledger.commit(ledger.newBatch().put(NAMES, "k", "last"));
        assertEquals(1, chainLength(ledger, "k"));
    }

    @Test
    @DisplayName("a range removal is applied and dropped once no reader can miss it")
    void rangeRemovals() {
        var ledger = new LiveLedger(List.of(NAMES), new HeapPool(1 << 20));
        var batch = ledger.newBatch();
        for (var i = 0; i < 100; i++) {
            batch.put(NAMES, "k" + (100 + i), "v");
        }
        ledger.commit(batch);
        var snapshot = ledger.snapshot();
        ledger.commit(ledger.newBatch().deleteRange(NAMES, KeyRange.all()));
        // The snapshot still reads the keys, so the removal waits
        assertEquals(1, ledger.state(NAMES).removals.length);
        assertEquals(100, ledger.state(NAMES).entries.size());
        assertEquals(100, snapshot.size(NAMES));
        snapshot.close();
        ledger.commit(ledger.newBatch().put(NAMES, "after", "v"));
        assertEquals(0, ledger.state(NAMES).removals.length);
        assertEquals(1, ledger.state(NAMES).entries.size());
        assertEquals(1, ledger.size(NAMES));
    }

    @Test
    @DisplayName("deleted keys leave the entries once no reader can see them")
    void deletes() {
        var ledger = new LiveLedger(List.of(NAMES), new HeapPool(1 << 20));
        ledger.commit(ledger.newBatch().put(NAMES, "a", "1").put(NAMES, "b", "2"));
        ledger.commit(ledger.newBatch().delete(NAMES, "a"));
        ledger.commit(ledger.newBatch().put(NAMES, "c", "3"));
        assertEquals(List.of("b", "c"), ledger.state(NAMES).entries.keySet().stream()
                .map(key -> KeyCodec.utf8().decode(key))
                .toList());
    }
}

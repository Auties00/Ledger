package com.github.auties00.ledger.engine;

import java.util.concurrent.atomic.AtomicLong;

final class Diag {
    static final AtomicLong COMMITS = new AtomicLong();
    static final AtomicLong LISTED = new AtomicLong();
    static final AtomicLong CREATED = new AtomicLong();
    static final AtomicLong SLAB = new AtomicLong();

    private Diag() {
    }

    static boolean slab(boolean placed) {
        if (placed) {
            SLAB.incrementAndGet();
        }
        return placed;
    }

    static void commit(LiveLedger ledger, java.util.Collection<KeyspaceState> states) {
        var n = COMMITS.incrementAndGet();
        if (n % 5000 != 0) {
            return;
        }
        var line = new StringBuilder("DIAG commits=" + n + " listed=" + LISTED.get() + " created=" + CREATED.get() + " slab=" + SLAB.get());
        for (var state : states) {
            if (state.resident || state.overwrites == null || !state.keyspace.name().equals("generic")) {
                continue;
            }
            line.append(" spills=").append(state.overwrites.spills()).append(" worthwhile=").append(state.overwrites.worthwhile())
                    .append(" judged=").append(state.overwrites.judged()).append(" sampled=").append(state.overwrites.sampledPutsForTests())
                    .append(" run=").append(state.run != null);
        }
        var slab = ledger.valueSlab();
        line.append(" hot=").append(slab != null && slab.hot());
        var spills = ledger.spills();
        if (spills != null) {
            line.append(" logged=").append(spills.loggedValuesForTests()).append(" drained=").append(spills.drainedRecordsForTests())
                    .append(" held=").append(spills.heldForTests());
        }
        System.err.println(line);
    }
}

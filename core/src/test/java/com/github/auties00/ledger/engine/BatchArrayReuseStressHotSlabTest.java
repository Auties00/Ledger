package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// Concurrent committers that reuse their batches, while the values they keep are on the chunks of a slab forced hot.
class BatchArrayReuseStressHotSlabTest extends BatchArrayReuseStressTest {
    @BeforeAll
    static void forceHot() {
        ValueSlab.forceNewHotForTests(1L << 20);
    }

    @AfterAll
    static void stopForcing() {
        ValueSlab.forceNewHotForTests(0);
    }
}

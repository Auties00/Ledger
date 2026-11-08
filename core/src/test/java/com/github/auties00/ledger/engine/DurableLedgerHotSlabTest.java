package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// The behaviour of a store in a directory while its values are on the chunks of a slab forced hot.
class DurableLedgerHotSlabTest extends DurableLedgerTest {
    @BeforeAll
    static void forceHot() {
        ValueSlab.forceNewHotForTests(1L << 20);
    }

    @AfterAll
    static void stopForcing() {
        ValueSlab.forceNewHotForTests(0);
    }
}

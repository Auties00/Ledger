package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// The differential test of a transaction's scan while the values are on the chunks of a slab forced hot.
class TransactionScanFuzzHotSlabTest extends TransactionScanFuzzTest {
    @BeforeAll
    static void forceHot() {
        ValueSlab.forceNewHotForTests(1L << 20);
    }

    @AfterAll
    static void stopForcing() {
        ValueSlab.forceNewHotForTests(0);
    }
}

package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// Transaction scans with writing visitors against a model, while scans read ahead as if every read of the data file were cold.
class TransactionScanFuzzColdReadsTest extends TransactionScanFuzzTest {
    @BeforeAll
    static void forceCold() {
        PageFile.forceColdNewForTests(true);
    }

    @AfterAll
    static void stopForcing() {
        PageFile.forceColdNewForTests(false);
    }
}

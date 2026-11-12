package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// What a transaction's scan passes when its visitor writes the scanned key space, while scans read ahead as if every read of the data file were cold.
class ScanContractColdReadsTest extends ScanContractTest {
    @BeforeAll
    static void forceCold() {
        PageFile.forceColdNewForTests(true);
    }

    @AfterAll
    static void stopForcing() {
        PageFile.forceColdNewForTests(false);
    }
}

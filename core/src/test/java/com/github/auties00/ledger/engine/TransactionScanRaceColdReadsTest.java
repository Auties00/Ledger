package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// Transaction scans beside commits, readers, checkpoints and reopens, while scans read ahead as if every read of the data file were cold.
class TransactionScanRaceColdReadsTest extends TransactionScanRaceTest {
    @BeforeAll
    static void forceCold() {
        PageFile.forceColdNewForTests(true);
    }

    @AfterAll
    static void stopForcing() {
        PageFile.forceColdNewForTests(false);
    }
}

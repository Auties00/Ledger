package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/// What a store recovers after a crash while its scans read ahead as if every read of its data file were cold.
class CrashRecoveryColdReadsTest extends CrashRecoveryTest {
    @BeforeAll
    static void forceCold() {
        PageFile.forceColdNewForTests(true);
    }

    @AfterAll
    static void stopForcing() {
        PageFile.forceColdNewForTests(false);
    }
}

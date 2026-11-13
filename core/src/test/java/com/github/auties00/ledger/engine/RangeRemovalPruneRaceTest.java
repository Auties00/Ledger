package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// A prune on a committing thread beside the publisher applying a range removal, forced at the point where the prune
/// has read the version below the one it keeps and is about to link the two: the publisher cuts the chain there and
/// drops the removal's record, which TransactionScanRaceTest met on Windows as a snapshot whose reverse scan saw a key
/// its forward scan did not.
class RangeRemovalPruneRaceTest {
    private static final Keyspace<String, String> RES = Keyspace.resident("res", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAG = Keyspace.paged("pag", KeyCodec.utf8(), ValueCodec.utf8());

    @ParameterizedTest(name = "resident={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a prune on a committing thread never links back versions a range removal applied meanwhile hid")
    void pruneBesideRemoval(boolean resident) throws Exception {
        var keyspace = resident ? RES : PAG;
        var ledger = (LiveLedger) Ledger.builder().memory().keyspace(RES).keyspace(PAG).build();
        try {
            ledger.commit(ledger.newBatch().put(keyspace, "k", "w"));
            // Keeps the removal a record: a reader before it still sees "w"
            var before = ledger.snapshot();
            ledger.update(tx -> tx.deleteRange(keyspace, KeyRange.between("a", "z")));
            var after = ledger.snapshot();
            assertEquals(Optional.empty(), after.get(keyspace, "k"));
            var committer = Thread.currentThread();
            var forced = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            // The prune of the next put of "k" keeps "v" for the latest reader and "w" for the reader before the
            // removal; before it links them, that reader leaves and another commit's publisher applies the removal
            ledger.hookPruneForTests(() -> {
                if (Thread.currentThread() != committer || !forced.compareAndSet(false, true)) {
                    return;
                }
                before.close();
                var other = Thread.ofPlatform().start(() -> {
                    try {
                        ledger.commit(ledger.newBatch().put(keyspace, "other", "x"));
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                });
                try {
                    other.join();
                } catch (InterruptedException exception) {
                    throw new AssertionError(exception);
                }
            });
            ledger.commit(ledger.newBatch().put(keyspace, "k", "v"));
            ledger.hookPruneForTests(null);
            assertNull(failure.get());
            assertTrue(forced.get(), "the prune reached the link");
            assertEquals(Optional.empty(), after.get(keyspace, "k"), "the removal hides the version before it");
            assertEquals(List.of(), after.range(keyspace, KeyRange.all(), 10));
            assertEquals(List.of(), after.range(keyspace, KeyRange.<String>all().reversed(), 10));
            after.close();
            try (var latest = ledger.snapshot()) {
                assertEquals(Optional.of("v"), latest.get(keyspace, "k"));
            }
        } finally {
            ledger.close();
        }
    }
}

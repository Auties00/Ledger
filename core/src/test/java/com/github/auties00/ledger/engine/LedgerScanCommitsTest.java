package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// A scan of the store itself, outside a transaction, whose visitor commits writes ahead of it, in memory and in a
/// directory, resident and paged.
class LedgerScanCommitsTest {
    private static final Keyspace<String, String> RES = Keyspace.resident("res", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAG = Keyspace.paged("pag", KeyCodec.utf8(), ValueCodec.utf8());

    @ParameterizedTest(name = "memory={0} {1}")
    @CsvSource({"true,res", "true,pag", "false,res", "false,pag"})
    @DisplayName("a scan of the store reads the state at its call, whatever its visitor commits")
    void ledgerScanIsSnapshot(boolean memory, String which, @TempDir Path dir) {
        var ks = which.equals("res") ? RES : PAG;
        try (var ledger = memory ? Ledger.builder().memory().keyspace(RES).keyspace(PAG).build()
                : Ledger.builder().directory(dir).encryption(Encryption.key(new byte[32])).keyspace(RES).keyspace(PAG).build()) {
            var model = new TreeMap<String, String>();
            var b = ledger.newBatch();
            for (var i = 0; i < 600; i += 2) {
                b.put(ks, "k%05d".formatted(i), "v" + i);
                model.put("k%05d".formatted(i), "v" + i);
            }
            ledger.commit(b);
            var before = model.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList();
            var seen = new ArrayList<String>();
            ledger.forEach(ks, KeyRange.all(), (k, v) -> {
                seen.add(k + "=" + v);
                var p = Integer.parseInt(k.substring(1));
                if (p % 14 == 0 && p + 21 < 600) {
                    ledger.commit(ledger.newBatch().put(ks, "k%05d".formatted(p + 20), "new").delete(ks, "k%05d".formatted(p + 10))
                            .put(ks, "k%05d".formatted(p + 21), "ins"));
                }
                return true;
            });
            assertEquals(before, seen);
        }
    }
}

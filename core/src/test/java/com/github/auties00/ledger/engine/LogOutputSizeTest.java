package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The size of the buffer the log seals its groups into: one block for a store of small commits, grown for larger
/// groups and taken back, after a run of small ones, to the size that run needed, so groups of two sizes neither keep
/// a large buffer nor make it, or the native buffer of a write through that follows it, change size group after group.
class LogOutputSizeTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Small segments, so a checkpoint leaves spares a group may be written through into
    private static final long SEGMENT = 64 << 10;
    // The small groups in a row after which the buffer goes back to their size
    private static final int SHRINK_AFTER = 256;

    private static Ledger open(Path directory, boolean encrypted, WriteThrough way) {
        var builder = Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none()).keyspace(NAMES);
        return ((LedgerBuilder) builder).segmentSize(SEGMENT).writeThrough(way).build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static void commit(Ledger ledger, Map<String, String> model, String key, int length) throws InterruptedException {
        var value = "v".repeat(length);
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    private static Map<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    private static void survivesPowerLoss(CrashFileSystem disk, boolean encrypted, Map<String, String> model) {
        for (var seed = 0; seed < 4; seed++) {
            var survivor = disk.afterPowerLoss(new Random(seed));
            try (var ledger = open(survivor.getPath("/store"), encrypted, new WriteThrough(null, null))) {
                assertEquals(model, contents(ledger), "seed " + seed);
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}, through: {1}")
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    @DisplayName("settles for groups of about 3 and 6 KB at the size they need, without changing it or the write through's buffer again")
    void settles(boolean encrypted, boolean through) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var allocations = new AtomicInteger();
        var way = new WriteThrough(through ? WriteThrough.Kind.DSYNC : null, null);
        way.allocatorForTests = size -> {
            allocations.incrementAndGet();
            return Arena.ofAuto().allocate(size, LogWriter.ALIGNMENT);
        };
        try (var ledger = open(disk.getPath("/store"), encrypted, way)) {
            var log = log(ledger);
            commit(ledger, model, "first", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength(), "a store of small commits keeps a buffer of one block");
            var commits = 0;
            // Segments enough for a checkpoint to leave spares, which groups are written through into
            while (log.currentSegment() < 8) {
                commit(ledger, model, "w" + commits++, 500);
            }
            ledger.checkpoint();
            var throughBefore = log.writtenThrough();
            var allocationsBefore = allocations.get();
            var changes = 0;
            var length = log.outputLength();
            for (var i = 0; i < 600; i++) {
                commit(ledger, model, "a" + i, i % 2 == 0 ? 3_000 : 6_000);
                if (log.outputLength() != length) {
                    changes++;
                    length = log.outputLength();
                }
            }
            assertTrue(changes <= 2, "the buffer changed size " + changes + " times");
            assertEquals(2 * LogWriter.ALIGNMENT, length, "groups of up to two blocks keep a buffer of two");
            if (through) {
                assertTrue(log.writtenThrough() > throughBefore, "no group was written through");
                assertTrue(allocations.get() - allocationsBefore <= 2,
                        "the write through's buffer was replaced " + (allocations.get() - allocationsBefore) + " times");
            }
        }
        survivesPowerLoss(disk, encrypted, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("goes back to one block after a large group once a run of small groups follows it, and not before")
    void shrinks(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = open(disk.getPath("/store"), encrypted, new WriteThrough(null, null))) {
            var log = log(ledger);
            commit(ledger, model, "large", 40_000);
            var grown = log.outputLength();
            assertTrue(grown > 40_000, "the buffer holds " + grown + " bytes after a group of 40 KB");
            for (var i = 0; i < SHRINK_AFTER; i++) {
                commit(ledger, model, "s" + i, 500);
                assertEquals(grown, log.outputLength(), "the buffer shrank after " + (i + 1) + " small groups");
            }
            commit(ledger, model, "next", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
            // A group too large to count as small starts the run again
            commit(ledger, model, "again", 20_000);
            for (var i = 0; i < SHRINK_AFTER - 1; i++) {
                commit(ledger, model, "t" + i, 500);
            }
            commit(ledger, model, "medium", 30_000);
            grown = log.outputLength();
            for (var i = 0; i < SHRINK_AFTER; i++) {
                commit(ledger, model, "u" + i, 500);
            }
            assertEquals(grown, log.outputLength(), "a large group in the run did not start it again");
            commit(ledger, model, "last", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
        }
        survivesPowerLoss(disk, encrypted, model);
    }
}

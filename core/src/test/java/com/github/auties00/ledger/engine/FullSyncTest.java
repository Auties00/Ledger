package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// The forces that empty the drive's cache on macOS: on there with native access, off everywhere else.
class FullSyncTest {
    private static final boolean MAC_OS = System.getProperty("os.name", "").startsWith("Mac");
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    @Test
    @DisplayName("on macOS with native access, a store forces through F_FULLFSYNC and works")
    void macOs(@TempDir Path directory) throws Exception {
        assumeTrue(MAC_OS, "macOS only");
        assertTrue(FullSync.available(directory), "the tests grant native access, so full syncs are available");
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32])).keyspace(NAMES).build()) {
            for (var i = 0; i < 20; i++) {
                ledger.commit(ledger.newBatch().put(NAMES, "k" + i, "v" + i)).awaitDurable();
            }
            ledger.checkpoint();
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32])).keyspace(NAMES).build()) {
            assertEquals(Optional.of("v19"), ledger.get(NAMES, "k19"));
        }
    }

    @Test
    @DisplayName("elsewhere, and on any other file system, a force is the channel's own")
    void elsewhere(@TempDir Path directory) {
        if (!MAC_OS) {
            assertFalse(FullSync.available(directory));
        }
        assertFalse(FullSync.available(CrashFileSystem.create().getPath("/store")));
    }
}

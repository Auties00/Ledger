package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// [Ledger#keyspaces()]: the key spaces a store holds, opened or not, across reopens, and only its own in memory.
class KeyspacesTest {
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> NEWER = Keyspace.paged("newer", KeyCodec.utf8(), ValueCodec.utf8());

    @Test
    @DisplayName("lists the key spaces of earlier opens it was not opened with, which open raw by name")
    void unopened(@TempDir Path directory) {
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none())
                .keyspace(CHATS).keyspace(MESSAGES).keyspace(NEWER).build()) {
            assertEquals(Set.of("chats", "messages", "newer"), ledger.keyspaces());
            ledger.commit(ledger.newBatch().put(NEWER, "k", "v"));
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(CHATS).build()) {
            assertEquals(Set.of("chats", "messages", "newer"), ledger.keyspaces());
            assertThrows(UnsupportedOperationException.class, () -> ledger.keyspaces().add("x"));
        }
        var raw = Keyspace.paged("newer", KeyCodec.bytes(), ValueCodec.bytes());
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(raw).build()) {
            assertEquals(1, ledger.forEach(raw, KeyRange.all(), (_, _) -> true));
            assertEquals(Set.of("chats", "messages", "newer"), ledger.keyspaces());
        }
    }

    @Test
    @DisplayName("a store in memory lists the key spaces it was opened with, and a closed store none")
    void memory() {
        var ledger = Ledger.builder().memory().keyspace(CHATS).keyspace(MESSAGES).build();
        assertEquals(Set.of("chats", "messages"), ledger.keyspaces());
        ledger.close();
        assertThrows(IllegalStateException.class, ledger::keyspaces);
    }
}

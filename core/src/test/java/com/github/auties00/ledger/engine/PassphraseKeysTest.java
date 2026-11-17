package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerKeyException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// A passphrase derives its master key once per salt and instance, so a store opened again with the same
/// [Encryption] skips the stretching, while another salt or passphrase still derives its own.
class PassphraseKeysTest {
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    @Test
    @DisplayName("derives once per salt, hands out copies, and forgets on destroy")
    void memoised() {
        var spec = EncryptionSpec.passphrase("secret".toCharArray());
        var derivations = new AtomicInteger();
        var salt = new byte[]{1, 2, 3};
        var first = spec.derivedKey(salt, 10, () -> {
            derivations.incrementAndGet();
            return new byte[]{42};
        });
        var second = spec.derivedKey(salt.clone(), 10, () -> {
            derivations.incrementAndGet();
            return new byte[]{7};
        });
        assertEquals(1, derivations.get());
        assertArrayEquals(first, second);
        assertNotSame(first, second);
        spec.derivedKey(new byte[]{9}, 10, () -> {
            derivations.incrementAndGet();
            return new byte[]{1};
        });
        spec.derivedKey(salt, 11, () -> {
            derivations.incrementAndGet();
            return new byte[]{1};
        });
        assertEquals(3, derivations.get());
        spec.destroy();
        spec.derivedKey(salt, 10, () -> {
            derivations.incrementAndGet();
            return new byte[]{1};
        });
        assertEquals(4, derivations.get());
    }

    @Test
    @DisplayName("a store reopened with the same instance reads its data, and a wrong passphrase is still refused")
    void reopen(@TempDir Path directory) {
        var encryption = Encryption.passphrase("correct horse".toCharArray());
        try (var ledger = Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES).build()) {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "b"));
        }
        var started = System.nanoTime();
        try (var ledger = Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES).build()) {
            assertEquals(Optional.of("b"), ledger.get(NAMES, "a"));
        }
        System.out.printf("reopen with a known passphrase: %.1f ms%n", (System.nanoTime() - started) / 1e6);
        assertThrows(LedgerKeyException.class, () -> Ledger.builder().directory(directory)
                .encryption(Encryption.passphrase("wrong".toCharArray())).keyspace(NAMES).build());
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Supplier;

/// The [Encryption] a store was asked for: a raw key, a passphrase or none, with a private copy of the secret.
///
/// A passphrase keeps the master keys it was stretched into, one per salt, so a store opened again with the same
/// instance, as an application does at each reconnection, pays the derivation once: about half a second of one
/// processor at [StoreKeys#PASSPHRASE_ITERATIONS] iterations. They live as long as the instance, which holds the
/// passphrase anyway, until [#destroy()].
public final class EncryptionSpec implements Encryption {
    /// No encryption.
    public static final EncryptionSpec NONE = new EncryptionSpec(Kind.NONE, null, null);
    /// The most master keys a passphrase keeps, one per store it opened: past it, the oldest goes.
    private static final int DERIVED_KEYS = 16;

    private final Kind kind;
    private final byte[] key;
    private final char[] passphrase;
    // The master keys derived from the passphrase, by salt and iteration count, oldest first; guarded by itself
    private final LinkedHashMap<String, byte[]> derived;

    private EncryptionSpec(Kind kind, byte[] key, char[] passphrase) {
        this.kind = kind;
        this.key = key;
        this.passphrase = passphrase;
        this.derived = kind == Kind.PASSPHRASE ? new LinkedHashMap<>() : null;
    }

    /// {@return encryption under a copy of `key`}
    ///
    /// @param key the master key, 32 bytes
    /// @throws IllegalArgumentException if `key` is not 32 bytes
    public static EncryptionSpec key(byte[] key) {
        Objects.requireNonNull(key, "key");
        if (key.length != 32) {
            throw new IllegalArgumentException("A master key is 32 bytes: " + key.length);
        }
        return new EncryptionSpec(Kind.KEY, key.clone(), null);
    }

    /// {@return encryption under a key derived from a copy of `passphrase`}
    ///
    /// @param passphrase the passphrase, not empty
    /// @throws IllegalArgumentException if `passphrase` is empty
    public static EncryptionSpec passphrase(char[] passphrase) {
        Objects.requireNonNull(passphrase, "passphrase");
        if (passphrase.length == 0) {
            throw new IllegalArgumentException("A passphrase cannot be empty");
        }
        return new EncryptionSpec(Kind.PASSPHRASE, null, passphrase.clone());
    }

    /// {@return the kind of secret}
    public Kind kind() {
        return kind;
    }

    /// {@return the raw master key, which [#destroy()] clears; `null` unless [#kind()] is [Kind#KEY]}
    byte[] key() {
        return key;
    }

    /// {@return the passphrase, which [#destroy()] clears; `null` unless [#kind()] is [Kind#PASSPHRASE]}
    char[] passphrase() {
        return passphrase;
    }

    /// {@return a copy of the master key derived from the passphrase with `salt` and `iterations`, derived by `derive`
    /// the first time and kept for the next}
    ///
    /// Concurrent opens with the same salt derive it once: the others wait for the first.
    byte[] derivedKey(byte[] salt, int iterations, Supplier<byte[]> derive) {
        var name = HexFormat.of().formatHex(salt) + ':' + iterations;
        synchronized (derived) {
            var known = derived.get(name);
            if (known == null) {
                known = derive.get();
                derived.put(name, known);
                if (derived.size() > DERIVED_KEYS) {
                    var oldest = derived.entrySet().iterator();
                    Arrays.fill(oldest.next().getValue(), (byte) 0);
                    oldest.remove();
                }
            }
            return known.clone();
        }
    }

    /// Clears the copy of the secret and the keys derived from it.
    public void destroy() {
        if (key != null) {
            Arrays.fill(key, (byte) 0);
        }
        if (passphrase != null) {
            Arrays.fill(passphrase, '\0');
        }
        if (derived != null) {
            synchronized (derived) {
                for (var master : derived.values()) {
                    Arrays.fill(master, (byte) 0);
                }
                derived.clear();
            }
        }
    }

    @Override
    public String toString() {
        return "Encryption[" + kind + "]";
    }

    /// The kinds of secret.
    public enum Kind {
        /// A raw 32-byte master key.
        KEY,
        /// A passphrase the master key is derived from.
        PASSPHRASE,
        /// No encryption.
        NONE
    }
}

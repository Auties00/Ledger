package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;

import java.util.Arrays;
import java.util.Objects;

/// The [Encryption] a store was asked for: a raw key, a passphrase or none, with a private copy of the secret.
public final class EncryptionSpec implements Encryption {
    /// No encryption.
    public static final EncryptionSpec NONE = new EncryptionSpec(Kind.NONE, null, null);

    private final Kind kind;
    private final byte[] key;
    private final char[] passphrase;

    private EncryptionSpec(Kind kind, byte[] key, char[] passphrase) {
        this.kind = kind;
        this.key = key;
        this.passphrase = passphrase;
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

    /// Clears the copy of the secret; the store calls it once it has derived its keys.
    public void destroy() {
        if (key != null) {
            Arrays.fill(key, (byte) 0);
        }
        if (passphrase != null) {
            Arrays.fill(passphrase, '\0');
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

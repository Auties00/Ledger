package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.EncryptionSpec;

/// How a store encrypts its files.
///
/// An encrypting store seals every log frame, page and header with AES-256-GCM, under keys derived per file from
/// one master key. The associated data binds each sealed unit to its file, its position and the generation that wrote
/// it, so a unit moved, replayed from an older file or altered fails to open: the store reports
/// [LedgerCorruptedException] rather than return it. Keys are sealed like values; what the files reveal is their
/// size, and how it grows. The master key is never written to disk; the store keeps a salt and a check that tells a
/// wrong key from a damaged file.
public sealed interface Encryption permits EncryptionSpec {
    /// {@return encryption under `key`}
    ///
    /// The store copies the key and keeps it in memory while it is open; the caller may clear its array afterward.
    ///
    /// @param key the master key, 32 bytes
    /// @throws IllegalArgumentException if `key` is not 32 bytes
    static Encryption key(byte[] key) {
        return EncryptionSpec.key(key);
    }

    /// {@return encryption under a key derived from `passphrase`}
    ///
    /// The key is derived with PBKDF2-HMAC-SHA256 over a random salt the store keeps, at 600,000 iterations, which
    /// costs about half a second each time the store opens. The store copies the passphrase; the caller may clear
    /// its array afterward.
    ///
    /// @param passphrase the passphrase, not empty
    /// @throws IllegalArgumentException if `passphrase` is empty
    static Encryption passphrase(char[] passphrase) {
        return EncryptionSpec.passphrase(passphrase);
    }

    /// {@return no encryption: the files hold their data in plaintext}
    ///
    /// Frames and pages still carry checksums, so damage is still detected.
    static Encryption none() {
        return EncryptionSpec.NONE;
    }
}

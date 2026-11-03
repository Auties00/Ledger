package com.github.auties00.ledger;

/// A store was opened with an encryption other than the one it was created with: a wrong key or passphrase, a key
/// for a plaintext store, or no key for an encrypted one.
///
/// The store tells this from damage with a check it keeps beside the salt, so a wrong key never surfaces as
/// [LedgerCorruptedException].
public final class LedgerKeyException extends LedgerException {
    /// Creates an exception with `message`.
    ///
    /// @param message the detail message, which says which encryption the store needs
    public LedgerKeyException(String message) {
        super(message, null);
    }
}

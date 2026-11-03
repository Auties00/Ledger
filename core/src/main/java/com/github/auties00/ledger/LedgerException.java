package com.github.auties00.ledger;

/// The failures a store reports.
///
/// Every failure is unchecked: a store either works or stops, and callers that can act on a failure match the kind
/// they can act on. Misuse (a closed store, an unknown key space) throws the JDK's [IllegalStateException] or
/// [IllegalArgumentException] instead.
public abstract sealed class LedgerException extends RuntimeException
        permits LedgerIoException, LedgerCorruptedException, LedgerKeyException, LedgerLockedException {
    /// Creates an exception with `message` and `cause`.
    ///
    /// @param message the detail message
    /// @param cause the cause, or `null` for none
    LedgerException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.github.auties00.ledger;

/// A commit found that a value one of its batch's guards read had changed, so none of its writes applied.
///
/// A guard ([WriteBatch#guard]) holds a batch to the value it read: a write of the key by another commit between
/// the read and the batch's turn fails the commit with this exception. The store stays healthy; the caller reads
/// again and builds a new batch.
public final class LedgerConflictException extends LedgerException {
    /// Creates an exception with `message`.
    ///
    /// @param message the detail message
    public LedgerConflictException(String message) {
        super(message, null);
    }
}

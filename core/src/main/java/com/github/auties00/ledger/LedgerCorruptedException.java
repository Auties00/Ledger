package com.github.auties00.ledger;

/// Data in the store's files fails authentication or its checksum, beyond what recovery repairs.
///
/// Recovery repairs a log whose tail a crash left torn. Damage anywhere else, or data altered on disk, throws this
/// exception from the read that meets it, or from opening the store when it is in the files every open reads. A write
/// that waited for the entries of a resident key space to reach memory throws it too when they could not, whatever the
/// failure, which is its cause.
public final class LedgerCorruptedException extends LedgerException {
    /// Creates an exception with `message`.
    ///
    /// @param message the detail message, which names the file and the position
    /// @param cause the cause, or `null` for none
    public LedgerCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}

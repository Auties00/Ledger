package com.github.auties00.ledger;

import java.io.IOException;

/// A read, write or force of the store's files failed.
///
/// After a failed write or force the store cannot tell which commits reached stable storage, so it stops: every
/// later commit, and every wait for durability, throws this exception again. Reads keep working. Reopening the store
/// recovers every commit that was durable.
public final class LedgerIoException extends LedgerException {
    /// Creates an exception with `message` and the I/O failure that caused it.
    ///
    /// @param message the detail message
    /// @param cause the I/O failure
    public LedgerIoException(String message, IOException cause) {
        super(message, cause);
    }

    /// {@return the I/O failure}
    @Override
    public synchronized IOException getCause() {
        return (IOException) super.getCause();
    }
}

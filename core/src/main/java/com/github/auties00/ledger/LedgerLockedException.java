package com.github.auties00.ledger;

import java.nio.file.Path;

/// A store could not open its directory because another store holds it, in this process or another.
public final class LedgerLockedException extends LedgerException {
    private final Path directory;

    /// Creates an exception for `directory`.
    ///
    /// @param directory the directory held by another store
    public LedgerLockedException(Path directory) {
        super("Another store holds " + directory, null);
        this.directory = directory;
    }

    /// {@return the directory held by another store}
    public Path directory() {
        return directory;
    }
}

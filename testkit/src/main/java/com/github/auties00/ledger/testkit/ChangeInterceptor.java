package com.github.auties00.ledger.testkit;

import java.io.IOException;
import java.nio.file.Path;

/// Sees each change of a [CrashFileSystem] before the file system makes it, to fail or delay that change alone.
///
/// {@snippet lang = java :
/// // The data file fails every write, as a full disk would, while the other files keep working
/// disk.intercept((path, kind) -> {
///     if (kind == ChangeKind.WRITE && path.endsWith("ledger.data")) {
///         throw new IOException("No space left on device");
///     }
/// });
/// }
@FunctionalInterface
public interface ChangeInterceptor {
    /// Runs before the change, on the thread that makes it, while no lock of the file system is held.
    ///
    /// @param path the file or directory the change touches; for a rename, its source
    /// @param kind the kind of change
    /// @throws IOException to fail the change, which then does not happen
    void beforeChange(Path path, ChangeKind kind) throws IOException;

    /// The kinds of change a [ChangeInterceptor] sees.
    enum ChangeKind {
        /// Writing bytes to a file.
        WRITE,
        /// Truncating a file.
        TRUNCATE,
        /// Forcing a file, or a directory.
        FORCE,
        /// Creating a file or a directory.
        CREATE,
        /// Deleting a file or a directory.
        DELETE,
        /// Renaming a file, or copying one.
        RENAME
    }
}

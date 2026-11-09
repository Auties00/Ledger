package com.github.auties00.ledger.engine;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// The forces that make file system changes, not only file contents, durable.
final class Durability {
    private static final boolean WINDOWS = System.getProperty("os.name", "").startsWith("Windows");

    private Durability() {
    }

    /// Forces the entries of `directory`, so the files created in it survive a loss of power, where a directory can be
    /// forced.
    ///
    /// Linux and macOS force a directory opened for reading. Windows cannot open one, and NTFS journals the entries
    /// itself, so there the call does nothing on the default file system: a rename is durable once the journal is
    /// flushed, which a force of a file does and a write through a handle opened with `FILE_FLAG_WRITE_THROUGH` that
    /// changes no metadata does not. Any other file system, such as one that simulates failures, is asked to force the
    /// directory, and is taken to journal as NTFS does if it refuses to open one as Windows refuses.
    ///
    /// @return whether the entries are durable now; if not, a caller that needs them to be forces a file it named
    static boolean forceDirectory(Path directory) throws IOException {
        var defaultFileSystem = directory.getFileSystem() == FileSystems.getDefault();
        if (defaultFileSystem && WINDOWS) {
            return false;
        }
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
            return true;
        } catch (AccessDeniedException exception) {
            if (defaultFileSystem) {
                throw exception;
            }
            return false;
        }
    }
}

package com.github.auties00.ledger.engine;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// The forces that make file system changes, not only file contents, durable.
final class Durability {
    private static final boolean WINDOWS = System.getProperty("os.name", "").startsWith("Windows");

    private Durability() {
    }

    /// Forces the entries of `directory`, so the files created in it survive a loss of power.
    ///
    /// Linux and macOS force a directory opened for reading; Windows cannot open one, and NTFS journals the entries
    /// itself, so there the call does nothing on the default file system. Any other file system, such as one that
    /// simulates failures, is asked to force the directory.
    static void forceDirectory(Path directory) throws IOException {
        if (directory.getFileSystem() == FileSystems.getDefault() && WINDOWS) {
            return;
        }
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}

package com.github.auties00.ledger.engine;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// The forces that make file system changes, not only file contents, durable.
final class Durability {
    private Durability() {
    }

    /// Forces the entries of `directory`, so the files created in it survive a loss of power.
    ///
    /// Linux and macOS force a directory opened for reading; Windows cannot open one, and NTFS journals the entries
    /// itself, so there the call does nothing.
    static void forceDirectory(Path directory) throws IOException {
        if (System.getProperty("os.name", "").startsWith("Windows")) {
            return;
        }
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}

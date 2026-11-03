package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerLockedException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// The exclusive lock a store holds on its directory, through `ledger.lock`, against other processes and other
/// stores of this one.
final class DirectoryLock implements AutoCloseable {
    static final String FILE = "ledger.lock";

    private final FileChannel channel;
    private final FileLock lock;

    private DirectoryLock(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    /// Takes the lock on `directory`.
    ///
    /// @throws LedgerLockedException if another store holds it
    /// @throws IOException if the lock file cannot be opened
    static DirectoryLock acquire(Path directory) throws IOException {
        var channel = FileChannel.open(directory.resolve(FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            var lock = channel.tryLock();
            if (lock == null) {
                throw new LedgerLockedException(directory);
            }
            return new DirectoryLock(channel, lock);
        } catch (OverlappingFileLockException exception) {
            channel.close();
            throw new LedgerLockedException(directory);
        } catch (IOException | RuntimeException exception) {
            channel.close();
            throw exception;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            channel.close();
        }
    }
}

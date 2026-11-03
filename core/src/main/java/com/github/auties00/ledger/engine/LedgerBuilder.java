package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/// Every stage of a [Ledger] builder: the stages are views of one object that collects the options.
public final class LedgerBuilder implements Ledger.Builder, Ledger.EncryptionStage, Ledger.Options {
    private static final long MIB = 1L << 20;
    private static final String LOG_DIRECTORY = "log";
    private static final long SEGMENT_SIZE = 64 * MIB;

    private Path directory;
    private Encryption encryption;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private IoPool ioPool;
    private long pageCache;
    private long checkpointEvery;
    private Duration commitWindow;
    private long segmentSize;

    /// Creates a builder with the default options.
    public LedgerBuilder() {
        this.keyspaces = new LinkedHashMap<>();
        this.pageCache = 64 * MIB;
        this.checkpointEvery = 64 * MIB;
        this.commitWindow = Duration.ZERO;
        this.segmentSize = SEGMENT_SIZE;
    }

    /// Sets the size past which the log starts a new segment, so tests can roll segments over without writing
    /// 64 MiB.
    LedgerBuilder segmentSize(long bytes) {
        this.segmentSize = bytes;
        return this;
    }

    @Override
    public Ledger.EncryptionStage directory(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
        return this;
    }

    @Override
    public Ledger.Options memory() {
        this.directory = null;
        this.encryption = Encryption.none();
        return this;
    }

    @Override
    public Ledger.Options encryption(Encryption encryption) {
        this.encryption = Objects.requireNonNull(encryption, "encryption");
        return this;
    }

    @Override
    public Ledger.Options keyspace(Keyspace<?, ?> keyspace) {
        Objects.requireNonNull(keyspace, "keyspace");
        if (keyspaces.putIfAbsent(keyspace.name(), keyspace) != null) {
            throw new IllegalArgumentException("Two key spaces are named " + keyspace.name());
        }
        return this;
    }

    @Override
    public Ledger.Options ioPool(IoPool pool) {
        this.ioPool = Objects.requireNonNull(pool, "pool");
        return this;
    }

    @Override
    public Ledger.Options pageCache(long bytes) {
        if (bytes < MIB) {
            throw new IllegalArgumentException("The page cache is at least 1 MiB: " + bytes);
        }
        this.pageCache = bytes;
        return this;
    }

    @Override
    public Ledger.Options checkpointEvery(long logBytes) {
        if (logBytes < MIB) {
            throw new IllegalArgumentException("Checkpoints are at least 1 MiB of log apart: " + logBytes);
        }
        this.checkpointEvery = logBytes;
        return this;
    }

    @Override
    public Ledger.Options commitWindow(Duration window) {
        Objects.requireNonNull(window, "window");
        if (window.isNegative() || window.compareTo(Duration.ofSeconds(1)) > 0) {
            throw new IllegalArgumentException("The commit window is from zero to one second: " + window);
        }
        this.commitWindow = window;
        return this;
    }

    @Override
    public Ledger build() {
        if (directory == null) {
            var ledger = new LiveLedger(keyspaces.values(), new HeapPool(pageCache));
            ledger.assignIds();
            return ledger;
        }
        return openDirectory();
    }

    private Ledger openDirectory() {
        var encryption = (EncryptionSpec) this.encryption;
        DirectoryLock lock = null;
        PlatformIoPool ownedPool = null;
        try {
            Files.createDirectories(directory);
            lock = DirectoryLock.acquire(directory);
            var logDirectory = directory.resolve(LOG_DIRECTORY);
            if (StoreHeader.read(directory) == null && hasFiles(logDirectory)) {
                throw new LedgerCorruptedException(directory + " holds a log but no header", null);
            }
            var opened = StoreHeader.openOrCreate(directory, encryption);
            Files.createDirectories(logDirectory);
            var ledger = new LiveLedger(keyspaces.values(), new HeapPool(pageCache));
            var recovered = LogRecovery.replay(logDirectory, opened.keys(), opened.header().storeId(), new LogRecovery.Sink() {
                @Override
                public void catalog(int keyspace, String name) {
                    ledger.bind(keyspace, name);
                }

                @Override
                public void commit(long sequence, byte[] body) {
                    ledger.replay(sequence, body);
                }
            });
            var catalog = ledger.assignIds();
            var pool = ioPool != null ? (PlatformIoPool) ioPool : (ownedPool = new PlatformIoPool(1));
            var log = new LogWriter(logDirectory, opened.keys(), opened.header().storeId(), recovered.lastSegment() + 1,
                    recovered.lastSequence(), segmentSize, pool.executor(), commitWindow);
            if (ownedPool != null) {
                ledger.attach(log, catalog, lock, ownedPool);
            } else {
                ledger.attach(log, catalog, lock);
            }
            return ledger;
        } catch (IOException exception) {
            release(lock, ownedPool);
            throw new LedgerIoException("The store in " + directory + " could not be opened", exception);
        } catch (RuntimeException | Error throwable) {
            release(lock, ownedPool);
            throw throwable;
        }
    }

    private static boolean hasFiles(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return false;
        }
        try (var files = Files.list(directory)) {
            return files.findAny().isPresent();
        }
    }

    private static void release(DirectoryLock lock, PlatformIoPool pool) {
        if (pool != null) {
            pool.close();
        }
        if (lock != null) {
            try {
                lock.close();
            } catch (IOException _) {
                // Opening failed already; the lock goes with the process at worst
            }
        }
    }
}

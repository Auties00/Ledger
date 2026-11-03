package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/// Every stage of a [Ledger] builder: the stages are views of one object that collects the options.
public final class LedgerBuilder implements Ledger.Builder, Ledger.EncryptionStage, Ledger.Options {
    private static final long MIB = 1L << 20;

    private Path directory;
    private Encryption encryption;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private IoPool ioPool;
    private long pageCache;
    private long checkpointEvery;
    private Duration commitWindow;

    /// Creates a builder with the default options.
    public LedgerBuilder() {
        this.keyspaces = new LinkedHashMap<>();
        this.pageCache = 64 * MIB;
        this.checkpointEvery = 64 * MIB;
        this.commitWindow = Duration.ZERO;
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
        // TODO: open the engine; this commit only defines the API
        throw new UnsupportedOperationException("The Ledger engine is not implemented yet");
    }
}

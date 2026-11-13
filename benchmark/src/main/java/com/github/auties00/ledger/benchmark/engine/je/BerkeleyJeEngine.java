package com.github.auties00.ledger.benchmark.engine.je;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Directories;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.TransactionConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;

/// Berkeley DB Java Edition, one transactional database per key space.
///
/// A deferred write commits with `COMMIT_WRITE_NO_SYNC`, which hands the log to the OS; a durable write commits with
/// `COMMIT_SYNC`, and JE groups concurrent forces. JE locks records rather than the whole store, so writers run
/// concurrently; a write that loses a lock conflict is retried.
///
/// JE has no snapshot reads, so [#openReadView()] is unsupported.
public final class BerkeleyJeEngine implements Engine {
    private final Path directory;
    private final Environment environment;
    private final EnumMap<Keyspace, Database> databases;
    private final TransactionConfig durableTransactions;
    private final TransactionConfig deferredTransactions;

    private BerkeleyJeEngine(Path directory, Environment environment, EnumMap<Keyspace, Database> databases) {
        this.directory = directory;
        this.environment = environment;
        this.databases = databases;
        this.durableTransactions = new TransactionConfig().setDurability(com.sleepycat.je.Durability.COMMIT_SYNC);
        this.deferredTransactions = new TransactionConfig().setDurability(com.sleepycat.je.Durability.COMMIT_WRITE_NO_SYNC);
    }

    /// Opens or creates an environment in `directory`.
    ///
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @return the open engine
    /// @throws IOException if the directory cannot be created
    public static BerkeleyJeEngine open(Path directory, EngineSettings settings) throws IOException {
        Files.createDirectories(directory);
        var config = new EnvironmentConfig()
                .setAllowCreate(true)
                .setTransactional(true)
                // A scan reads behind the writer's record locks: past JE's 500 ms default it failed rather than waited
                .setLockTimeout(60, java.util.concurrent.TimeUnit.SECONDS);
        config.setCacheSize(settings.cacheBytes());
        var environment = new Environment(directory.toFile(), config);
        var databases = new EnumMap<Keyspace, Database>(Keyspace.class);
        for (var keyspace : Keyspace.values()) {
            databases.put(keyspace, environment.openDatabase(null, keyspace.name().toLowerCase(Locale.ROOT), new DatabaseConfig()
                    .setAllowCreate(true)
                    .setTransactional(true)));
        }
        return new BerkeleyJeEngine(directory, environment, databases);
    }

    @Override
    public void write(WriteBatch batch, Durability durability) {
        var config = durability == Durability.DURABLE ? durableTransactions : deferredTransactions;
        while (true) {
            var transaction = environment.beginTransaction(null, config);
            try {
                for (var i = 0; i < batch.size(); i++) {
                    var database = databases.get(batch.keyspace(i));
                    var key = new DatabaseEntry(batch.key(i));
                    var value = batch.value(i);
                    if (value == null) {
                        database.delete(transaction, key);
                    } else {
                        database.put(transaction, key, new DatabaseEntry(value));
                    }
                }
                transaction.commit();
                return;
            } catch (LockConflictException _) {
                transaction.abort();
            } catch (RuntimeException exception) {
                transaction.abort();
                throw exception;
            }
        }
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) {
        var value = new DatabaseEntry();
        var status = databases.get(keyspace).get(null, new DatabaseEntry(key), value, LockMode.READ_COMMITTED);
        return status == OperationStatus.SUCCESS ? value.getData() : null;
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        var visited = 0;
        try (var cursor = databases.get(keyspace).openCursor(null, CursorConfig.READ_COMMITTED)) {
            var key = new DatabaseEntry();
            var value = new DatabaseEntry();
            OperationStatus status;
            if (descending) {
                key.setData(to);
                status = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
                status = status == OperationStatus.SUCCESS
                        ? cursor.getPrev(key, value, LockMode.DEFAULT)
                        : cursor.getLast(key, value, LockMode.DEFAULT);
                while (visited < limit && status == OperationStatus.SUCCESS) {
                    var current = Arrays.copyOfRange(key.getData(), key.getOffset(), key.getOffset() + key.getSize());
                    if (Arrays.compareUnsigned(current, from) < 0) {
                        break;
                    }
                    visited++;
                    if (!visitor.visit(current, value.getData())) {
                        break;
                    }
                    status = cursor.getPrev(key, value, LockMode.DEFAULT);
                }
            } else {
                key.setData(from);
                status = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
                while (visited < limit && status == OperationStatus.SUCCESS) {
                    var current = Arrays.copyOfRange(key.getData(), key.getOffset(), key.getOffset() + key.getSize());
                    if (Arrays.compareUnsigned(current, to) >= 0) {
                        break;
                    }
                    visited++;
                    if (!visitor.visit(current, value.getData())) {
                        break;
                    }
                    status = cursor.getNext(key, value, LockMode.DEFAULT);
                }
            }
        }
        return visited;
    }

    @Override
    public ReadView openReadView() {
        throw new UnsupportedOperationException("Berkeley DB JE has no snapshot reads");
    }

    /// {@inheritDoc}
    ///
    /// @implNote This implementation forces a JE checkpoint, which writes the dirty internal nodes of every tree to
    /// the log so that recovery starts from there.
    @Override
    public void checkpoint() {
        environment.checkpoint(new CheckpointConfig().setForce(true));
    }

    @Override
    public long sizeOnDisk() throws IOException {
        return Directories.size(directory);
    }

    @Override
    public void close() {
        for (var database : databases.values()) {
            database.close();
        }
        environment.close();
    }
}

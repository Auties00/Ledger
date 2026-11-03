package com.github.auties00.ledger.benchmark.engine.sqlite;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Directories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/// SQLite through the xerial driver in WAL mode, one `WITHOUT ROWID` table of `(k BLOB PRIMARY KEY, v BLOB)` per key
/// space.
///
/// Writes go through one connection, since SQLite allows one writer; reads go through a pool of connections, which
/// WAL mode lets run alongside the writer. A deferred write commits with `synchronous=NORMAL`, which in WAL mode
/// writes the log without forcing it; a durable write commits with `synchronous=FULL`, which forces the log. SQLite
/// has no group commit, so durable writers queue behind each other's forces.
public final class SqliteEngine implements Engine {
    private static final int READERS = Math.max(4, Runtime.getRuntime().availableProcessors());

    private final Path directory;
    private final Connection writer;
    private final EnumMap<Keyspace, PreparedStatement> upserts;
    private final EnumMap<Keyspace, PreparedStatement> deletes;
    private final EnumMap<Keyspace, PreparedStatement> rangeDeletes;
    private final ReentrantLock writeLock;
    private final BlockingQueue<Reader> readers;
    private final List<Reader> allReaders;
    private Durability writerDurability;

    private SqliteEngine(Path directory, Connection writer, List<Reader> readers) throws SQLException {
        this.directory = directory;
        this.writer = writer;
        this.upserts = new EnumMap<>(Keyspace.class);
        this.deletes = new EnumMap<>(Keyspace.class);
        this.rangeDeletes = new EnumMap<>(Keyspace.class);
        for (var keyspace : Keyspace.values()) {
            var table = table(keyspace);
            upserts.put(keyspace, writer.prepareStatement("INSERT INTO " + table + " (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v"));
            deletes.put(keyspace, writer.prepareStatement("DELETE FROM " + table + " WHERE k = ?"));
            rangeDeletes.put(keyspace, writer.prepareStatement("DELETE FROM " + table + " WHERE k >= ? AND k < ?"));
        }
        this.writeLock = new ReentrantLock();
        this.readers = new ArrayBlockingQueue<>(readers.size(), false, readers);
        this.allReaders = readers;
        this.writerDurability = Durability.DEFERRED;
    }

    /// Opens or creates `store.db` in `directory`.
    ///
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @return the open engine
    /// @throws IOException if SQLite fails to open the database or create its tables
    public static SqliteEngine open(Path directory, EngineSettings settings) throws IOException {
        Files.createDirectories(directory);
        var url = "jdbc:sqlite:" + directory.resolve("store.db");
        var cachePragma = "PRAGMA cache_size = -" + Math.max(1024, settings.cacheBytes() >> 10);
        try {
            var writer = DriverManager.getConnection(url);
            try (var statement = writer.createStatement()) {
                statement.execute("PRAGMA journal_mode = WAL");
                statement.execute("PRAGMA synchronous = NORMAL");
                // Another process may hold the write lock; wait for it rather than fail
                statement.execute("PRAGMA busy_timeout = 60000");
                statement.execute(cachePragma);
                for (var keyspace : Keyspace.values()) {
                    statement.execute("CREATE TABLE IF NOT EXISTS " + table(keyspace) + " (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID");
                }
            }
            var readers = new ArrayList<Reader>();
            for (var i = 0; i < READERS; i++) {
                var connection = DriverManager.getConnection(url);
                try (var statement = connection.createStatement()) {
                    // The page cache is per connection: the readers share the budget
                    statement.execute("PRAGMA cache_size = -" + Math.max(1024, (settings.cacheBytes() >> 10) / READERS));
                    statement.execute("PRAGMA busy_timeout = 60000");
                }
                readers.add(new Reader(connection));
            }
            return new SqliteEngine(directory, writer, readers);
        } catch (SQLException exception) {
            throw new IOException(exception);
        }
    }

    private static String table(Keyspace keyspace) {
        return keyspace.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public void write(WriteBatch batch, Durability durability) throws IOException {
        writeLock.lock();
        try {
            setDurability(durability);
            writer.setAutoCommit(false);
            try {
                for (var i = 0; i < batch.size(); i++) {
                    var keyspace = batch.keyspace(i);
                    var value = batch.value(i);
                    if (value == null) {
                        var delete = deletes.get(keyspace);
                        delete.setBytes(1, batch.key(i));
                        delete.executeUpdate();
                    } else {
                        var upsert = upserts.get(keyspace);
                        upsert.setBytes(1, batch.key(i));
                        upsert.setBytes(2, value);
                        upsert.executeUpdate();
                    }
                }
                writer.commit();
            } catch (SQLException exception) {
                writer.rollback();
                throw exception;
            } finally {
                writer.setAutoCommit(true);
            }
        } catch (SQLException exception) {
            throw new IOException(exception);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void deleteRange(Keyspace keyspace, byte[] from, byte[] to, Durability durability) throws IOException {
        writeLock.lock();
        try {
            setDurability(durability);
            var delete = rangeDeletes.get(keyspace);
            delete.setBytes(1, from);
            delete.setBytes(2, to);
            delete.executeUpdate();
        } catch (SQLException exception) {
            throw new IOException(exception);
        } finally {
            writeLock.unlock();
        }
    }

    private void setDurability(Durability durability) throws SQLException {
        if (writerDurability == durability) {
            return;
        }
        try (var statement = writer.createStatement()) {
            statement.execute(durability == Durability.DURABLE ? "PRAGMA synchronous = FULL" : "PRAGMA synchronous = NORMAL");
        }
        writerDurability = durability;
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) throws IOException {
        var reader = acquire();
        try {
            return reader.get(keyspace, key);
        } catch (SQLException exception) {
            throw new IOException(exception);
        } finally {
            readers.add(reader);
        }
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) throws IOException {
        var reader = acquire();
        try {
            var statement = reader.scan(keyspace, descending);
            statement.setBytes(1, from);
            statement.setBytes(2, to);
            statement.setInt(3, limit);
            var visited = 0;
            try (var results = statement.executeQuery()) {
                while (results.next()) {
                    visited++;
                    if (!visitor.visit(results.getBytes(1), results.getBytes(2))) {
                        break;
                    }
                }
            }
            return visited;
        } catch (SQLException exception) {
            throw new IOException(exception);
        } finally {
            readers.add(reader);
        }
    }

    private Reader acquire() throws IOException {
        try {
            return readers.take();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException(exception);
        }
    }

    @Override
    public ReadView openReadView() throws IOException {
        var reader = acquire();
        try {
            reader.connection.setAutoCommit(false);
            // The read transaction, and so the snapshot, starts with the first read
            reader.get(Keyspace.GENERIC, new byte[0]);
        } catch (SQLException exception) {
            readers.add(reader);
            throw new IOException(exception);
        }
        return new ReadView() {
            @Override
            public byte[] get(Keyspace keyspace, byte[] key) throws IOException {
                try {
                    return reader.get(keyspace, key);
                } catch (SQLException exception) {
                    throw new IOException(exception);
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    reader.connection.rollback();
                    reader.connection.setAutoCommit(true);
                } catch (SQLException exception) {
                    throw new IOException(exception);
                } finally {
                    readers.add(reader);
                }
            }
        };
    }

    @Override
    public long sizeOnDisk() throws IOException {
        return Directories.size(directory);
    }

    @Override
    public void close() throws IOException {
        try {
            for (var reader : allReaders) {
                reader.connection.close();
            }
            try (var statement = writer.createStatement()) {
                statement.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            writer.close();
        } catch (SQLException exception) {
            throw new IOException(exception);
        }
    }

    /// A read connection with its prepared statements.
    private static final class Reader {
        private final Connection connection;
        private final EnumMap<Keyspace, PreparedStatement> gets;
        private final EnumMap<Keyspace, PreparedStatement> ascending;
        private final EnumMap<Keyspace, PreparedStatement> descending;

        Reader(Connection connection) throws SQLException {
            this.connection = connection;
            this.gets = new EnumMap<>(Keyspace.class);
            this.ascending = new EnumMap<>(Keyspace.class);
            this.descending = new EnumMap<>(Keyspace.class);
            for (var keyspace : Keyspace.values()) {
                var table = table(keyspace);
                gets.put(keyspace, connection.prepareStatement("SELECT v FROM " + table + " WHERE k = ?"));
                ascending.put(keyspace, connection.prepareStatement("SELECT k, v FROM " + table + " WHERE k >= ? AND k < ? ORDER BY k ASC LIMIT ?"));
                descending.put(keyspace, connection.prepareStatement("SELECT k, v FROM " + table + " WHERE k >= ? AND k < ? ORDER BY k DESC LIMIT ?"));
            }
        }

        byte[] get(Keyspace keyspace, byte[] key) throws SQLException {
            var statement = gets.get(keyspace);
            statement.setBytes(1, key);
            try (var results = statement.executeQuery()) {
                return results.next() ? results.getBytes(1) : null;
            }
        }

        PreparedStatement scan(Keyspace keyspace, boolean descending) {
            return descending ? this.descending.get(keyspace) : ascending.get(keyspace);
        }
    }
}

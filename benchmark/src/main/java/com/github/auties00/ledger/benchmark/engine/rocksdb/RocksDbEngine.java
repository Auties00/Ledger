package com.github.auties00.ledger.benchmark.engine.rocksdb;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Directories;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;

/// RocksDB through JNI, one column family per key space, sharing one block cache.
///
/// A durable write sets `sync` on the write, so concurrent durable writers share forces through RocksDB's own group
/// commit; a deferred write reaches the write-ahead log without a force. Ranges are deleted with a range tombstone.
/// Options are RocksDB's defaults apart from the shared block cache.
public final class RocksDbEngine implements Engine {
    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final RocksDB database;
    private final DBOptions databaseOptions;
    private final ColumnFamilyOptions familyOptions;
    private final LRUCache cache;
    private final List<ColumnFamilyHandle> handles;
    private final EnumMap<Keyspace, ColumnFamilyHandle> families;
    private final WriteOptions durableWrites;
    private final WriteOptions deferredWrites;

    private RocksDbEngine(Path directory, RocksDB database, DBOptions databaseOptions, ColumnFamilyOptions familyOptions,
                          LRUCache cache, List<ColumnFamilyHandle> handles, EnumMap<Keyspace, ColumnFamilyHandle> families) {
        this.directory = directory;
        this.database = database;
        this.databaseOptions = databaseOptions;
        this.familyOptions = familyOptions;
        this.cache = cache;
        this.handles = handles;
        this.families = families;
        this.durableWrites = new WriteOptions().setSync(true);
        this.deferredWrites = new WriteOptions().setSync(false);
    }

    /// Opens or creates a database in `directory`.
    ///
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @return the open engine
    /// @throws IOException if RocksDB fails to open
    public static RocksDbEngine open(Path directory, EngineSettings settings) throws IOException {
        Files.createDirectories(directory);
        var cache = new LRUCache(settings.cacheBytes());
        var familyOptions = new ColumnFamilyOptions()
                .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
        var databaseOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        var descriptors = new ArrayList<ColumnFamilyDescriptor>();
        descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions));
        for (var keyspace : Keyspace.values()) {
            descriptors.add(new ColumnFamilyDescriptor(keyspace.name().getBytes(StandardCharsets.US_ASCII), familyOptions));
        }
        var handles = new ArrayList<ColumnFamilyHandle>();
        try {
            var database = RocksDB.open(databaseOptions, directory.toString(), descriptors, handles);
            var families = new EnumMap<Keyspace, ColumnFamilyHandle>(Keyspace.class);
            for (var keyspace : Keyspace.values()) {
                families.put(keyspace, handles.get(keyspace.ordinal() + 1));
            }
            return new RocksDbEngine(directory, database, databaseOptions, familyOptions, cache, handles, families);
        } catch (RocksDBException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public void write(WriteBatch batch, Durability durability) throws IOException {
        try (var rocksBatch = new org.rocksdb.WriteBatch()) {
            for (var i = 0; i < batch.size(); i++) {
                var family = families.get(batch.keyspace(i));
                var value = batch.value(i);
                if (value == null) {
                    rocksBatch.delete(family, batch.key(i));
                } else {
                    rocksBatch.put(family, batch.key(i), value);
                }
            }
            database.write(durability == Durability.DURABLE ? durableWrites : deferredWrites, rocksBatch);
        } catch (RocksDBException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) throws IOException {
        try {
            return database.get(families.get(keyspace), key);
        } catch (RocksDBException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        var visited = 0;
        try (var iterator = database.newIterator(families.get(keyspace))) {
            if (descending) {
                iterator.seekForPrev(to);
                if (iterator.isValid() && Arrays.equals(iterator.key(), to)) {
                    iterator.prev();
                }
                while (visited < limit && iterator.isValid()) {
                    var key = iterator.key();
                    if (Arrays.compareUnsigned(key, from) < 0) {
                        break;
                    }
                    visited++;
                    if (!visitor.visit(key, iterator.value())) {
                        break;
                    }
                    iterator.prev();
                }
            } else {
                iterator.seek(from);
                while (visited < limit && iterator.isValid()) {
                    var key = iterator.key();
                    if (Arrays.compareUnsigned(key, to) >= 0) {
                        break;
                    }
                    visited++;
                    if (!visitor.visit(key, iterator.value())) {
                        break;
                    }
                    iterator.next();
                }
            }
        }
        return visited;
    }

    @Override
    public void deleteRange(Keyspace keyspace, byte[] from, byte[] to, Durability durability) throws IOException {
        try {
            database.deleteRange(families.get(keyspace), durability == Durability.DURABLE ? durableWrites : deferredWrites, from, to);
        } catch (RocksDBException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public ReadView openReadView() {
        var snapshot = database.getSnapshot();
        var options = new ReadOptions().setSnapshot(snapshot);
        return new ReadView() {
            @Override
            public byte[] get(Keyspace keyspace, byte[] key) throws IOException {
                try {
                    return database.get(families.get(keyspace), options, key);
                } catch (RocksDBException exception) {
                    throw new IOException(exception);
                }
            }

            @Override
            public void close() {
                options.close();
                database.releaseSnapshot(snapshot);
            }
        };
    }

    /// {@inheritDoc}
    ///
    /// @implNote This implementation flushes the memtables of every column family to sorted files and waits for the
    /// flush; compactions it triggers go on in the background.
    @Override
    public void checkpoint() throws IOException {
        try (var options = new FlushOptions().setWaitForFlush(true)) {
            database.flush(options, handles);
        } catch (RocksDBException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public long sizeOnDisk() throws IOException {
        return Directories.size(directory);
    }

    @Override
    public void close() throws IOException {
        try {
            database.flushWal(true);
        } catch (RocksDBException exception) {
            throw new IOException(exception);
        } finally {
            for (var handle : handles) {
                handle.close();
            }
            database.close();
            durableWrites.close();
            deferredWrites.close();
            databaseOptions.close();
            familyOptions.close();
            cache.close();
        }
    }
}

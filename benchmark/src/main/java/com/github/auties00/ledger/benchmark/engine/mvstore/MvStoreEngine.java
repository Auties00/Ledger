package com.github.auties00.ledger.benchmark.engine.mvstore;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Directories;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.mvstore.type.ByteArrayDataType;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.locks.ReentrantLock;

/// H2 MVStore configured as Cobalt's current store configures it: one file, one map per key space, the background
/// writer left on.
///
/// A write applies its batch to the maps and calls `commit()`, which writes a chunk; a durable write then calls
/// `sync()`. Writers are serialized, and every durable write forces the file on its own: MVStore has no group
/// commit.
///
/// @implNote MVStore makes a batch spanning several maps atomic only through its `TransactionStore`, which this
/// adapter does not use, so its background writer can persist part of a batch. The adapter keeps the background
/// writer because it is what compacts the file, and Cobalt runs MVStore the same way.
public final class MvStoreEngine implements Engine {
    private final Path directory;
    private final MVStore store;
    private final EnumMap<Keyspace, MVMap<byte[], byte[]>> maps;
    private final ReentrantLock writeLock;

    private MvStoreEngine(Path directory, MVStore store, EnumMap<Keyspace, MVMap<byte[], byte[]>> maps) {
        this.directory = directory;
        this.store = store;
        this.maps = maps;
        this.writeLock = new ReentrantLock();
    }

    /// Opens or creates `store.mv` in `directory`.
    ///
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @param password the encryption password, or `null` to store plaintext
    /// @return the open engine
    /// @throws java.io.IOException if the directory cannot be created
    public static MvStoreEngine open(Path directory, EngineSettings settings, char[] password) throws java.io.IOException {
        Files.createDirectories(directory);
        var builder = new MVStore.Builder()
                .fileName(directory.resolve("store.mv").toString())
                .cacheSize(Math.toIntExact(Math.max(1, settings.cacheBytes() >> 20)));
        if (password != null) {
            builder.encryptionKey(password);
        }
        var store = builder.open();
        var maps = new EnumMap<Keyspace, MVMap<byte[], byte[]>>(Keyspace.class);
        for (var keyspace : Keyspace.values()) {
            var map = store.openMap(keyspace.name().toLowerCase(), new MVMap.Builder<byte[], byte[]>()
                    .keyType(UnsignedBytesType.INSTANCE)
                    .valueType(ByteArrayDataType.INSTANCE));
            maps.put(keyspace, map);
        }
        return new MvStoreEngine(directory, store, maps);
    }

    @Override
    public void write(WriteBatch batch, Durability durability) {
        writeLock.lock();
        try {
            for (var i = 0; i < batch.size(); i++) {
                var map = maps.get(batch.keyspace(i));
                var value = batch.value(i);
                if (value == null) {
                    map.remove(batch.key(i));
                } else {
                    map.put(batch.key(i), value);
                }
            }
            store.commit();
            if (durability == Durability.DURABLE) {
                store.sync();
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) {
        return copy(maps.get(keyspace).get(key));
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        return scan(maps.get(keyspace), from, to, descending, limit, visitor);
    }

    /// {@return a copy of `value`, or `null`: MVStore hands out the arrays its maps hold}
    private static byte[] copy(byte[] value) {
        return value == null ? null : value.clone();
    }

    private static int scan(MVMap<byte[], byte[]> map, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        var visited = 0;
        if (descending) {
            var start = map.lowerKey(to);
            if (start == null || Arrays.compareUnsigned(start, from) < 0) {
                return 0;
            }
            var cursor = map.cursor(start, from, true);
            while (visited < limit && cursor.hasNext()) {
                var key = cursor.next();
                visited++;
                if (!visitor.visit(key.clone(), cursor.getValue().clone())) {
                    break;
                }
            }
        } else {
            var cursor = map.cursor(from);
            while (visited < limit && cursor.hasNext()) {
                var key = cursor.next();
                if (Arrays.compareUnsigned(key, to) >= 0) {
                    break;
                }
                visited++;
                if (!visitor.visit(key.clone(), cursor.getValue().clone())) {
                    break;
                }
            }
        }
        return visited;
    }

    @Override
    public ReadView openReadView() {
        // Registering the version keeps the chunks it references from being reused while the view is open
        var usage = store.registerVersionUsage();
        var version = usage.version;
        var views = new EnumMap<Keyspace, MVMap<byte[], byte[]>>(Keyspace.class);
        for (var entry : maps.entrySet()) {
            views.put(entry.getKey(), entry.getValue().openVersion(version));
        }
        return new ReadView() {
            @Override
            public byte[] get(Keyspace keyspace, byte[] key) {
                return copy(views.get(keyspace).get(key));
            }

            @Override
            public void close() {
                store.deregisterVersionUsage(usage);
            }
        };
    }

    @Override
    public long sizeOnDisk() throws java.io.IOException {
        return Directories.size(directory);
    }

    @Override
    public void close() {
        store.close();
    }

    /// Keys compared as unsigned bytes, as every engine of the benchmark orders them.
    private static final class UnsignedBytesType extends BasicDataType<byte[]> {
        private static final UnsignedBytesType INSTANCE = new UnsignedBytesType();

        @Override
        public int getMemory(byte[] obj) {
            return ByteArrayDataType.INSTANCE.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, byte[] obj) {
            ByteArrayDataType.INSTANCE.write(buff, obj);
        }

        @Override
        public byte[] read(ByteBuffer buff) {
            return ByteArrayDataType.INSTANCE.read(buff);
        }

        @Override
        public byte[][] createStorage(int size) {
            return new byte[size][];
        }

        @Override
        public int compare(byte[] a, byte[] b) {
            return Arrays.compareUnsigned(a, b);
        }
    }
}

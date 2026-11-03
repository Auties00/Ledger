package com.github.auties00.ledger.benchmark.engine.lmdb;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import org.lmdbjava.ByteArrayProxy;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.KeyRange;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.locks.ReentrantLock;

/// LMDB through lmdbjava, one named database per key space, reading through a memory map.
///
/// A deferred write commits with `MDB_NOSYNC`, which survives a crash of the process. A durable write commits with
/// `MDB_NOSYNC` cleared, so LMDB forces the data pages and then the meta page, as it does by default. LMDB allows one
/// writer at a time and has no group commit, so durable writers queue behind each other's forces. The map is sized
/// up front at twice [EngineSettings#expectedBytes()] plus 64 MiB; a workload that outgrows it fails with
/// `MDB_MAP_FULL`. On Windows, LMDB 0.9 creates the data file at the full map size.
///
/// @implNote lmdbjava cannot change environment flags after opening, so the adapter calls `mdb_env_set_flags`
/// through lmdbjava's package-private binding. Where that fails it forces with `mdb_env_sync` after a `MDB_NOSYNC`
/// commit instead, which costs one force instead of two and gives up LMDB's write ordering, and says so on standard
/// error.
public final class LmdbEngine implements Engine {
    private static final int NO_SYNC = EnvFlags.MDB_NOSYNC.getMask();

    private final Path directory;
    private final Env<byte[]> environment;
    private final EnumMap<Keyspace, Dbi<byte[]>> databases;
    private final ReentrantLock writeLock;
    private final SyncSwitch syncSwitch;

    private LmdbEngine(Path directory, Env<byte[]> environment, EnumMap<Keyspace, Dbi<byte[]>> databases) {
        this.directory = directory;
        this.environment = environment;
        this.databases = databases;
        this.writeLock = new ReentrantLock();
        this.syncSwitch = SyncSwitch.of(environment);
    }

    /// Opens or creates an environment in `directory`.
    ///
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @return the open engine
    /// @throws IOException if the directory cannot be created
    public static LmdbEngine open(Path directory, EngineSettings settings) throws IOException {
        Files.createDirectories(directory);
        var mapSize = settings.expectedBytes() * 2 + (64L << 20);
        var environment = Env.create(ByteArrayProxy.PROXY_BA)
                .setMapSize(mapSize)
                .setMaxDbs(Keyspace.values().length)
                .setMaxReaders(4096)
                .open(directory.toFile(), EnvFlags.MDB_NOTLS, EnvFlags.MDB_NOSYNC);
        var databases = new EnumMap<Keyspace, Dbi<byte[]>>(Keyspace.class);
        for (var keyspace : Keyspace.values()) {
            // LMDB orders keys by memcmp, but lmdbjava checks range bounds in Java with a signed comparator unless
            // given one; false keeps LMDB's native order rather than calling back into Java
            databases.put(keyspace, environment.openDbi(keyspace.name(), Arrays::compareUnsigned, false, DbiFlags.MDB_CREATE));
        }
        return new LmdbEngine(directory, environment, databases);
    }

    @Override
    public void write(WriteBatch batch, Durability durability) {
        writeLock.lock();
        try {
            var synchronous = durability == Durability.DURABLE && syncSwitch != null;
            if (synchronous) {
                syncSwitch.noSync(false);
            }
            try (var transaction = environment.txnWrite()) {
                for (var i = 0; i < batch.size(); i++) {
                    var database = databases.get(batch.keyspace(i));
                    var value = batch.value(i);
                    if (value == null) {
                        database.delete(transaction, batch.key(i));
                    } else {
                        database.put(transaction, batch.key(i), value);
                    }
                }
                transaction.commit();
            } finally {
                if (synchronous) {
                    syncSwitch.noSync(true);
                }
            }
            if (durability == Durability.DURABLE && syncSwitch == null) {
                environment.sync(true);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) {
        try (var transaction = environment.txnRead()) {
            return databases.get(keyspace).get(transaction, key);
        }
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        var range = descending ? KeyRange.openClosedBackward(to, from) : KeyRange.closedOpen(from, to);
        var visited = 0;
        try (var transaction = environment.txnRead();
             var entries = databases.get(keyspace).iterate(transaction, range)) {
            for (var entry : entries) {
                if (visited >= limit) {
                    break;
                }
                visited++;
                if (!visitor.visit(entry.key(), entry.val())) {
                    break;
                }
            }
        }
        return visited;
    }

    @Override
    public ReadView openReadView() {
        var transaction = environment.txnRead();
        return new ReadView() {
            @Override
            public byte[] get(Keyspace keyspace, byte[] key) {
                return databases.get(keyspace).get(transaction, key);
            }

            @Override
            public void close() {
                transaction.close();
            }
        };
    }

    /// {@inheritDoc}
    ///
    /// @implNote This implementation counts the pages up to the highest one in use: LMDB extends its data file to
    /// the whole map size on some platforms, so the file size says nothing about the data.
    @Override
    public long sizeOnDisk() throws IOException {
        var info = environment.info();
        var pageSize = environment.stat().pageSize;
        var lockFile = directory.resolve("lock.mdb");
        return (info.lastPageNumber + 1) * pageSize + (Files.exists(lockFile) ? Files.size(lockFile) : 0);
    }

    @Override
    public void close() {
        environment.sync(true);
        environment.close();
    }

    /// Turns `MDB_NOSYNC` on and off on an open environment through lmdbjava's package-private binding.
    private record SyncSwitch(Object library, Method setFlags, Object pointer) {
        static SyncSwitch of(Env<?> environment) {
            try {
                var libraryField = Class.forName("org.lmdbjava.Library").getDeclaredField("LIB");
                libraryField.setAccessible(true);
                var setFlags = Class.forName("org.lmdbjava.Library$Lmdb")
                        .getMethod("mdb_env_set_flags", jnr.ffi.Pointer.class, int.class, int.class);
                setFlags.setAccessible(true);
                var pointer = Env.class.getDeclaredMethod("pointer");
                pointer.setAccessible(true);
                return new SyncSwitch(libraryField.get(null), setFlags, pointer.invoke(environment));
            } catch (ReflectiveOperationException | RuntimeException exception) {
                System.err.println("lmdb: cannot toggle MDB_NOSYNC (" + exception + "), durable commits force once with mdb_env_sync");
                return null;
            }
        }

        void noSync(boolean enabled) {
            try {
                var result = (int) setFlags.invoke(library, pointer, NO_SYNC, enabled ? 1 : 0);
                if (result != 0) {
                    throw new IllegalStateException("mdb_env_set_flags failed: " + result);
                }
            } catch (IllegalAccessException | InvocationTargetException exception) {
                throw new IllegalStateException(exception);
            }
        }
    }
}

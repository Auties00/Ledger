package com.github.auties00.ledger.benchmark.engine.ledger;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Directories;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Locale;

/// Ledger, as Cobalt will open it: one store, a resident key space for the small records read on every message
/// (sessions, chats) and paged key spaces for the rest.
///
/// A durable write commits and waits for its commit to be durable, so concurrent writers share forces; a deferred
/// write only commits. Range deletes are Ledger's own, which records the range rather than visiting its keys.
public final class LedgerEngine implements Engine {
    // Every store of the benchmark opens under the same key, so a template opens in the next run
    private static final byte[] KEY = new byte[32];

    private final Ledger ledger;
    private final EnumMap<Keyspace, com.github.auties00.ledger.Keyspace<byte[], byte[]>> keyspaces;
    private final Path directory;

    private LedgerEngine(Ledger ledger, EnumMap<Keyspace, com.github.auties00.ledger.Keyspace<byte[], byte[]>> keyspaces, Path directory) {
        this.ledger = ledger;
        this.keyspaces = keyspaces;
        this.directory = directory;
    }

    /// Opens or creates the store in `directory`.
    ///
    /// @param settings the shared settings; their cache becomes the page cache
    /// @param encrypted whether the store encrypts its files, with AES-256-GCM
    /// @return the open engine
    public static LedgerEngine open(Path directory, EngineSettings settings, boolean encrypted) {
        var keyspaces = new EnumMap<Keyspace, com.github.auties00.ledger.Keyspace<byte[], byte[]>>(Keyspace.class);
        var options = Ledger.builder()
                .directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .pageCache(Math.max(1L << 20, settings.cacheBytes()))
                .largeValues(settings.largeValues());
        for (var keyspace : Keyspace.values()) {
            var name = keyspace.name().toLowerCase(Locale.ROOT).replace('_', '-');
            var resident = keyspace == Keyspace.SESSIONS || keyspace == Keyspace.CHATS;
            var mapped = resident
                    ? com.github.auties00.ledger.Keyspace.resident(name, KeyCodec.bytes(), ValueCodec.bytes())
                    : com.github.auties00.ledger.Keyspace.paged(name, KeyCodec.bytes(), ValueCodec.bytes());
            keyspaces.put(keyspace, mapped);
            options.keyspace(mapped);
        }
        return new LedgerEngine(options.build(), keyspaces, directory);
    }

    @Override
    public void write(WriteBatch batch, Durability durability) throws IOException {
        var writes = ledger.newBatch();
        for (var i = 0; i < batch.size(); i++) {
            var keyspace = keyspaces.get(batch.keyspace(i));
            var value = batch.value(i);
            if (value == null) {
                writes.delete(keyspace, batch.key(i));
            } else {
                writes.put(keyspace, batch.key(i), value);
            }
        }
        var commit = ledger.commit(writes);
        if (durability == Durability.DURABLE) {
            await(commit);
        }
    }

    private static void await(com.github.auties00.ledger.Commit commit) throws IOException {
        try {
            commit.awaitDurable();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for durability");
        }
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) {
        return ledger.get(keyspaces.get(keyspace), key).orElse(null);
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        var range = KeyRange.between(from, to);
        var visited = new int[1];
        ledger.forEachEncoded(keyspaces.get(keyspace), descending ? range.reversed() : range, (key, value) -> {
            visited[0]++;
            return visitor.visit(bytes(key), bytes(value)) && visited[0] < limit;
        });
        return visited[0];
    }

    @Override
    public void deleteRange(Keyspace keyspace, byte[] from, byte[] to, Durability durability) throws IOException {
        var commit = ledger.commit(ledger.newBatch().deleteRange(keyspaces.get(keyspace), KeyRange.between(from, to)));
        if (durability == Durability.DURABLE) {
            await(commit);
        }
    }

    @Override
    public ReadView openReadView() {
        var snapshot = ledger.snapshot();
        return new LedgerReadView(snapshot, keyspaces);
    }

    @Override
    public long sizeOnDisk() throws IOException {
        return Directories.size(directory);
    }

    @Override
    public void close() {
        ledger.close();
    }

    /// {@return the bytes of `segment`, in a new array}
    ///
    /// `MemorySegment.toArray` would build the name of the array class on every call, a cost the other engines do not
    /// pay for their arrays.
    private static byte[] bytes(MemorySegment segment) {
        var array = new byte[Math.toIntExact(segment.byteSize())];
        MemorySegment.copy(segment, 0, MemorySegment.ofArray(array), 0, array.length);
        return array;
    }

    /// A read view over a [Snapshot].
    private record LedgerReadView(Snapshot snapshot, EnumMap<Keyspace, com.github.auties00.ledger.Keyspace<byte[], byte[]>> keyspaces)
            implements ReadView {
        @Override
        public byte[] get(Keyspace keyspace, byte[] key) {
            return snapshot.get(keyspaces.get(keyspace), key).orElse(null);
        }

        @Override
        public void close() {
            snapshot.close();
        }
    }
}

package com.github.auties00.ledger.benchmark.engine.floor;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Directories;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

/// The cost floor: appends every batch to one file and keeps the whole index in memory.
///
/// A durable write forces the file after its append, outside the append lock, so concurrent durable writers can
/// share a force where the OS allows it. Nothing is ever read back, compacted or recovered, and reads are served from
/// memory: the numbers bound what the device charges for the bytes and forces of a workload, not what an engine can
/// do. A reopened floor is empty.
public final class AppendLogFloor implements Engine {
    private final Path directory;
    private final FileChannel log;
    private final EnumMap<Keyspace, ConcurrentSkipListMap<byte[], byte[]>> index;
    private final ReentrantLock appendLock;
    private ByteBuffer buffer;

    private AppendLogFloor(Path directory, FileChannel log) {
        this.directory = directory;
        this.log = log;
        this.index = new EnumMap<>(Keyspace.class);
        for (var keyspace : Keyspace.values()) {
            index.put(keyspace, new ConcurrentSkipListMap<>(Arrays::compareUnsigned));
        }
        this.appendLock = new ReentrantLock();
        this.buffer = ByteBuffer.allocateDirect(1 << 20);
    }

    /// Creates an empty log in `directory`, replacing any earlier one.
    ///
    /// @param directory the directory the floor owns
    /// @return the open floor
    /// @throws IOException if the log cannot be created
    public static AppendLogFloor open(Path directory) throws IOException {
        Files.createDirectories(directory);
        var log = FileChannel.open(directory.resolve("floor.log"), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        return new AppendLogFloor(directory, log);
    }

    @Override
    public void write(WriteBatch batch, Durability durability) throws IOException {
        append(batch, true, durability);
    }

    /// Appends `batch` to the log, applies it to the index when `apply` is set, then forces the log if `durability`
    /// asks for it.
    private void append(WriteBatch batch, boolean apply, Durability durability) throws IOException {
        appendLock.lock();
        try {
            var size = 4;
            for (var i = 0; i < batch.size(); i++) {
                var value = batch.value(i);
                size += 9 + batch.key(i).length + (value == null ? 0 : value.length);
            }
            if (buffer.capacity() < size) {
                buffer = ByteBuffer.allocateDirect(Integer.highestOneBit(size) << 1);
            }
            buffer.clear();
            buffer.putInt(batch.size());
            for (var i = 0; i < batch.size(); i++) {
                var key = batch.key(i);
                var value = batch.value(i);
                buffer.put((byte) batch.keyspace(i).ordinal());
                buffer.putInt(key.length);
                buffer.put(key);
                buffer.putInt(value == null ? -1 : value.length);
                if (value != null) {
                    buffer.put(value);
                }
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                log.write(buffer);
            }
            for (var i = 0; apply && i < batch.size(); i++) {
                var keyspace = index.get(batch.keyspace(i));
                var value = batch.value(i);
                if (value == null) {
                    keyspace.remove(batch.key(i));
                } else {
                    keyspace.put(batch.key(i), value);
                }
            }
        } finally {
            appendLock.unlock();
        }
        if (durability == Durability.DURABLE) {
            log.force(false);
        }
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) {
        var value = index.get(keyspace).get(key);
        return value == null ? null : value.clone();
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) {
        var range = index.get(keyspace).subMap(from, true, to, false);
        var entries = descending ? range.descendingMap() : range;
        var visited = 0;
        for (var entry : entries.entrySet()) {
            if (visited >= limit) {
                break;
            }
            visited++;
            if (!visitor.visit(entry.getKey().clone(), entry.getValue().clone())) {
                break;
            }
        }
        return visited;
    }

    @Override
    public void deleteRange(Keyspace keyspace, byte[] from, byte[] to, Durability durability) throws IOException {
        // The two bounds stand in for a range record, as large as an engine's range tombstone
        append(new WriteBatch().delete(keyspace, from).delete(keyspace, to), false, durability);
        index.get(keyspace).subMap(from, true, to, false).clear();
    }

    @Override
    public ReadView openReadView() {
        throw new UnsupportedOperationException("The floor keeps no versions");
    }

    @Override
    public long sizeOnDisk() throws IOException {
        return Directories.size(directory);
    }

    @Override
    public void close() throws IOException {
        log.force(false);
        log.close();
    }
}

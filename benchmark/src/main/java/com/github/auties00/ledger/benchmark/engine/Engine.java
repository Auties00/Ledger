package com.github.auties00.ledger.benchmark.engine;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

/// One storage engine, adapted to the operations the workloads need.
///
/// Every key space of [Keyspace] exists from the moment the engine opens. Keys are 1 to 511 bytes long, the range
/// every engine accepts (LMDB's limit), and compare as unsigned bytes.
///
/// # Threading
/// Every method may be called from any thread, virtual threads included, and concurrently with any other method
/// except [#close()]. An engine that allows only one writer serializes writers itself.
public interface Engine extends AutoCloseable {
    /// Applies `batch` atomically.
    ///
    /// @param batch the operations to apply
    /// @param durability when the operations must reach stable storage
    /// @throws IOException if the engine fails to write
    void write(WriteBatch batch, Durability durability) throws IOException;

    /// {@return the value stored under `key`, in an array the caller owns, or `null` if there is none}
    ///
    /// An engine that keeps the arrays it stores hands out copies: a caller may change what it gets, and handing out
    /// the engine's own would skip the copy every other engine makes.
    ///
    /// @param keyspace the key space to read
    /// @param key the key to look up
    /// @throws IOException if the engine fails to read
    byte[] get(Keyspace keyspace, byte[] key) throws IOException;

    /// Visits the entries whose keys are in `[from, to)`, in ascending or descending key order, until `limit`
    /// entries were visited or the visitor returns `false`.
    ///
    /// @param keyspace the key space to read
    /// @param from the lowest key included
    /// @param to the first key excluded
    /// @param descending whether to start from the highest key
    /// @param limit the most entries to visit
    /// @param visitor receives each entry; the arrays it receives may be reused after it returns
    /// @return the number of entries visited
    /// @throws IOException if the engine fails to read
    int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) throws IOException;

    /// Deletes every entry whose key is in `[from, to)`.
    ///
    /// @implSpec The default implementation scans the range and deletes the keys it finds in one batch.
    /// @param keyspace the key space to write
    /// @param from the lowest key deleted
    /// @param to the first key kept
    /// @param durability when the deletion must reach stable storage
    /// @throws IOException if the engine fails to read or write
    default void deleteRange(Keyspace keyspace, byte[] from, byte[] to, Durability durability) throws IOException {
        var batch = new WriteBatch();
        scan(keyspace, from, to, false, Integer.MAX_VALUE, (key, _) -> {
            batch.delete(keyspace, key.clone());
            return true;
        });
        write(batch, durability);
    }

    /// Replaces the value under `key` with what `function` makes of it, so that no other compute of the key comes
    /// between the value read and the value written.
    ///
    /// `function` receives the value, `null` if there is none, and returns the new value, `null` to delete the key, or
    /// the array it received to leave it unchanged; it may run more than once, and must not change the array it
    /// receives.
    ///
    /// @implSpec The default implementation calls [#computeIn] with an empty batch.
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param function makes the new value from the current one
    /// @param durability when the write must reach stable storage
    /// @throws IOException if the engine fails to read or write
    default void compute(Keyspace keyspace, byte[] key, UnaryOperator<byte[]> function, Durability durability) throws IOException {
        computeIn(new WriteBatch(), keyspace, key, function, durability);
    }

    /// Applies `batch` and what `function` makes of the value under `key`, atomically, as [#compute] does for the key.
    ///
    /// @implSpec The default implementation holds a lock of [#STRIPES] for the key, which every compute of the key takes,
    /// while it reads the value, applies the function and writes the batch with the result: the per-key lock an
    /// application adds over an engine that has no read-modify-write of its own, which every writer of the key must take.
    /// @param batch the other writes, which this call may extend
    /// @param keyspace the key space of the computed key
    /// @param key the computed key
    /// @param function makes the new value from the current one
    /// @param durability when the writes must reach stable storage
    /// @throws IOException if the engine fails to read or write
    default void computeIn(WriteBatch batch, Keyspace keyspace, byte[] key, UnaryOperator<byte[]> function,
                           Durability durability) throws IOException {
        var lock = STRIPES[(System.identityHashCode(this) * 31 + keyspace.ordinal() * 17 + Arrays.hashCode(key)) & (STRIPES.length - 1)];
        lock.lock();
        try {
            var current = get(keyspace, key);
            var result = function.apply(current);
            if (result != current) {
                if (result == null) {
                    batch.delete(keyspace, key);
                } else {
                    batch.put(keyspace, key, result);
                }
            }
            if (batch.size() > 0) {
                write(batch, durability);
            }
        } finally {
            lock.unlock();
        }
    }

    /// {@return the number of entries of `keyspace` whose keys are in `[from, to)`}
    ///
    /// @implSpec The default implementation scans the range.
    /// @param keyspace the key space to count
    /// @param from the lowest key counted
    /// @param to the first key not counted
    /// @throws IOException if the engine fails to read
    default long count(Keyspace keyspace, byte[] from, byte[] to) throws IOException {
        return scan(keyspace, from, to, false, Integer.MAX_VALUE, (_, _) -> true);
    }

    /// The 256 locks the default [#computeIn] stripes the computed keys of every engine over.
    ReentrantLock[] STRIPES = java.util.stream.Stream.generate(ReentrantLock::new).limit(256).toArray(ReentrantLock[]::new);

    /// Opens a read view that keeps seeing the current state while later writes go on.
    ///
    /// A workload holds one open to measure what a long reader costs the writers, for example in file growth.
    ///
    /// @return the read view, which the caller closes
    /// @throws IOException if the engine fails to open it
    ReadView openReadView() throws IOException;

    /// Moves what the engine holds of recent writes only in memory or in its log into its main files, as it does on
    /// its own from time to time, and returns once that is done: a checkpoint.
    ///
    /// A workload calls it to measure what that step costs while writes go on, not for durability, which
    /// [Durability#DURABLE] already gives.
    ///
    /// @implSpec The default implementation does nothing, for an engine whose writes reach its main files as they
    /// commit.
    /// @throws IOException if the engine fails to write
    default void checkpoint() throws IOException {
    }

    /// {@return the bytes the engine's files occupy}
    ///
    /// An engine that reserves file space before it uses it reports the space its data takes instead.
    ///
    /// @throws IOException if the files cannot be measured
    long sizeOnDisk() throws IOException;

    /// Closes the engine; writes made with [Durability#DEFERRED] are persisted first.
    ///
    /// @throws IOException if the engine fails to persist or release its files
    @Override
    void close() throws IOException;

    /// A consistent view of the store at the moment it was opened.
    interface ReadView extends AutoCloseable {
        /// {@return the value stored under `key` when the view was opened, in an array the caller owns, or `null` if
        /// there was none}
        ///
        /// @param keyspace the key space to read
        /// @param key the key to look up
        /// @throws IOException if the engine fails to read
        byte[] get(Keyspace keyspace, byte[] key) throws IOException;

        @Override
        void close() throws IOException;
    }

    /// Receives the entries of a scan.
    @FunctionalInterface
    interface EntryVisitor {
        /// Receives one entry, in arrays the visitor owns.
        ///
        /// @param key the entry's key
        /// @param value the entry's value
        /// @return whether to continue the scan
        boolean visit(byte[] key, byte[] value);
    }
}

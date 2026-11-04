package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.LedgerBuilder;

import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;

/// An open store.
///
/// {@snippet lang = java :
/// var sessions = Keyspace.resident("sessions", KeyCodec.utf8(), SessionCodec.INSTANCE);
/// var messages = Keyspace.paged("messages", MessageKey.CODEC, MessageCodec.INSTANCE);
/// try (var ledger = Ledger.builder()
///         .directory(Path.of("account"))
///         .encryption(Encryption.key(masterKey))
///         .keyspace(sessions)
///         .keyspace(messages)
///         .build()) {
///     var commit = ledger.commit(ledger.newBatch()
///             .put(messages, key, message)
///             .put(sessions, address, session));
///     commit.awaitDurable(); // before acknowledging the message
///     var page = ledger.range(messages, MessageKey.chat(chat).reversed(), 50);
/// }
/// }
///
/// # Writes
/// Writes are atomic batches, applied one at a time in the order they are committed; any number of threads may
/// commit concurrently. A commit is visible to every read that starts after it returns, and durable later:
/// [Commit#awaitDurable()] waits for one commit, [#awaitDurable()] for every commit made so far. Concurrent commits
/// share the forces that make them durable, so a thousand threads waiting for durability cost the device about as
/// much as one.
///
/// The writes since the last checkpoint stay in memory until the next one. When commits come faster than
/// checkpoints can write them, and the log holds three checkpoint intervals ([Options#checkpointEvery(long)]) the
/// files do not, a commit waits for the running checkpoint before it applies: memory stays bounded, at the cost of
/// throughput.
///
/// # Reads
/// Reads never wait for writes and never see part of a batch. The read methods of this interface read the latest
/// state, each on its own; a [Snapshot] reads one state for as long as it stays open.
///
/// # Durability
/// A durable commit survives a crash of the process and a loss of power. A commit that is visible but not yet
/// durable survives a crash of the process only. When the store opens it recovers every durable commit and drops
/// the rest; it never recovers part of a batch.
///
/// A failed force leaves the store unable to tell which commits reached stable storage, so it stops: every later
/// commit, and every wait for durability, throws [LedgerIoException]. Reopen the store to recover.
///
/// On macOS a force reaches stable storage only when the application grants the store's module native access,
/// with `--enable-native-access=com.github.auties00.ledger`: the store then empties the drive's cache with
/// `fcntl(F_FULLFSYNC)`, as the JDK does not. Without it, a durable commit survives a crash of the system, but a loss
/// of power may take it from the drive's cache.
///
/// # Memory
/// The store keeps the bytes it encrypts, decrypts and writes on the Java heap, in arrays it recycles: the JDK's
/// AES-GCM runs fastest on arrays, and recycling keeps the memory from becoming garbage. The page cache
/// ([Options#pageCache(long)]) is part of that memory, so it counts against the heap's maximum size. Codecs read and
/// write that memory in place, through [java.lang.foreign.MemorySegment] views.
///
/// # Threading
/// Every method may be called from any thread, virtual threads included. Calls that wait (a commit while the
/// writer is busy, a wait for durability) park the calling thread without holding a carrier. The store writes and
/// forces its log on the platform threads of its [IoPool], and writes the checkpoints it starts on its own on a
/// platform thread of their own; a read of a page the cache does not hold reads the file on the calling thread.
///
/// # Lifecycle
/// A store holds an exclusive lock on its directory from [Options#build()] until [#close()]: one process, and one
/// store in it, writes a directory at a time.
public non-sealed interface Ledger extends View, AutoCloseable {
    /// {@return a builder that opens a store}
    static Builder builder() {
        return new LedgerBuilder();
    }

    /// {@return an empty batch of writes to this store}
    ///
    /// @throws IllegalStateException if the store is closed
    WriteBatch newBatch();

    /// Applies `batch` atomically.
    ///
    /// The batch is visible when this method returns; the returned commit says when it becomes durable. The batch
    /// may be reused once this method returns.
    ///
    /// @param batch the writes to apply
    /// @return the commit, to wait on for durability
    /// @throws IllegalArgumentException if another store created `batch`
    /// @throws LedgerIoException if the store has stopped after a failed write
    /// @throws IllegalStateException if the store is closed
    Commit commit(WriteBatch batch);

    /// Runs `transaction` alone against the latest state and applies its writes atomically.
    ///
    /// The transaction reads the latest state and its own writes, and no other write runs while it does, so a
    /// value it reads cannot change before its writes apply. Its writes are visible when this method returns; the
    /// returned commit says when they become durable. If `transaction` throws, none of its writes apply and the
    /// exception propagates.
    ///
    /// @apiNote Other commits wait while the transaction runs: keep it to reading and writing. [#commit(WriteBatch)]
    /// encodes its values before it waits for the writer, so blind writes are cheaper through it.
    /// @param transaction reads and writes the store; it must not keep the [Transaction] it receives
    /// @return the commit, to wait on for durability
    /// @throws LedgerIoException if the store has stopped after a failed write
    /// @throws IllegalStateException if the store is closed
    Commit update(Consumer<Transaction> transaction);

    /// Waits until every commit made before this call is durable.
    ///
    /// @throws InterruptedException if interrupted while waiting; the commits still become durable
    /// @throws LedgerIoException if a force failed
    /// @throws IllegalStateException if the store is closed
    void awaitDurable() throws InterruptedException;

    /// Opens a snapshot of the latest state.
    ///
    /// @apiNote A snapshot keeps the pages of its state from being reused, so the files grow while one stays open
    /// under steady writes. Close it as soon as its reads are done.
    /// @return the snapshot, which the caller closes
    /// @throws IllegalStateException if the store is closed
    Snapshot snapshot();

    /// Writes a checkpoint now: the pages of every commit so far, so that the next open replays less of the log.
    ///
    /// The store also checkpoints on its own, as [Options#checkpointEvery(long)] says; calling this method is never
    /// needed for durability.
    ///
    /// @throws LedgerIoException if the checkpoint cannot be written
    /// @throws IllegalStateException if the store is closed
    void checkpoint();

    /// Makes every commit durable, writes a checkpoint and releases the directory.
    ///
    /// Commits and reads that are running finish first; later calls throw [IllegalStateException]. Closing a
    /// closed store does nothing.
    ///
    /// @throws LedgerIoException if the commits cannot be made durable or the checkpoint cannot be written
    @Override
    void close();

    /// The first stage of a [Ledger] builder: where the store keeps its data.
    sealed interface Builder permits LedgerBuilder {
        /// Keeps the store in `directory`, creating it and its files if needed.
        ///
        /// @param directory the directory the store owns; nothing else should write to it
        /// @return the next stage, which chooses the encryption
        EncryptionStage directory(Path directory);

        /// Keeps the store in memory only: nothing is written to disk, every commit is durable at once, and the
        /// data is gone when the store closes.
        ///
        /// @return the options
        Options memory();
    }

    /// The stage of a [Ledger] builder that chooses how the files are encrypted.
    sealed interface EncryptionStage permits LedgerBuilder {
        /// Encrypts the files as `encryption` says.
        ///
        /// A store opened again must use the encryption it was created with; a store created with
        /// [Encryption#none()] stays plaintext.
        ///
        /// @param encryption the encryption
        /// @return the options
        Options encryption(Encryption encryption);
    }

    /// The options of a [Ledger] builder.
    sealed interface Options permits LedgerBuilder {
        /// Opens the store with `keyspace`, creating it if the store does not have it yet.
        ///
        /// Key spaces the store has but was not opened with are kept untouched.
        ///
        /// @param keyspace the key space
        /// @return these options
        /// @throws IllegalArgumentException if another key space with the same name was added
        Options keyspace(Keyspace<?, ?> keyspace);

        /// Runs the store's I/O on `pool`, which several stores may share.
        ///
        /// By default each store has a pool of its own, closed with it.
        ///
        /// @param pool the pool; the store does not close it
        /// @return these options
        Options ioPool(IoPool pool);

        /// Limits the memory that caches the pages of [Keyspace.Residency#PAGED] key spaces.
        ///
        /// The cache is on the Java heap, so it counts against the heap's maximum size. The default is 64 MiB.
        /// Resident key spaces are held whole, outside this limit.
        ///
        /// @param bytes the limit, at least 1 MiB
        /// @return these options
        /// @throws IllegalArgumentException if `bytes` is below 1 MiB
        Options pageCache(long bytes);

        /// Writes a checkpoint whenever the log has grown by `logBytes` since the last one.
        ///
        /// Fewer checkpoints write less; more checkpoints open faster, since an open replays the log written since
        /// the last checkpoint. The default is 64 MiB.
        ///
        /// @param logBytes the log growth between checkpoints, at least 1 MiB
        /// @return these options
        /// @throws IllegalArgumentException if `logBytes` is below 1 MiB
        Options checkpointEvery(long logBytes);

        /// Sends the failures of the work the store does on its own threads to `listener`: a checkpoint it started
        /// that could not be written.
        ///
        /// The store goes on after such a failure, since its log still holds every commit, and its next checkpoint
        /// retries; until one succeeds the log grows and opening replays more of it. A failure of the log itself stops
        /// the store instead, and every later commit throws it. Without a listener, these failures are not reported.
        ///
        /// @param listener receives each failure on the thread that met it, and must not block for long
        /// @return these options
        Options onBackgroundFailure(Consumer<? super LedgerException> listener);

        /// Lets a commit wait up to `window` for other commits to join the force it starts.
        ///
        /// The default, zero, starts each force as soon as the previous one ends, which already groups the commits
        /// that arrive during a force. A window trades the latency of a lone commit for fewer forces under a burst.
        ///
        /// @param window the longest wait, from zero to one second
        /// @return these options
        /// @throws IllegalArgumentException if `window` is negative or longer than one second
        Options commitWindow(Duration window);

        /// Opens the store.
        ///
        /// @return the open store
        /// @throws LedgerLockedException if another process, or another store in this process, holds the directory
        /// @throws LedgerKeyException if the store was created with another encryption
        /// @throws LedgerCorruptedException if a file fails authentication beyond what recovery repairs
        /// @throws LedgerIoException if the files cannot be read or written
        Ledger build();
    }
}

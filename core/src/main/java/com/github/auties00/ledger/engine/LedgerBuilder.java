package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.function.Consumer;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/// Every stage of a [Ledger] builder: the stages are views of one object that collects the options.
public final class LedgerBuilder implements Ledger.Builder, Ledger.EncryptionStage, Ledger.Options {
    private static final long MIB = 1L << 20;
    private static final String LOG_DIRECTORY = "log";
    // A running store keeps one obsolete segment of this length as a spare; a closed one keeps no log
    private static final long SEGMENT_SIZE = 16 * MIB;

    private Path directory;
    private Encryption encryption;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private IoPool ioPool;
    private long pageCache;
    private long checkpointEvery;
    private Duration commitWindow;
    private long segmentSize;
    private long checkpointHeap;
    private Consumer<? super LedgerException> backgroundFailures;
    private long largeValues;
    private final Map<String, Long> largeValuesByKeyspace;
    // Null for the mode the system property names
    private WriteThrough.Mode writeThrough;
    // Null to make one from the mode
    private WriteThrough writeThroughWay;
    // Null for the mode the system property names
    private LogWriter.Overlap overlap;
    private boolean residentAtOpen;

    /// Creates a builder with the default options.
    public LedgerBuilder() {
        this.keyspaces = new LinkedHashMap<>();
        this.pageCache = defaultPageCache();
        this.checkpointEvery = defaultCheckpointEvery();
        this.commitWindow = Duration.ZERO;
        this.segmentSize = SEGMENT_SIZE;
        // The writes the trees lack may hold up to half the heap before commits wait for more than two intervals
        this.checkpointHeap = Runtime.getRuntime().maxMemory() / 2;
        this.backgroundFailures = _ -> {
        };
        this.largeValues = MIB;
        this.largeValuesByKeyspace = new LinkedHashMap<>();
    }

    /// Sets the size past which the log starts a new segment, so tests can roll segments over without writing
    /// 64 MiB.
    LedgerBuilder segmentSize(long bytes) {
        this.segmentSize = bytes;
        return this;
    }

    /// Sets how the log writes the groups that one write holds, in place of the system property
    /// `ledger.log.writeThrough`, so tests can write through on a file system that simulates failures, and force on
    /// one that would write through.
    LedgerBuilder writeThrough(WriteThrough.Mode mode) {
        this.writeThrough = Objects.requireNonNull(mode, "mode");
        return this;
    }

    /// Sets the way the log starts writing groups through, in place of the one a mode gives, so tests can start from
    /// a way their platform would not choose and from what a device reports. It serves one build.
    LedgerBuilder writeThrough(WriteThrough way) {
        this.writeThroughWay = Objects.requireNonNull(way, "way");
        return this;
    }

    /// Sets whether the log writes a group through while the one before it is still on its way to the device, in place
    /// of the system property `ledger.log.overlap`, so tests can have groups go beside one another without waiting for
    /// the epochs that decide it, and keep them apart.
    LedgerBuilder overlap(LogWriter.Overlap mode) {
        this.overlap = Objects.requireNonNull(mode, "mode");
        return this;
    }

    /// Sets the heap the writes the trees of the process's stores lack may hold before this store's commits wait for
    /// a checkpoint, by the logs' estimates, so tests can reach it without filling half the heap.
    LedgerBuilder checkpointHeap(long bytes) {
        this.checkpointHeap = bytes;
        return this;
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
    public Ledger.Options onBackgroundFailure(Consumer<? super LedgerException> listener) {
        this.backgroundFailures = Objects.requireNonNull(listener, "listener");
        return this;
    }

    /// {@return the most memory the free arrays of the store's pool may hold: a quarter of the page cache, at least 16
    /// MiB}
    ///
    /// The pool recycles the arrays a store fills and empties over and over, batches, sealed pages and the buffers of
    /// large values, whose working set is a checkpoint's flush and a few buffers; as large as the page cache, it held
    /// up to as much again in arrays no one used.
    private long pooledBytes() {
        return Math.max(16 * MIB, pageCache / 4);
    }

    @Override
    public Ledger.Options pageCache(long bytes) {
        if (bytes < MIB) {
            throw new IllegalArgumentException("The page cache is at least 1 MiB: " + bytes);
        }
        this.pageCache = bytes;
        return this;
    }

    /// {@return the page cache limit of a store whose options do not choose one: a sixteenth of the heap's maximum size,
    /// between 1 MiB and 64 MiB}
    ///
    /// The pages it caches count against the heap: 64 MiB whatever its size was a quarter of a heap of 256 MiB.
    static long defaultPageCache() {
        return Math.clamp(Runtime.getRuntime().maxMemory() / 16, MIB, 64 * MIB);
    }

    /// {@return the checkpoint interval of a store whose options do not choose one: a sixteenth of the heap's maximum
    /// size, between 64 MiB and 512 MiB}
    ///
    /// A checkpoint of writes spread over a store larger than the interval rewrites a page for each write or two, so
    /// the interval sets the throughput of such writes: 256 MiB rather than 64 MiB made them three to four times
    /// faster. The writes it holds cost the heap about twice their bytes in the log.
    static long defaultCheckpointEvery() {
        return Math.clamp(Runtime.getRuntime().maxMemory() / 16, 64 * MIB, 512 * MIB);
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
    public Ledger.Options loadResidentAtOpen(boolean atOpen) {
        this.residentAtOpen = atOpen;
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
    public Ledger.Options largeValues(long bytes) {
        this.largeValues = checkLargeValues(bytes);
        return this;
    }

    @Override
    public Ledger.Options largeValues(Keyspace<?, ?> keyspace, long bytes) {
        Objects.requireNonNull(keyspace, "keyspace");
        largeValuesByKeyspace.put(keyspace.name(), checkLargeValues(bytes));
        return this;
    }

    private static long checkLargeValues(long bytes) {
        if (bytes < PageFormat.SIZE) {
            throw new IllegalArgumentException("Large values are at least 4 KiB: " + bytes);
        }
        return bytes;
    }

    @Override
    public Ledger build() {
        if (directory == null) {
            var ledger = new LiveLedger(keyspaces.values(), HeapPool.process());
            HeapPool.join(ledger, pooledBytes());
            ledger.assignIds();
            return ledger;
        }
        return openDirectory();
    }

    private Ledger openDirectory() {
        var encryption = (EncryptionSpec) this.encryption;
        DirectoryLock lock = null;
        PlatformIoPool ownedPool = null;
        PageFile pages = null;
        try {
            Files.createDirectories(directory);
            lock = DirectoryLock.acquire(directory);
            var logDirectory = directory.resolve(LOG_DIRECTORY);
            if (StoreHeader.read(directory) == null && (hasFiles(logDirectory) || Files.exists(directory.resolve(PageFile.FILE)))) {
                throw new LedgerCorruptedException(directory + " holds data but no header", null);
            }
            var opened = StoreHeader.openOrCreate(directory, encryption);
            Files.createDirectories(logDirectory);
            var heap = HeapPool.process();
            pages = PageFile.open(directory, new PageCipher(opened.keys().pageKey()), pageCache, heap);
            var ledger = new LiveLedger(keyspaces.values(), heap);
            HeapPool.join(ledger, pooledBytes());
            var superblock = pages.superblock();
            var blobPages = 0;
            var blobFree = new BitSet();
            if (superblock != null && superblock.blobFreeList() != null) {
                blobPages = superblock.blobPageCount();
                blobFree = Catalog.decodeFreeList(pages.read(superblock.blobFreeList(), PageFormat.FREE_LIST, false), blobPages);
            }
            // The epoch is fixed once recovery has read those of the opens since the checkpoint
            var blobs = BlobFile.open(directory, new PageCipher(opened.keys().blobKey()), pages.cache(), heap, blobPages, blobFree);
            pages.useBlobs(blobs);
            ledger.useBlobs(blobs, largeValues, largeValuesByKeyspace);
            var epochs = new int[]{superblock == null ? 0 : superblock.epoch()};
            FreeSpace space;
            if (superblock == null) {
                space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                ledger.useDataFile(pages, space);
            } else {
                var free = Catalog.decodeFreeList(pages.read(superblock.freeList(), PageFormat.FREE_LIST, false), superblock.pageCount());
                space = new FreeSpace(superblock.pageCount(), free);
                ledger.useDataFile(pages, space);
                ledger.loadCheckpoint(superblock, Catalog.decode(pages.read(superblock.catalog(), PageFormat.CATALOG, false)),
                        residentAtOpen, backgroundFailures);
            }
            var firstSegment = superblock == null ? 0 : superblock.logStart();
            var files = new LogFiles(logDirectory, opened.keys(), opened.header().storeId());
            files.deleteStale();
            // Segments before the superblock's first one: the process that wrote it stopped before its checkpoint
            // retired them, maybe before the superblock left the OS cache, which recovery is about to trust
            var crashed = !files.segments().isEmpty() && files.segments().getFirst() < firstSegment;
            if (crashed) {
                pages.force();
            }
            var recovered = LogRecovery.replay(files, opened.keys(), opened.header().storeId(), firstSegment,
                    superblock == null ? 0 : superblock.sequence(), new LogRecovery.Sink() {
                        @Override
                        public void catalog(int keyspace, String name) {
                            ledger.bind(keyspace, name);
                        }

                        @Override
                        public boolean commit(long sequence, byte[] body) {
                            return ledger.replay(sequence, body);
                        }

                        @Override
                        public void epoch(int epoch) {
                            epochs[0] = Integer.compareUnsigned(epoch, epochs[0]) > 0 ? epoch : epochs[0];
                        }
                    });
            blobs.begin(epochs[0] + 1);
            var catalog = ledger.assignIds();
            var pool = ioPool != null ? (PlatformIoPool) ioPool : (ownedPool = new PlatformIoPool(1));
            // Numbers only grow, past every segment and the first one the checkpoint may need
            var first = Math.max(recovered.lastSegment() + 1, Math.max(firstSegment, 1));
            var empty = recovered.empty();
            if (empty != null) {
                // Kept by the last close, or prepared by a crashed open and never written: the writer writes into it
                first = empty.number();
            }
            if (recovered.lastWritten() != null || crashed) {
                // What a process that stopped left in the cache, durable before a commit follows it: the commits it
                // replayed, their units, and the name of a segment it prepared, which this open may write into
                if (recovered.lastWritten() != null) {
                    try (var channel = FileChannel.open(recovered.lastWritten(), StandardOpenOption.WRITE)) {
                        channel.force(false);
                    }
                }
                blobs.force();
                pages.force();
                if (Durability.forceDirectory(logDirectory) && empty != null) {
                    empty = empty.named();
                }
            } else if (empty != null && !empty.nameDurable() && Durability.forceDirectory(logDirectory)) {
                // A segment no close marked, so a process may have stopped before it forced the name; the mark saves a
                // clean open this force. Where a directory cannot be forced, the first group into it is forced instead
                empty = empty.named();
            }
            var through = writeThroughWay != null ? writeThroughWay
                    : WriteThrough.of(writeThrough != null ? writeThrough : WriteThrough.Mode.fromProperty(), logDirectory);
            var log = new LogWriter(files, opened.keys(), opened.header().storeId(), first, recovered.lastSequence(),
                    segmentSize, pool.executor(), ownedPool == null ? pool::queued : null, commitWindow, empty, through,
                    overlap != null ? overlap : LogWriter.Overlap.fromProperty());
            var deltaTable = superblock == null ? null : superblock.deltaTable();
            var deltas = deltaTable == null ? DeltaTable.EMPTY
                    : DeltaTable.decode(pages.read(deltaTable, PageFormat.DELTA_TABLE, false));
            var checkpointer = new Checkpointer(ledger, pages, space, blobs, log, files, checkpointEvery,
                    superblock == null ? 0 : superblock.number(), deltas, deltaTable, backgroundFailures,
                    superblock == null ? 0 : superblock.epoch(), epochs[0], checkpointHeap);
            if (ownedPool != null) {
                ledger.attach(log, catalog, checkpointer, pages, lock, ownedPool);
            } else {
                ledger.attach(log, catalog, checkpointer, pages, lock);
            }
            // Once nothing can fail: a store that failed to open would count toward the process's budget
            checkpointer.open(ledger.replayedBytes(), ledger.replayedHeap());
            return ledger;
        } catch (IOException exception) {
            release(pages, lock, ownedPool);
            throw new LedgerIoException("The store in " + directory + " could not be opened", exception);
        } catch (RuntimeException | Error throwable) {
            release(pages, lock, ownedPool);
            throw throwable;
        }
    }

    private static boolean hasFiles(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return false;
        }
        try (var files = Files.list(directory)) {
            return files.findAny().isPresent();
        }
    }

    private static void release(PageFile pages, DirectoryLock lock, PlatformIoPool pool) {
        if (pages != null) {
            pages.close();
        }
        if (pool != null) {
            pool.close();
        }
        if (lock != null) {
            try {
                lock.close();
            } catch (IOException _) {
                // Opening failed already; the lock goes with the process at worst
            }
        }
    }
}

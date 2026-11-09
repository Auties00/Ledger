package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/// The data file of a store: its units, read through the page cache and verified against the reference that named
/// them, and the superblocks.
///
/// Reads run on the calling thread, each on a channel of its own from a small pool: Windows serializes positional
/// reads on one channel, and an interrupt during a read closes the channel it ran on, which then is replaced. The reads
/// a checkpoint batches ([#readBatch(List, byte\[\])]) take channels of a pool of their own, so they never keep the
/// store's other reads waiting for one. Writes run on the one thread that writes a checkpoint, through a [Writer].
final class PageFile implements AutoCloseable {
    static final String FILE = "ledger.data";
    /// The channels reads take turns on, and the most threads [#readAll(List, byte, int)] reads on.
    static final int READ_CHANNELS = 8;
    /// The most reads of [#readBatch(List, byte\[\])] that run at once, however many threads of a checkpoint batch
    /// reads. On Windows a read that meets a force of the log ends only with the force, about 12 ms later, however many
    /// are in flight, so the reads a checkpoint gets done in that time are the reads it has in flight.
    private static final int BATCH_READS = 32;
    /// The most pages [#readAll(List, byte, int)] and [#readBatch(List, byte\[\])] read with one call.
    static final int RUN_PAGES = 64;
    private static final int STAGED_LIMIT = 8 << 20;
    // The staged bytes a flush seals before it writes them, which bounds the sealed arrays it holds at once
    private static final long SEAL_WINDOW = 1 << 20;
    // The bytes a checkpoint writes from which it forces them beside its work, as the device takes them, instead of
    // all at once before its superblock: forced at the end, the writes of a checkpoint that rewrites hundreds of
    // megabytes kept the device idle while it built them, then made it wait seconds for them
    private static final long FORCE_EVERY = 32L << 20;
    // Not on Windows, where the forces beside the work made the checkpoints of the random overwrite benchmark slower
    // (14% fewer overwrites a second on the same runner) where they made them faster on Linux (19% more)
    private static final boolean FORCE_BESIDE = !System.getProperty("os.name", "").startsWith("Windows");
    /// Marks a read that accepts a leaf or a branch.
    static final byte NODE = -1;

    private final Path path;
    private final PageCipher cipher;
    private final PageCache cache;
    private final HeapPool pool;
    private final ReadChannels readers;
    // The channels of the reads a checkpoint batches, opened as they are first needed
    private final ReadChannels batchReaders;
    // Closes the batch channels no read took for a tick of the idle ticker, which holds it weakly
    private final IdleTicker.Watched trimmer;
    // The leaves the batches read, for tests that a checkpoint reads the bases of delta records together
    private final AtomicLong batchedLeaves = new AtomicLong();
    private final FileChannel writer;
    // The file of large values, which leaves point into
    private volatile BlobFile blobs;
    private volatile boolean closed;

    private PageFile(Path path, PageCipher cipher, PageCache cache, HeapPool pool) throws IOException {
        this.path = path;
        this.cipher = cipher;
        this.cache = cache;
        this.pool = pool;
        this.writer = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.readers = new ReadChannels(READ_CHANNELS);
        this.batchReaders = new ReadChannels(BATCH_READS);
        this.trimmer = batchReaders::trimIfIdle;
        try {
            readers.open(READ_CHANNELS);
        } catch (IOException exception) {
            close();
            throw exception;
        }
        IdleTicker.watch(trimmer);
    }

    /// Opens the data file of the store in `directory`, creating it with two empty superblocks if there is none.
    ///
    /// @param cacheBytes the most bytes the page cache holds
    static PageFile open(Path directory, PageCipher cipher, long cacheBytes, HeapPool pool) throws IOException {
        var path = directory.resolve(FILE);
        if (!Files.exists(path)) {
            var temporary = directory.resolve(FILE + ".tmp");
            try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                var empty = ByteBuffer.allocate(PageFormat.FIRST_UNIT_PAGE * PageFormat.SIZE);
                while (empty.hasRemaining()) {
                    channel.write(empty, empty.position());
                }
                channel.force(true);
            }
            Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            Durability.forceDirectory(directory);
        }
        var cache = PageCache.shared(cacheBytes);
        try {
            return new PageFile(path, cipher, cache, pool);
        } catch (IOException | RuntimeException failure) {
            cache.close();
            throw failure;
        }
    }

    /// {@return the newest superblock that authenticates, or `null` if no checkpoint was ever made durable}
    ///
    /// A superblock that fails authentication was torn by a crash while it was written, so the other one holds the
    /// last durable checkpoint; if the other one was never written, no checkpoint was.
    ///
    /// @throws LedgerCorruptedException if both superblocks were written and neither authenticates
    Superblock superblock() {
        Superblock newest = null;
        var damaged = 0;
        var written = 0;
        for (var slot = 0; slot < PageFormat.FIRST_UNIT_PAGE; slot++) {
            var sealed = new byte[PageFormat.SIZE];
            if (!readAvailable(sealed, PageFormat.offset(slot)) || isZero(sealed)) {
                continue;
            }
            written++;
            var plain = new byte[PageFormat.SIZE];
            if (!cipher.open(sealed, 0, 1, plain) || plain[PageFormat.KIND] != PageFormat.SUPERBLOCK
                    || Bytes.i32(plain, PageFormat.PAGE) != slot || Bytes.i32(plain, PageFormat.PAGES) != 1) {
                damaged++;
                continue;
            }
            var superblock = Superblock.read(plain);
            if (superblock.slot() != slot) {
                damaged++;
                continue;
            }
            if (newest == null || superblock.number() > newest.number()) {
                newest = superblock;
            }
        }
        if (newest == null && damaged == 2 && written == 2) {
            throw new LedgerCorruptedException(path + " has no superblock that authenticates", null);
        }
        return newest;
    }

    /// Writes `superblock` to its page and forces it.
    void writeSuperblock(Superblock superblock, long nonceHigh, int nonceLow) throws IOException {
        var plain = new byte[PageFormat.SIZE];
        header(plain, PageFormat.SUPERBLOCK, 0, superblock.slot(), 1, Superblock.CONTENT, nonceHigh, nonceLow);
        superblock.write(plain);
        var sealed = new byte[PageFormat.SIZE];
        cipher.seal(plain, 1, sealed, 0);
        var buffer = ByteBuffer.wrap(sealed);
        while (buffer.hasRemaining()) {
            writer.write(buffer, PageFormat.offset(superblock.slot()) + buffer.position());
        }
        force(false);
    }

    /// {@return the plaintext of the unit `ref` names, from the cache or the file}
    ///
    /// A delta record reads as the leaf it makes of its base ([DeltaLeaves]). A unit read for the cache may end with its
    /// content ([PageFormat#cachedLength(int, int)]); one read past it holds its whole pages.
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    /// @param cached whether to keep the unit in the cache, and the units a delta record's leaf is made of
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    /// @throws LedgerIoException if the file cannot be read
    byte[] read(PageRef ref, byte kind, boolean cached) {
        return read(ref, kind, cached, true);
    }

    /// {@return the plaintext of the unit `ref` names, from the cache or the file}
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    /// @param cached whether to keep the unit in the cache
    /// @param referenced whether a unit the cache takes counts as read, rather than as the first it evicts
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    /// @throws LedgerIoException if the file cannot be read
    byte[] read(PageRef ref, byte kind, boolean cached, boolean referenced) {
        var hit = cache.get(ref);
        if (hit != null && kindMatches(hit[PageFormat.KIND], kind)) {
            return hit;
        }
        if (ref.isRecord()) {
            // The delta page and the base go to the cache as the entries it evicts first: the leaf they make is what a
            // read uses again, and holding all three filled the cache with each leaf twice
            var leaf = DeltaLeaves.materialize(ref, (unit, unitKind) -> read(unit, unitKind, cached, false));
            if (!kindMatches(leaf[PageFormat.KIND], kind)) {
                throw new LedgerCorruptedException(path + " holds a delta record where a branch belongs, at page " + ref.page(), null);
            }
            if (cached) {
                cache.put(ref, leaf, referenced);
            }
            return leaf;
        }
        var length = ref.pages() * PageFormat.SIZE;
        var sealed = pool.acquire(length);
        try {
            readFully(readers, readers.take(), sealed, length, PageFormat.offset(ref.page()));
            // The header is the additional data of the tag and the open copies it as it is, so what it says holds once
            // the unit authenticates: it is checked first, and tells the bytes to keep
            var content = Bytes.i32(sealed, PageFormat.LENGTH);
            var plain = ref.describes(sealed) && kindMatches(sealed[PageFormat.KIND], kind) && PageFormat.fits(ref.pages(), content)
                    ? cipher.open(sealed, 0, ref.pages(), cached ? PageFormat.cachedLength(ref.pages(), content) : length, pool)
                    : null;
            if (plain == null) {
                throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
            }
            if (cached) {
                cache.put(ref, plain, referenced);
            }
            return plain;
        } finally {
            pool.release(sealed);
        }
    }

    /// {@return the plaintext of the units `refs` name, in their order, none of them cached}
    ///
    /// Units in consecutive pages are read with one call, up to [#RUN_PAGES] pages, and the calls run on at most
    /// `threads` threads, the caller's and virtual ones, and no more than there are read channels: reading many units
    /// one after another out of the OS cache made the caller wait for each read in turn.
    ///
    /// @param kind the kind every unit must have, or [#NODE] for leaves and branches
    /// @throws LedgerCorruptedException if a unit fails authentication or is not the one its reference names
    /// @throws LedgerIoException if the file cannot be read
    byte[][] readAll(List<PageRef> refs, byte kind, int threads) {
        var kinds = new byte[refs.size()];
        Arrays.fill(kinds, kind);
        return readRuns(refs, kinds, readers, Math.min(READ_CHANNELS, threads), true);
    }

    /// {@return the plaintext of the units `refs` name, in their order, read from the file, none of them cached and each
    /// as long as the page cache would keep it}
    ///
    /// For the reads of a checkpoint, whose caller looked in the cache and in the checkpoint's writer already. Units in
    /// consecutive pages are read with one call, up to [#RUN_PAGES] pages; the calls run on the caller's thread and on
    /// virtual threads, on channels apart from those of the store's other reads, and at most [#BATCH_READS] of them at
    /// once across every thread that calls this method. A single call runs on the caller's thread alone.
    ///
    /// @param refs the units, none of them a delta record and none named twice
    /// @param kinds the kind each unit must have, by index, or [#NODE] for a leaf or a branch
    /// @throws IllegalArgumentException if a reference names a delta record
    /// @throws LedgerCorruptedException if a unit fails authentication or is not the one its reference names
    /// @throws LedgerIoException if the file cannot be read
    byte[][] readBatch(List<PageRef> refs, byte[] kinds) {
        var leaves = 0;
        for (var i = 0; i < refs.size(); i++) {
            var ref = refs.get(i);
            if (ref.isRecord()) {
                throw new IllegalArgumentException("A batch reads units, not the delta record at page " + ref.page());
            }
            if (kinds[i] == PageFormat.LEAF) {
                leaves++;
            }
        }
        batchedLeaves.addAndGet(leaves);
        return readRuns(refs, kinds, batchReaders, BATCH_READS, false);
    }

    /// {@return the plaintext of the units `refs` name, in their order, read in runs of consecutive pages on at most
    /// `threads` threads, the caller's among them, each run on a channel of `channels`}
    ///
    /// Every thread it starts ends before it returns or throws.
    ///
    /// @param throughCache whether a unit read alone, a delta record or one the cache holds among them, is read
    ///                     through [#read(PageRef, byte, boolean)]
    private byte[][] readRuns(List<PageRef> refs, byte[] kinds, ReadChannels channels, int threads, boolean throughCache) {
        var units = new byte[refs.size()][];
        var order = new Integer[refs.size()];
        for (var i = 0; i < order.length; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (left, right) -> Integer.compare(refs.get(left).page(), refs.get(right).page()));
        // Runs of indexes into refs, by page
        var runs = new ArrayList<int[]>();
        var run = new ArrayList<Integer>();
        var runPages = 0;
        var next = -1;
        for (var index : order) {
            var ref = refs.get(index);
            var alone = throughCache && (ref.isRecord() || cache.get(ref) != null);
            if (!run.isEmpty() && (alone || ref.page() != next || runPages + ref.pages() > RUN_PAGES)) {
                runs.add(run.stream().mapToInt(Integer::intValue).toArray());
                run.clear();
                runPages = 0;
            }
            if (alone) {
                runs.add(new int[]{index});
                continue;
            }
            run.add(index);
            runPages += ref.pages();
            next = ref.page() + ref.pages();
        }
        if (!run.isEmpty()) {
            runs.add(run.stream().mapToInt(Integer::intValue).toArray());
        }
        var claimed = new AtomicInteger();
        var failure = new AtomicReference<Throwable>();
        Runnable reader = () -> {
            for (var i = claimed.getAndIncrement(); i < runs.size() && failure.get() == null; i = claimed.getAndIncrement()) {
                try {
                    var indexes = runs.get(i);
                    if (throughCache && indexes.length == 1) {
                        units[indexes[0]] = read(refs.get(indexes[0]), kinds[indexes[0]], false);
                    } else {
                        readRun(channels, refs, indexes, kinds, units, !throughCache);
                    }
                } catch (Throwable throwable) {
                    // An error too, such as running out of memory: the slot it left empty would read as a unit
                    failure.compareAndSet(null, throwable);
                }
            }
        };
        var started = new Thread[Math.max(0, Math.min(threads, runs.size()) - 1)];
        var count = 0;
        try {
            for (; count < started.length; count++) {
                started[count] = Thread.ofVirtual().name("ledger-read").start(reader);
            }
        } catch (Throwable throwable) {
            // The threads started read on and are joined; the caller's reads nothing more
            failure.compareAndSet(null, throwable);
        }
        reader.run();
        var interrupted = false;
        for (var i = 0; i < count; i++) {
            while (true) {
                try {
                    started[i].join();
                    break;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return switch (failure.get()) {
            case null -> units;
            case RuntimeException exception -> throw exception;
            case Error error -> throw error;
            case Throwable other -> throw new IllegalStateException(other);
        };
    }

    /// Reads the units at `indexes` of `refs`, in consecutive pages, with one call on a channel of `channels`, into
    /// `units`.
    ///
    /// It takes the channel before the array the call reads into, so the arrays the reads hold at once are bounded by
    /// the channels in use, not by the threads that wait for one.
    ///
    /// @param forCache whether each unit may end with its content, as the page cache keeps it
    private void readRun(ReadChannels channels, List<PageRef> refs, int[] indexes, byte[] kinds, byte[][] units, boolean forCache) {
        var first = refs.get(indexes[0]).page();
        var last = refs.get(indexes[indexes.length - 1]);
        var length = (last.page() + last.pages() - first) * PageFormat.SIZE;
        var channel = channels.take();
        byte[] sealed;
        try {
            sealed = pool.acquire(length);
        } catch (Throwable throwable) {
            channels.give(channel);
            throw throwable;
        }
        try {
            readFully(channels, channel, sealed, length, PageFormat.offset(first));
            for (var index : indexes) {
                var ref = refs.get(index);
                var offset = (ref.page() - first) * PageFormat.SIZE;
                // The header is the additional data of the tag, so a length it lies about fails the open
                var content = Bytes.i32(sealed, offset + PageFormat.LENGTH);
                var plain = PageFormat.fits(ref.pages(), content)
                        ? cipher.open(sealed, offset, ref.pages(), forCache ? PageFormat.cachedLength(ref.pages(), content) : ref.pages() * PageFormat.SIZE, pool)
                        : null;
                if (plain == null || !ref.describes(plain) || !kindMatches(plain[PageFormat.KIND], kinds[index])) {
                    throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
                }
                units[index] = plain;
            }
        } finally {
            pool.release(sealed);
        }
    }

    /// {@return the plaintext of the unit `ref` names if the cache holds it, or `null`}
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    byte[] cached(PageRef ref, byte kind) {
        var hit = cache.get(ref);
        return hit != null && kindMatches(hit[PageFormat.KIND], kind) ? hit : null;
    }

    /// {@return the leaves [#readBatch(List, byte\[\])] was asked for, for tests of what a checkpoint reads together}
    long batchedLeavesForTests() {
        return batchedLeaves.get();
    }

    private static boolean kindMatches(byte actual, byte expected) {
        return expected == NODE ? actual == PageFormat.LEAF || actual == PageFormat.BRANCH : actual == expected;
    }

    /// Reads `length` bytes at `position` into `array` on `channel`, which `channels` gave, and gives it back to them,
    /// or the channel that replaced it if an interrupt closed it.
    private void readFully(ReadChannels channels, FileChannel channel, byte[] array, int length, long position) {
        var interrupted = false;
        try {
            var buffer = ByteBuffer.wrap(array, 0, length);
            while (buffer.hasRemaining()) {
                try {
                    if (channel.read(buffer, position + buffer.position()) < 0) {
                        throw new LedgerCorruptedException(path + " ends before the unit at " + position, null);
                    }
                } catch (ClosedByInterruptException _) {
                    // The interrupt closed the channel; read on a new one and give the caller its interrupt back
                    interrupted |= Thread.interrupted();
                    channel = FileChannel.open(path, StandardOpenOption.READ);
                }
            }
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            channels.give(channel);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean readAvailable(byte[] array, long position) {
        var channel = readers.take();
        try {
            var buffer = ByteBuffer.wrap(array);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer, position + buffer.position()) < 0) {
                    return false;
                }
            }
            return true;
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            readers.give(channel);
        }
    }

    private static boolean isZero(byte[] array) {
        for (var value : array) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    /// Fills the header of `unit`.
    static void header(byte[] unit, byte kind, int level, int page, int pages, int length, long nonceHigh, int nonceLow) {
        unit[PageFormat.KIND] = kind;
        unit[PageFormat.FORMAT] = PageFormat.VERSION;
        unit[PageFormat.LEVEL] = (byte) level;
        unit[3] = 0;
        Bytes.i32(unit, PageFormat.PAGES, pages);
        Bytes.i32(unit, PageFormat.PAGE, page);
        Bytes.i32(unit, PageFormat.LENGTH, length);
        Bytes.i64(unit, PageFormat.NONCE, nonceHigh);
        Bytes.i32(unit, PageFormat.NONCE + 8, nonceLow);
        Bytes.i32(unit, 28, 0);
    }

    /// Forces every unit written so far, and the length of the file.
    void force() throws IOException {
        force(true);
    }

    private void force(boolean metadata) throws IOException {
        writer.force(metadata);
    }

    /// Cuts the file to `pages` pages, every one past them being free.
    void truncate(int pages) throws IOException {
        writer.truncate(PageFormat.offset(pages));
    }

    /// {@return the page cache}
    PageCache cache() {
        return cache;
    }

    /// Makes `blobs` the file of large values the trees point into; this file closes it.
    void useBlobs(BlobFile blobs) {
        this.blobs = blobs;
    }

    /// {@return the file of large values the trees point into}
    BlobFile blobs() {
        return blobs;
    }

    /// {@return a writer of the units of one checkpoint, sealed under nonces that start with `nonceHigh`}
    Writer writer(long nonceHigh) {
        return new Writer(nonceHigh, () -> true);
    }

    /// {@return a writer of the units of one checkpoint, sealed under nonces that start with `nonceHigh`, that forces
    /// them beside its work only while `lagging` holds}
    Writer writer(long nonceHigh, BooleanSupplier lagging) {
        return new Writer(nonceHigh, lagging);
    }

    @Override
    public void close() {
        closed = true;
        IdleTicker.unwatch(trimmer);
        cache.close();
        var external = blobs;
        if (external != null) {
            external.close();
        }
        readers.closeIdle();
        batchReaders.closeIdle();
        try {
            writer.close();
        } catch (IOException _) {
            // Every write a checkpoint relies on was forced before its superblock
        }
    }

    /// Read channels of the file, each serving one read at a time, at most a number of them in use at once.
    ///
    /// A read waits for a channel while all are in use. One is opened when a read finds none idle, or finds one an
    /// interrupt closed; one given back after the file closed is closed, so none outlives the file, not even one a read
    /// held while the file closed.
    private final class ReadChannels {
        private final Semaphore permits;
        private final ArrayBlockingQueue<FileChannel> idle;
        // Whether a read took a channel since the last trim; plain, as a stale read only delays a trim or closes
        // channels just used, which the next reads open again, and written only when it changes, so the reads of
        // every thread do not take its line from each other
        private boolean used;

        /// Makes a pool of at most `capacity` channels in use at once, none open yet.
        ReadChannels(int capacity) {
            this.permits = new Semaphore(capacity);
            this.idle = new ArrayBlockingQueue<>(capacity);
        }

        /// Opens `count` channels, at most the capacity, idle until reads take them.
        void open(int count) throws IOException {
            for (var i = 0; i < count; i++) {
                idle.add(FileChannel.open(path, StandardOpenOption.READ));
            }
        }

        /// {@return a channel for one read alone, which [#give(FileChannel)] gives back; waits while every channel is in
        /// use}
        ///
        /// @throws IllegalStateException if the file is closed
        /// @throws LedgerIoException if a channel cannot be opened
        FileChannel take() {
            if (closed) {
                throw new IllegalStateException("The store is closed");
            }
            permits.acquireUninterruptibly();
            if (!used) {
                used = true;
            }
            var channel = idle.poll();
            if (channel != null && channel.isOpen()) {
                return channel;
            }
            // None open yet, or one an interrupt closed and that could not be replaced at the time
            try {
                return FileChannel.open(path, StandardOpenOption.READ);
            } catch (IOException exception) {
                permits.release();
                throw new LedgerIoException(path + " could not be opened", exception);
            }
        }

        /// Gives back `channel`, which [#take()] gave or which replaced the one it gave.
        void give(FileChannel channel) {
            if (!idle.offer(channel)) {
                close(channel);
            }
            permits.release();
            if (closed) {
                closeIdle();
            }
        }

        /// Closes the idle channels if no read took one since the last call.
        void trimIfIdle() {
            if (used) {
                used = false;
            } else {
                closeIdle();
            }
        }

        /// Closes the idle channels; the reads that find none idle later open others.
        void closeIdle() {
            for (var channel = idle.poll(); channel != null; channel = idle.poll()) {
                close(channel);
            }
        }

        private static void close(FileChannel channel) {
            try {
                channel.close();
            } catch (IOException _) {
                // Nothing was written through a read channel
            }
        }
    }

    /// Writes the units of one checkpoint: it queues them, then seals and writes them in order of their pages,
    /// coalescing consecutive ones into one call.
    ///
    /// Each unit gets the next nonce of the checkpoint: the checkpoint's random first 8 bytes and a counter. A queued
    /// unit is read back through [#staged(PageRef)], since the file does not hold it yet.
    ///
    /// It is safe for concurrent use, as the tasks of a checkpoint that rewrites its trees in parallel use it; a flush
    /// seals its units on every processor.
    final class Writer {
        /// The queued units from which a flush seals them on every processor.
        private static final int PARALLEL_SEALS = 16;

        private final Object lock;
        private final long nonceHigh;
        private int nonceLow;
        private final TreeMap<Integer, Staged> staged;
        private long stagedBytes;
        private long written;
        // The bytes written when the last force beside the checkpoint started
        private long forcedUpTo;
        // The force beside the checkpoint, running or done, or null
        private FutureTask<Void> forcing;
        // Whether the commits wait for the checkpoint, which only then forces beside its work
        private final BooleanSupplier lagging;

        private Writer(long nonceHigh, BooleanSupplier lagging) {
            this.lock = new Object();
            this.nonceHigh = nonceHigh;
            this.staged = new TreeMap<>();
            this.lagging = lagging;
        }

        /// Queues `unit`, whose content is filled in, as the unit of `pages` pages at `page`.
        ///
        /// `unit` may end with its content, or anywhere up to the end of the unit: the unit is sealed as if zeros
        /// followed it. The writer, and the page cache if `cached`, own `unit` from now on.
        ///
        /// @param length the bytes of content
        /// @return the reference of the unit
        /// @throws IllegalArgumentException if `unit` ends before its content or past the unit, or the content does not
        ///                                  fit in the unit
        PageRef write(byte kind, int level, int page, int pages, byte[] unit, int length, boolean cached) throws IOException {
            synchronized (lock) {
                return write(kind, level, page, pages, unit, length, cached, nextNonce());
            }
        }

        /// Queues `unit` as [#write(byte, int, int, int, byte\[\], int, boolean)] does, sealed under `nonce`, which
        /// [#nextNonce()] gave.
        ///
        /// @return the reference of the unit
        PageRef write(byte kind, int level, int page, int pages, byte[] unit, int length, boolean cached, int nonce) throws IOException {
            if (!PageFormat.fits(pages, length) || unit.length < PageFormat.HEADER + length || unit.length > pages * PageFormat.SIZE) {
                throw new IllegalArgumentException("A unit of " + pages + " pages cannot hold " + length + " bytes of content in "
                        + unit.length + " bytes");
            }
            synchronized (lock) {
                var ref = new PageRef(page, pages, nonceHigh, nonce);
                header(unit, kind, level, page, pages, length, ref.nonceHigh(), ref.nonceLow());
                if (staged.put(page, new Staged(ref, unit)) != null) {
                    throw new IllegalStateException("A checkpoint wrote page " + page + " twice");
                }
                stagedBytes += (long) pages * PageFormat.SIZE;
                if (cached) {
                    cache.put(ref, unit, false);
                } else {
                    cache.remove(page);
                }
                if (stagedBytes >= STAGED_LIMIT) {
                    flush();
                }
                return ref;
            }
        }

        /// {@return the plaintext of the queued unit `ref` names, or `null` if it is not queued}
        byte[] staged(PageRef ref) {
            synchronized (lock) {
                var entry = staged.get(ref.page());
                return entry != null && entry.ref().equals(ref) ? entry.unit() : null;
            }
        }

        /// {@return the first 8 bytes of every nonce of this writer}
        long nonceHigh() {
            return nonceHigh;
        }

        /// {@return the next nonce, for a unit written outside this writer or whose reference is needed before it is
        /// queued}
        int nextNonce() {
            synchronized (lock) {
                if (nonceLow == -1) {
                    throw new IllegalStateException("A checkpoint ran out of nonces");
                }
                return nonceLow++;
            }
        }

        /// Seals and writes every queued unit.
        ///
        /// The units leave the queue once they are written, so a read of one meanwhile still finds it there.
        void flush() throws IOException {
            synchronized (lock) {
                flushQueued();
            }
        }

        private void flushQueued() throws IOException {
            var entries = new ArrayList<>(staged.values());
            // A window at a time: sealing all the staged units before writing any left the pool holding a whole flush
            // of sealed arrays, 8 MiB, from one checkpoint to the next
            for (var from = 0; from < entries.size(); ) {
                var to = from;
                var bytes = 0L;
                while (to < entries.size() && (to == from || bytes + (long) entries.get(to).ref().pages() * PageFormat.SIZE <= SEAL_WINDOW)) {
                    bytes += (long) entries.get(to).ref().pages() * PageFormat.SIZE;
                    to++;
                }
                flushWindow(entries.subList(from, to));
                from = to;
            }
            written += stagedBytes;
            staged.clear();
            stagedBytes = 0;
            forceBeside();
        }

        /// Seals and writes the staged units of `entries`, in the order of their pages.
        private void flushWindow(List<Staged> entries) throws IOException {
            var run = new ArrayList<ByteBuffer>();
            var sealedUnits = new byte[entries.size()][];
            var runStart = -1L;
            var runEnd = -1L;
            try {
                seal(entries, sealedUnits);
                for (var index = 0; index < entries.size(); index++) {
                    var entry = entries.get(index);
                    var length = entry.ref().pages() * PageFormat.SIZE;
                    var sealed = sealedUnits[index];
                    var start = PageFormat.offset(entry.ref().page());
                    if (start != runEnd && !run.isEmpty()) {
                        writeRun(run, runStart);
                        run.clear();
                    }
                    if (run.isEmpty()) {
                        runStart = start;
                    }
                    run.add(ByteBuffer.wrap(sealed, 0, length));
                    runEnd = start + length;
                }
                if (!run.isEmpty()) {
                    writeRun(run, runStart);
                }
            } finally {
                for (var sealed : sealedUnits) {
                    if (sealed != null) {
                        pool.release(sealed);
                    }
                }
            }
        }

        /// Seals the staged units of `entries` into arrays of the pool, in `sealedUnits` by index, on every processor
        /// from [#PARALLEL_SEALS] units; `sealedUnits` holds those it sealed if it throws.
        ///
        /// The caller holds the lock and waits for the seals, so they run on virtual threads and never in the common
        /// pool, where the tasks of a parallel update wait for this same lock: a seal forked to the deque of a worker
        /// parked on a join, above a task no free worker is left to steal from its base, would never run, and the
        /// checkpoint with it.
        private void seal(List<Staged> entries, byte[][] sealedUnits) {
            var claimed = new AtomicInteger();
            var failure = new AtomicReference<Throwable>();
            Runnable sealer = () -> {
                for (var i = claimed.getAndIncrement(); i < entries.size() && failure.get() == null; i = claimed.getAndIncrement()) {
                    try {
                        var entry = entries.get(i);
                        var sealed = pool.acquire(entry.ref().pages() * PageFormat.SIZE);
                        sealedUnits[i] = sealed;
                        cipher.seal(entry.unit(), entry.ref().pages(), sealed, 0);
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }
            };
            var started = new Thread[entries.size() < PARALLEL_SEALS ? 0 : Runtime.getRuntime().availableProcessors() - 1];
            var count = 0;
            try {
                for (; count < started.length; count++) {
                    started[count] = Thread.ofVirtual().name("ledger-seal").start(sealer);
                }
            } catch (Throwable throwable) {
                // The threads started seal on and are joined; the caller's seals nothing more
                failure.compareAndSet(null, throwable);
            }
            sealer.run();
            var interrupted = false;
            for (var i = 0; i < count; i++) {
                while (true) {
                    try {
                        started[i].join();
                        break;
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            switch (failure.get()) {
                case null -> {
                }
                case RuntimeException exception -> throw exception;
                case Error error -> throw error;
                case Throwable other -> throw new IllegalStateException(other);
            }
        }

        /// Starts forcing what the checkpoint wrote so far, beside its work, once it wrote [#FORCE_EVERY] bytes since
        /// the last force started and that one is done, while the checkpoint lags; throws the failure of that one.
        ///
        /// A checkpoint the commits do not wait for forces at its end only: forcing beside it made the device take
        /// its writes while the commits' log writes queued behind them, which cost an ingest of a history a tenth of
        /// its rate on Linux.
        private void forceBeside() throws IOException {
            if (forcing != null) {
                if (!forcing.isDone()) {
                    return;
                }
                await(forcing);
                forcing = null;
            }
            if (!FORCE_BESIDE || written - forcedUpTo < FORCE_EVERY || !lagging.getAsBoolean()) {
                return;
            }
            forcedUpTo = written;
            // The data only: the length of the file and the device's cache wait for the force before the superblock
            var task = new FutureTask<Void>(() -> {
                PageFile.this.writer.force(false);
                return null;
            });
            Thread.ofVirtual().name("ledger-page-force").start(task);
            forcing = task;
        }

        /// Waits for the force running beside the checkpoint, if any.
        ///
        /// @throws IOException if it failed: a write it did not make durable may be lost even if a later force
        ///                     succeeds, so the checkpoint fails
        void awaitForce() throws IOException {
            FutureTask<Void> running;
            synchronized (lock) {
                running = forcing;
                forcing = null;
            }
            if (running != null) {
                await(running);
            }
        }

        private static void await(FutureTask<Void> task) throws IOException {
            try {
                task.get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while forcing the data file", exception);
            } catch (ExecutionException exception) {
                throw exception.getCause() instanceof IOException failure ? failure
                        : new IOException("The data file could not be forced", exception.getCause());
            }
        }

        private void writeRun(ArrayList<ByteBuffer> run, long position) throws IOException {
            var buffers = run.toArray(ByteBuffer[]::new);
            var remaining = 0L;
            for (var buffer : buffers) {
                remaining += buffer.remaining();
            }
            writer.position(position);
            while (remaining > 0) {
                remaining -= writer.write(buffers);
            }
        }

        /// {@return the bytes written or queued so far}
        long written() {
            synchronized (lock) {
                return written + stagedBytes;
            }
        }

        /// Drops every queued unit, for a checkpoint that failed.
        void discard() {
            try {
                awaitForce();
            } catch (IOException _) {
                // The checkpoint failed already; its pages go back
            }
            synchronized (lock) {
                for (var entry : staged.values()) {
                    cache.remove(entry.ref().page());
                }
                staged.clear();
                stagedBytes = 0;
            }
        }
    }

    /// A queued unit.
    private record Staged(PageRef ref, byte[] unit) {
    }
}

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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

/// The data file of a store: its units, read through the page cache and verified against the reference that named
/// them, and the superblocks.
///
/// Reads run on the calling thread, each on a channel of its own from a small pool: Windows serializes positional
/// reads on one channel, and an interrupt during a read closes the channel it ran on, which then is replaced. Units are
/// written by the threads of the checkpoint that writes them, through a [Writer].
final class PageFile implements AutoCloseable {
    static final String FILE = "ledger.data";
    private static final int READ_CHANNELS = 8;
    /// The most pages [#readAll(List, byte)] reads with one call, and a [Writer] writes with one call but for a unit
    /// longer than that.
    private static final int RUN_PAGES = 64;
    /// The queued bytes from which a [Writer] flushes its queue.
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
    private final ArrayBlockingQueue<FileChannel> readers;
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
        this.readers = new ArrayBlockingQueue<>(READ_CHANNELS);
        try {
            for (var i = 0; i < READ_CHANNELS; i++) {
                readers.add(FileChannel.open(path, StandardOpenOption.READ));
            }
        } catch (IOException exception) {
            close();
            throw exception;
        }
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
    /// A delta record reads as the leaf it makes of its base ([DeltaLeaves]).
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
            readFully(sealed, length, PageFormat.offset(ref.page()));
            var plain = new byte[length];
            if (!cipher.open(sealed, 0, ref.pages(), plain) || !ref.describes(plain) || !kindMatches(plain[PageFormat.KIND], kind)) {
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
    /// Units in consecutive pages are read with one call, up to [#RUN_PAGES] pages, and the calls run on as many
    /// virtual threads as there are read channels: reading many units one after another out of the OS cache made the
    /// caller wait for each read in turn.
    ///
    /// @param kind the kind every unit must have, or [#NODE] for leaves and branches
    /// @throws LedgerCorruptedException if a unit fails authentication or is not the one its reference names
    /// @throws LedgerIoException if the file cannot be read
    byte[][] readAll(List<PageRef> refs, byte kind) {
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
            var alone = ref.isRecord() || cache.get(ref) != null;
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
        var failure = new AtomicReference<RuntimeException>();
        Runnable reader = () -> {
            for (var i = claimed.getAndIncrement(); i < runs.size() && failure.get() == null; i = claimed.getAndIncrement()) {
                try {
                    readRun(refs, runs.get(i), kind, units);
                } catch (RuntimeException exception) {
                    failure.compareAndSet(null, exception);
                }
            }
        };
        var threads = Math.min(READ_CHANNELS, runs.size());
        if (threads <= 1) {
            reader.run();
        } else {
            var started = new Thread[threads];
            for (var i = 0; i < threads; i++) {
                started[i] = Thread.ofVirtual().name("ledger-read").start(reader);
            }
            var interrupted = false;
            for (var thread : started) {
                while (true) {
                    try {
                        thread.join();
                        break;
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (failure.get() != null) {
            throw failure.get();
        }
        return units;
    }

    /// Reads the units at `indexes` of `refs`, in consecutive pages, with one call, into `units`.
    private void readRun(List<PageRef> refs, int[] indexes, byte kind, byte[][] units) {
        if (indexes.length == 1) {
            units[indexes[0]] = read(refs.get(indexes[0]), kind, false);
            return;
        }
        var first = refs.get(indexes[0]).page();
        var last = refs.get(indexes[indexes.length - 1]);
        var length = (last.page() + last.pages() - first) * PageFormat.SIZE;
        var sealed = pool.acquire(length);
        try {
            readFully(sealed, length, PageFormat.offset(first));
            for (var index : indexes) {
                var ref = refs.get(index);
                var plain = new byte[ref.pages() * PageFormat.SIZE];
                if (!cipher.open(sealed, (ref.page() - first) * PageFormat.SIZE, ref.pages(), plain) || !ref.describes(plain)
                        || !kindMatches(plain[PageFormat.KIND], kind)) {
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

    private static boolean kindMatches(byte actual, byte expected) {
        return expected == NODE ? actual == PageFormat.LEAF || actual == PageFormat.BRANCH : actual == expected;
    }

    private void readFully(byte[] array, int length, long position) {
        var channel = takeChannel();
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
            readers.offer(channel);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean readAvailable(byte[] array, long position) {
        var channel = takeChannel();
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
            readers.offer(channel);
        }
    }

    private FileChannel takeChannel() {
        if (closed) {
            throw new IllegalStateException("The store is closed");
        }
        var interrupted = false;
        try {
            while (true) {
                try {
                    var channel = readers.take();
                    if (channel.isOpen()) {
                        return channel;
                    }
                    // A channel an interrupt closed and that could not be replaced at the time
                    try {
                        return FileChannel.open(path, StandardOpenOption.READ);
                    } catch (IOException exception) {
                        readers.offer(channel);
                        throw new LedgerIoException(path + " could not be opened", exception);
                    }
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
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
        return new Writer(nonceHigh, () -> true, STAGED_LIMIT, 0);
    }

    /// {@return a writer of the units of one checkpoint, sealed under nonces that start with `nonceHigh`, that forces
    /// them beside its work only while `lagging` holds}
    Writer writer(long nonceHigh, BooleanSupplier lagging) {
        return new Writer(nonceHigh, lagging, STAGED_LIMIT, 0);
    }

    /// {@return a writer as [#writer(long)] gives, that flushes its queue from `stagedLimit` bytes and whose nonces end
    /// with counters from `firstNonce`; for tests}
    Writer writerForTests(long nonceHigh, long stagedLimit, int firstNonce) {
        return new Writer(nonceHigh, () -> true, stagedLimit, firstNonce);
    }

    @Override
    public void close() {
        closed = true;
        cache.close();
        var external = blobs;
        if (external != null) {
            external.close();
        }
        for (var channel : readers) {
            try {
                channel.close();
            } catch (IOException _) {
                // Nothing was written through a read channel
            }
        }
        try {
            writer.close();
        } catch (IOException _) {
            // Every write a checkpoint relies on was forced before its superblock
        }
    }

    /// Writes the units of one checkpoint: it queues them, then seals and writes them in order of their pages, each run
    /// of consecutive pages with one positional write.
    ///
    /// Each unit gets the next nonce of the checkpoint: the checkpoint's random first 8 bytes and a counter. A queued
    /// unit is read back through [#staged(PageRef)], since the file does not hold it yet; it leaves the queue once the
    /// write of its run returned, so a read finds every unit the writer took either queued or in the file.
    ///
    /// It is safe for concurrent use, as the tasks of a checkpoint that rewrites its trees in parallel use it, and
    /// queueing a unit takes no lock. The task whose unit brings the queue to its limit flushes it, unless another one
    /// is flushing, while the others go on queueing; one that brings it to twice the limit waits for that flush and
    /// flushes in turn, so the queue holds at most about twice the limit. A flush seals its units on every processor: a
    /// lock that queueing shared with the flushes kept every other task of the checkpoint waiting through each flush,
    /// and gave its seals no help.
    ///
    /// Once a flush fails, every later write and flush throws, and once the checkpoint [discards][#discard()] the
    /// writer nothing it queued reaches the file: a write after the checkpoint gave its pages back could overwrite a
    /// unit a later checkpoint wrote there.
    final class Writer {
        /// The queued units from which a flush seals them on every processor.
        private static final int PARALLEL_SEALS = 16;
        private static final Comparator<Staged> BY_PAGE = Comparator.comparingInt(entry -> entry.ref().page());

        private final long nonceHigh;
        private final AtomicInteger nonceLow;
        // The queued units by page; one leaves once the write of its run returned
        private final ConcurrentHashMap<Integer, Staged> staged;
        // The bytes of the queued units, which lags the queue by the units being queued or written
        private final AtomicLong stagedBytes;
        // The queued bytes from which a write flushes the queue
        private final long stagedLimit;
        // Held by the one thread that flushes, and by a discard; guards written, forcedUpTo and forcing
        private final ReentrantLock flushing;
        private long written;
        // The bytes written when the last force beside the checkpoint started
        private long forcedUpTo;
        // The force beside the checkpoint, running or done, or null
        private FutureTask<Void> forcing;
        // Whether the commits wait for the checkpoint, which only then forces beside its work
        private final BooleanSupplier lagging;
        // Set holding flushing once the checkpoint failed: nothing is queued or written after it
        private volatile boolean discarded;
        // What made a flush fail, which every later write and flush throws
        private volatile Throwable failure;

        private Writer(long nonceHigh, BooleanSupplier lagging, long stagedLimit, int firstNonce) {
            this.nonceHigh = nonceHigh;
            this.nonceLow = new AtomicInteger(firstNonce);
            this.staged = new ConcurrentHashMap<>();
            this.stagedBytes = new AtomicLong();
            this.stagedLimit = stagedLimit;
            this.flushing = new ReentrantLock();
            this.lagging = lagging;
        }

        /// Queues `unit`, whose content is filled in, as the unit of `pages` pages at `page`.
        ///
        /// The writer, and the page cache if `cached`, own `unit` from now on.
        ///
        /// @return the reference of the unit
        /// @throws IOException if this write flushed the queue and that failed, or an earlier flush failed
        /// @throws IllegalStateException if the writer was discarded
        PageRef write(byte kind, int level, int page, int pages, byte[] unit, int length, boolean cached) throws IOException {
            return write(kind, level, page, pages, unit, length, cached, nextNonce());
        }

        /// Queues `unit` as [#write(byte, int, int, int, byte\[\], int, boolean)] does, sealed under `nonce`, which
        /// [#nextNonce()] gave.
        ///
        /// @return the reference of the unit
        PageRef write(byte kind, int level, int page, int pages, byte[] unit, int length, boolean cached, int nonce) throws IOException {
            ensureUsable();
            var ref = new PageRef(page, pages, nonceHigh, nonce);
            header(unit, kind, level, page, pages, length, nonceHigh, nonce);
            if (staged.putIfAbsent(page, new Staged(ref, unit)) != null) {
                throw new IllegalStateException("A checkpoint wrote page " + page + " twice");
            }
            if (cached) {
                cache.put(ref, unit, false);
            } else {
                cache.remove(page);
            }
            var queued = stagedBytes.addAndGet((long) pages * PageFormat.SIZE);
            if (queued >= stagedLimit) {
                flushFull(queued >= 2 * stagedLimit);
            }
            return ref;
        }

        /// Flushes the queue, which reached its limit, unless another thread is flushing it and `wait` is false.
        ///
        /// It looks at the queue again once it let go of the lock: a thread whose unit brought the queue to its limit
        /// while the lock was held left that queue to the holder, which flushes it unless another thread already does.
        ///
        /// @param wait whether the queue holds twice its limit, so the caller waits for a running flush rather than
        ///             queue more
        private void flushFull(boolean wait) throws IOException {
            do {
                if (wait) {
                    flushing.lock();
                    wait = false;
                } else if (!flushing.tryLock()) {
                    return;
                }
                try {
                    while (stagedBytes.get() >= stagedLimit) {
                        flushQueued();
                    }
                } finally {
                    flushing.unlock();
                }
            } while (stagedBytes.get() >= stagedLimit);
        }

        /// {@return the plaintext of the queued unit `ref` names, or `null` if it is not queued}
        byte[] staged(PageRef ref) {
            // A unit of an earlier checkpoint, as most a checkpoint reads are, was never queued here
            if (ref.nonceHigh() != nonceHigh) {
                return null;
            }
            var entry = staged.get(ref.page());
            return entry != null && entry.ref().equals(ref) ? entry.unit() : null;
        }

        /// {@return the first 8 bytes of every nonce of this writer}
        long nonceHigh() {
            return nonceHigh;
        }

        /// {@return the next nonce, for a unit written outside this writer or whose reference is needed before it is
        /// queued}
        ///
        /// @throws IllegalStateException if every nonce was taken: the counter never wraps, which would seal two units
        ///                               under one nonce
        int nextNonce() {
            while (true) {
                var nonce = nonceLow.get();
                if (nonce == -1) {
                    throw new IllegalStateException("A checkpoint ran out of nonces");
                }
                if (nonceLow.compareAndSet(nonce, nonce + 1)) {
                    return nonce;
                }
            }
        }

        /// Seals and writes every queued unit, waiting for a flush another thread runs.
        ///
        /// The units leave the queue once they are written, so a read of one meanwhile still finds it there.
        ///
        /// @throws IOException if the flush fails, or an earlier one did
        /// @throws IllegalStateException if the writer was discarded
        void flush() throws IOException {
            flushing.lock();
            try {
                ensureUsable();
                while (!staged.isEmpty()) {
                    flushQueued();
                }
            } finally {
                flushing.unlock();
            }
        }

        /// Throws what stops this writer from queueing or writing, if anything does.
        private void ensureUsable() throws IOException {
            if (discarded) {
                throw new IllegalStateException("The checkpoint of this writer failed");
            }
            var failed = failure;
            if (failed != null) {
                throw new IOException("A flush of the checkpoint failed", failed);
            }
        }

        /// Seals and writes the units queued when it starts, in the order of their pages; holding [#flushing].
        ///
        /// A failure stays: the units it left unwritten stay queued, for [#discard()] to drop, and every later write
        /// and flush throws it, so no other thread writes on into pages the failed checkpoint gives back.
        private void flushQueued() throws IOException {
            ensureUsable();
            var entries = staged.values().toArray(Staged[]::new);
            Arrays.sort(entries, BY_PAGE);
            try {
                // A window at a time: sealing all the staged units before writing any left the pool holding a whole
                // flush of sealed arrays, 8 MiB, from one checkpoint to the next
                for (var from = 0; from < entries.length; ) {
                    from = flushWindow(entries, from);
                }
                forceBeside();
            } catch (IOException | RuntimeException | Error exception) {
                failure = exception;
                throw exception;
            }
        }

        /// Seals and writes the units of `entries`, sorted by page, from `from` up to [#SEAL_WINDOW] bytes of them;
        /// holding [#flushing].
        ///
        /// The units are sealed side by side into arrays of up to [#RUN_PAGES] pages, or of their own pages for a
        /// longer unit, and each run of consecutive pages in one array goes to the file with one write: a write of a
        /// buffer for each unit, gathered or not, is a system call for each unit on Windows. A unit leaves the queue
        /// once its run is written.
        ///
        /// @return the index past the units written
        private int flushWindow(Staged[] entries, int from) throws IOException {
            var to = from;
            var left = 0L;
            while (to < entries.length && (to == from || left + entries[to].length() <= SEAL_WINDOW)) {
                left += entries[to].length();
                to++;
            }
            var count = to - from;
            // The array each unit is sealed into and where in it
            var arrays = new byte[count][];
            var offsets = new int[count];
            // The first unit of each run, then the end of the window
            var runs = new int[count + 1];
            var runCount = 0;
            var slabs = new ArrayList<byte[]>();
            try {
                byte[] slab = null;
                var capacity = 0L;
                var used = 0;
                var next = -1;
                for (var index = 0; index < count; index++) {
                    var ref = entries[from + index].ref();
                    var length = ref.pages() * PageFormat.SIZE;
                    if (slab == null || used + length > capacity) {
                        // No larger than the units left to seal, so a flush of a few units takes a small array
                        capacity = Math.max(length, Math.min((long) RUN_PAGES * PageFormat.SIZE, left));
                        slab = pool.acquire((int) capacity);
                        slabs.add(slab);
                        used = 0;
                        next = -1;
                    }
                    if (ref.page() != next) {
                        runs[runCount++] = index;
                    }
                    arrays[index] = slab;
                    offsets[index] = used;
                    used += length;
                    left -= length;
                    next = ref.page() + ref.pages();
                }
                runs[runCount] = count;
                var indexes = IntStream.range(0, count);
                (count < PARALLEL_SEALS ? indexes : indexes.parallel()).forEach(index -> {
                    var entry = entries[from + index];
                    cipher.seal(entry.unit(), entry.ref().pages(), arrays[index], offsets[index]);
                });
                for (var run = 0; run < runCount; run++) {
                    var first = runs[run];
                    var end = runs[run + 1];
                    var length = offsets[end - 1] + entries[from + end - 1].length() - offsets[first];
                    writeRun(arrays[first], offsets[first], length, PageFormat.offset(entries[from + first].ref().page()));
                    for (var index = first; index < end; index++) {
                        var entry = entries[from + index];
                        if (staged.remove(entry.ref().page(), entry)) {
                            written += entry.length();
                            stagedBytes.addAndGet(-entry.length());
                        }
                    }
                }
            } finally {
                for (var slab : slabs) {
                    pool.release(slab);
                }
            }
            return to;
        }

        /// Starts forcing what the checkpoint wrote so far, beside its work, once it wrote [#FORCE_EVERY] bytes since
        /// the last force started and that one is done, while the checkpoint lags; throws the failure of that one;
        /// holding [#flushing].
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
        ///                     succeeds, so the checkpoint fails; or if a flush failed
        void awaitForce() throws IOException {
            FutureTask<Void> running;
            flushing.lock();
            try {
                ensureUsable();
                running = forcing;
                forcing = null;
            } finally {
                flushing.unlock();
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

        /// Writes the `length` bytes of `array` from `offset` to the file at `position`.
        private void writeRun(byte[] array, int offset, int length, long position) throws IOException {
            var buffer = ByteBuffer.wrap(array, offset, length);
            while (buffer.hasRemaining()) {
                PageFile.this.writer.write(buffer, position + buffer.position() - offset);
            }
        }

        /// Drops every queued unit, for a checkpoint that failed, once a flush that runs ends; nothing is queued or
        /// written after.
        void discard() {
            FutureTask<Void> running;
            flushing.lock();
            try {
                discarded = true;
                for (var entry : staged.values()) {
                    cache.remove(entry.ref().page());
                }
                staged.clear();
                stagedBytes.set(0);
                running = forcing;
                forcing = null;
            } finally {
                flushing.unlock();
            }
            if (running != null) {
                try {
                    await(running);
                } catch (IOException _) {
                    // The checkpoint failed already; its pages go back
                }
            }
        }
    }

    /// A queued unit.
    private record Staged(PageRef ref, byte[] unit) {
        /// {@return the bytes of the unit}
        int length() {
            return ref.pages() * PageFormat.SIZE;
        }
    }
}

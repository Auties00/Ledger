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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/// The data file of a store: its units, read through the page cache and verified against the reference that named
/// them, and the superblocks.
///
/// Reads run on the calling thread, each on a channel of its own from a small pool: Windows serializes positional
/// reads on one channel, and an interrupt during a read closes the channel it ran on, which then is replaced. Writes are
/// those of a checkpoint, through a [Writer], which writes one batch of units at a time.
final class PageFile implements AutoCloseable {
    static final String FILE = "ledger.data";
    /// The channels reads take turns on, and the most threads [#readAll(List, byte, int)] reads on.
    static final int READ_CHANNELS = 8;
    /// The most pages [#readAll(List, byte, int)] reads with one call.
    static final int RUN_PAGES = 64;
    // The queued bytes that make a batch, which a write seals and writes outside the writer's lock while the next ones
    // queue. A batch in flight holds this and the unit that passes it at most, and the writes that queue behind it wait
    // once the queue holds twice this, each with its unit queued: about three times this of plaintext the writer keeps
    // from the cache's evictions, plus one unit per writing task, and the sealed arrays of one window
    private static final int STAGED_LIMIT = 4 << 20;
    // The staged bytes a flush seals before it writes them, which bounds the sealed arrays it holds at once
    private static final long SEAL_WINDOW = 1 << 20;
    // The most bytes of one write of consecutive units, which the JDK copies into a direct buffer it keeps for the
    // thread, any task of a checkpoint that fills a batch
    private static final int RUN_BYTES = 256 << 10;
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
        return load(ref, kind, cached, referenced);
    }

    /// {@return the plaintext of the unit `ref` names, from the file, for a read that found the cache without it}
    ///
    /// A delta record still reads its delta page and its base through the cache.
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    /// @param cached whether to keep the unit in the cache
    /// @param referenced whether a unit the cache takes counts as read, rather than as the first it evicts
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    /// @throws LedgerIoException if the file cannot be read
    byte[] load(PageRef ref, byte kind, boolean cached, boolean referenced) {
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
    /// `threads` virtual threads, and no more than there are read channels: reading many units one after another out of
    /// the OS cache made the caller wait for each read in turn.
    ///
    /// @param kind the kind every unit must have, or [#NODE] for leaves and branches
    /// @throws LedgerCorruptedException if a unit fails authentication or is not the one its reference names
    /// @throws LedgerIoException if the file cannot be read
    byte[][] readAll(List<PageRef> refs, byte kind, int threads) {
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
            var alone = ref.isRecord() || cache.peek(ref) != null;
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
        var running = Math.min(Math.min(READ_CHANNELS, threads), runs.size());
        ClaimedIndexes.run("ledger-read", runs.size(), running - 1, i -> readRun(refs, runs.get(i), kind, units));
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

    /// {@return the node child `index` of the branch `parent` names, from the cache or the file}
    ///
    /// A child the cache holds costs no [PageRef]: the page, nonce and slot come straight from the branch.
    ///
    /// @throws LedgerCorruptedException if the node fails authentication or is not the one the branch names
    /// @throws LedgerIoException if the file cannot be read
    byte[] node(byte[] parent, int index) {
        var hit = cachedNode(parent, index);
        return hit != null ? hit : load(TreeNodes.childRef(parent, index), NODE, true, true);
    }

    /// {@return the node child `index` of the branch `parent` names if the cache holds it, or `null`}
    byte[] cachedNode(byte[] parent, int index) {
        var entry = TreeNodes.entry(parent, index);
        var key = PageCache.dataKey(TreeNodes.childPage(parent, entry), TreeNodes.childSlot(parent, entry));
        var hit = cache.get(key, TreeNodes.childNonceHigh(parent, entry), TreeNodes.childNonceLow(parent, entry), 1);
        return hit != null && kindMatches(hit[PageFormat.KIND], NODE) ? hit : null;
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

    /// Writes the units of one checkpoint: it queues them, then seals and writes them in batches, in order of their
    /// pages, each run of consecutive pages with one call.
    ///
    /// Each unit gets the next nonce of the checkpoint: the checkpoint's random first 8 bytes and a counter. A queued
    /// unit is read back through [#staged(PageRef)] until the file holds it.
    ///
    /// It is safe for concurrent use, as the tasks of a checkpoint that rewrites its trees in parallel use it. The
    /// write that fills a batch seals it, on every processor, and writes it outside the writer's lock, while the other
    /// tasks queue the next one: one batch is written at a time, and a write that queues a second batch's worth behind
    /// the one being written waits for it. A batch that fails to be written stays readable through
    /// [#staged(PageRef)] until [#discard()], and every later write and flush fails with its failure.
    final class Writer {
        /// The queued units from which a flush seals them on every processor.
        private static final int PARALLEL_SEALS = 16;

        private final Object lock;
        private final long nonceHigh;
        // The last 4 bytes of the next nonce; -1 once every one was given
        private final AtomicInteger nonceLow;
        // The units queued for the next batch, by page; guarded by lock
        private TreeMap<Integer, Staged> staged;
        private long stagedBytes;
        // The batch being written, or the one whose write failed, by page, or none; replaced and never changed while it
        // is written, as the thread writing it reads it without the lock; guarded by lock
        private Map<Integer, Staged> inFlight;
        private long inFlightBytes;
        // Whether a thread writes the batch in inFlight; guarded by lock
        private boolean flushing;
        // The failure of a batch or of a force beside the checkpoint, which every later write and flush throws; guarded
        // by lock
        private Throwable failure;
        // Whether the checkpoint failed and gave its units back; guarded by lock
        private boolean discarded;
        // The bytes the file holds; guarded by lock
        private long written;
        // The bytes written when the last force beside the checkpoint started; guarded by lock
        private long forcedUpTo;
        // The force beside the checkpoint, running or done, or null; guarded by lock
        private FutureTask<Void> forcing;
        // Whether the commits wait for the checkpoint, which only then forces beside its work
        private final BooleanSupplier lagging;

        private Writer(long nonceHigh, BooleanSupplier lagging) {
            this.lock = new Object();
            this.nonceHigh = nonceHigh;
            this.nonceLow = new AtomicInteger();
            this.staged = new TreeMap<>();
            this.inFlight = Map.of();
            this.lagging = lagging;
        }

        /// Queues `unit`, whose content is filled in, as the unit of `pages` pages at `page`.
        ///
        /// `unit` may end with its content, or anywhere up to the end of the unit: the unit is sealed as if zeros
        /// followed it. The writer, and the page cache if `cached`, own `unit` from now on.
        ///
        /// It writes the batch the unit fills, and waits for the batch being written if a second one queued behind it.
        ///
        /// @param length the bytes of content
        /// @return the reference of the unit
        /// @throws IllegalArgumentException if `unit` ends before its content or past the unit, or the content does not
        ///                                  fit in the unit
        /// @throws IOException if a batch of this checkpoint could not be written, by this call or an earlier one
        PageRef write(byte kind, int level, int page, int pages, byte[] unit, int length, boolean cached) throws IOException {
            return write(kind, level, page, pages, unit, length, cached, nextNonce());
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
            var ref = new PageRef(page, pages, nonceHigh, nonce);
            header(unit, kind, level, page, pages, length, ref.nonceHigh(), ref.nonceLow());
            boolean fills;
            boolean full;
            synchronized (lock) {
                checkWritable();
                // In either map, as one map held both before a flush wrote them
                if (inFlight.containsKey(page) || staged.putIfAbsent(page, new Staged(ref, unit)) != null) {
                    throw new IllegalStateException("A checkpoint wrote page " + page + " twice");
                }
                stagedBytes += (long) pages * PageFormat.SIZE;
                fills = !flushing && stagedBytes >= STAGED_LIMIT;
                full = flushing && stagedBytes >= 2L * STAGED_LIMIT;
            }
            // Outside the writer's lock: the tasks of a checkpoint all pass through it, for every unit they read too,
            // so one waiting for the cache's lock, or rebuilding its table or evicting, would hold up the others.
            // Before the batch is taken: a batch in flight is written only by the thread that took it, so a failure
            // of the cache, such as an OutOfMemoryError of a rebuild of its table, between the two would leave it unwritten
            // and every thread waiting for its end waiting for good; the unit is queued, and the next write or flush
            // takes it
            if (cached) {
                cache.put(ref, unit, false);
            } else {
                cache.remove(page);
            }
            if (!fills && !full) {
                return ref;
            }
            if (full) {
                await(() -> flushing && stagedBytes >= 2L * STAGED_LIMIT && failure == null && !discarded);
            }
            Map<Integer, Staged> batch;
            synchronized (lock) {
                checkWritable();
                batch = takeBatch();
            }
            writeBatches(batch);
            return ref;
        }

        /// {@return the plaintext of the queued unit `ref` names, or `null` if it is not queued}
        ///
        /// A unit of another checkpoint is never queued here, which it tells without the lock.
        byte[] staged(PageRef ref) {
            if (ref.nonceHigh() != nonceHigh) {
                return null;
            }
            synchronized (lock) {
                var entry = staged.get(ref.page());
                if (entry == null) {
                    entry = inFlight.get(ref.page());
                }
                return entry != null && entry.ref().equals(ref) ? entry.unit() : null;
            }
        }

        /// {@return the first 8 bytes of every nonce of this writer}
        long nonceHigh() {
            return nonceHigh;
        }

        /// {@return the next nonce, for a unit written outside this writer or whose reference is needed before it is
        /// queued}
        ///
        /// @throws IllegalStateException if every nonce was given
        int nextNonce() {
            while (true) {
                var value = nonceLow.get();
                // Never past -1, which would wrap to the first nonce
                if (value == -1) {
                    throw new IllegalStateException("A checkpoint ran out of nonces");
                }
                if (nonceLow.compareAndSet(value, value + 1)) {
                    return value;
                }
            }
        }

        /// Seals and writes every queued unit, after the batch being written, if any.
        ///
        /// The units leave the queue once they are written, so a read of one meanwhile still finds it there.
        ///
        /// @throws IOException if a batch of this checkpoint could not be written, by this call or an earlier one
        void flush() throws IOException {
            while (true) {
                await(() -> flushing);
                Map<Integer, Staged> batch;
                synchronized (lock) {
                    if (flushing) {
                        continue;
                    }
                    checkWritable();
                    if (staged.isEmpty()) {
                        return;
                    }
                    batch = swap();
                }
                writeBatches(batch);
            }
        }

        /// {@return a batch of the queued units to write, now in flight, if they fill one and none is in flight}
        ///
        /// Holding the lock.
        private Map<Integer, Staged> takeBatch() {
            return flushing || failure != null || discarded || stagedBytes < STAGED_LIMIT ? null : swap();
        }

        /// {@return the queued units of the lowest pages, [#STAGED_LIMIT] bytes of them and the unit that passes it, or
        /// all of them if fewer, as the batch to write, now in flight}
        ///
        /// Holding the lock, with no batch in flight. The others stay queued: the writes that queue a second batch
        /// behind the one in flight go on until the queue holds two, so taking all of them would make a batch of twice
        /// the limit, with two more queued behind it.
        private Map<Integer, Staged> swap() {
            // Nothing changes before the allocations, which may fail
            SortedMap<Integer, Staged> batch = staged;
            var rest = new TreeMap<Integer, Staged>();
            var bytes = stagedBytes;
            if (bytes > STAGED_LIMIT) {
                var taken = 0L;
                for (var entry : staged.entrySet()) {
                    if (taken >= STAGED_LIMIT) {
                        // The view is never changed, as the queue that backs it is replaced
                        batch = staged.headMap(entry.getKey());
                        rest = new TreeMap<>(staged.tailMap(entry.getKey()));
                        bytes = taken;
                        break;
                    }
                    taken += (long) entry.getValue().ref().pages() * PageFormat.SIZE;
                }
            }
            inFlight = batch;
            inFlightBytes = bytes;
            staged = rest;
            stagedBytes -= bytes;
            flushing = true;
            return batch;
        }

        /// Throws the failure of this checkpoint's writer, if any; holding the lock.
        ///
        /// @throws IOException if a batch or a force beside the checkpoint failed with one
        /// @throws IllegalStateException if one failed otherwise, or the checkpoint was abandoned: no task of a
        ///                               checkpoint writes after it gave back its pages
        private void checkWritable() throws IOException {
            switch (failure) {
                case null -> {
                }
                case IOException exception -> throw new IOException("A write of this checkpoint failed", exception);
                default -> throw new IllegalStateException("A write of this checkpoint failed", failure);
            }
            if (discarded) {
                throw new IllegalStateException("A checkpoint wrote a unit after it was abandoned");
            }
        }

        /// Writes `batch`, then each batch that filled while it was written, while this thread is the one to.
        private void writeBatches(Map<Integer, Staged> batch) throws IOException {
            while (batch != null) {
                batch = writeBatch(batch);
            }
        }

        /// Seals and writes `batch`, the one in flight, in order of its pages, then records how that ended: written, or
        /// a failure that leaves it in flight, where reads still find its units, which then are in no other place they
        /// authenticate.
        ///
        /// @return the next batch, now in flight, that this thread is to write, or `null`
        private Map<Integer, Staged> writeBatch(Map<Integer, Staged> batch) throws IOException {
            Throwable thrown = null;
            try {
                writeUnits(new ArrayList<>(batch.values()));
            } catch (Throwable throwable) {
                thrown = throwable;
            }
            Map<Integer, Staged> next = null;
            synchronized (lock) {
                try {
                    flushing = false;
                    if (thrown == null) {
                        written += inFlightBytes;
                        inFlight = Map.of();
                        inFlightBytes = 0;
                        try {
                            forceBeside();
                        } catch (Throwable force) {
                            thrown = force;
                        }
                    }
                    if (thrown == null) {
                        next = takeBatch();
                    } else if (failure == null) {
                        failure = thrown;
                    }
                } finally {
                    // The writes waiting for this batch, and a flush or a discard
                    lock.notifyAll();
                }
            }
            switch (thrown) {
                case null -> {
                    return next;
                }
                case IOException exception -> throw exception;
                case RuntimeException exception -> throw exception;
                case Error error -> throw error;
                default -> throw new IllegalStateException(thrown);
            }
        }

        /// Seals and writes `entries`, in the order of their pages, a window at a time.
        ///
        /// Sealing all the units of a batch before writing any left the pool holding a whole batch of sealed arrays from
        /// one checkpoint to the next; a window's units seal into one array.
        private void writeUnits(List<Staged> entries) throws IOException {
            for (var from = 0; from < entries.size(); ) {
                var to = from;
                var bytes = 0L;
                while (to < entries.size() && (to == from || bytes + (long) entries.get(to).ref().pages() * PageFormat.SIZE <= SEAL_WINDOW)) {
                    bytes += (long) entries.get(to).ref().pages() * PageFormat.SIZE;
                    to++;
                }
                writeWindow(entries.subList(from, to), (int) bytes);
                from = to;
            }
        }

        /// Seals the units of `entries`, `bytes` in all, one after the other into an array of the pool, on every
        /// processor from [#PARALLEL_SEALS] units, and writes each run of them on consecutive pages with one call of at
        /// most [#RUN_BYTES].
        ///
        /// The caller waits for the seals, so they run on virtual threads and never in the common pool, where the tasks
        /// of a parallel update wait for this writer: a split of a seal forked to the deque of a worker parked on a
        /// join, after the caller last looked there or above a task only a free worker can take from its base, would
        /// never run, and the checkpoint with it. It waits for the units and not for the threads ([ClaimedIndexes]):
        /// an application's virtual threads may hold every carrier for as long as they compute.
        private void writeWindow(List<Staged> entries, int bytes) throws IOException {
            var count = entries.size();
            var offsets = new int[count + 1];
            for (var i = 0; i < count; i++) {
                offsets[i + 1] = offsets[i] + entries.get(i).ref().pages() * PageFormat.SIZE;
            }
            var sealed = pool.acquire(bytes);
            try {
                var helpers = count < PARALLEL_SEALS ? 0 : Runtime.getRuntime().availableProcessors() - 1;
                ClaimedIndexes.run("ledger-seal", count, helpers, i -> {
                    var entry = entries.get(i);
                    cipher.seal(entry.unit(), entry.ref().pages(), sealed, offsets[i]);
                });
                var first = 0;
                var runStart = PageFormat.offset(entries.getFirst().ref().page());
                for (var i = 1; i <= count; i++) {
                    var runLength = offsets[i] - offsets[first];
                    if (i == count || PageFormat.offset(entries.get(i).ref().page()) != runStart + runLength
                            || offsets[i + 1] - offsets[first] > RUN_BYTES) {
                        writeRun(sealed, offsets[first], runLength, runStart);
                        if (i < count) {
                            first = i;
                            runStart = PageFormat.offset(entries.get(i).ref().page());
                        }
                    }
                }
            } finally {
                pool.release(sealed);
            }
        }

        /// Writes the `length` bytes of `sealed` at `offset` to the file at `position`, with one call unless the system
        /// takes fewer.
        ///
        /// One buffer and not one per unit: Windows writes a gathering write's buffers one call at a time, and the
        /// JDK copies each heap buffer it writes into a direct one of its size.
        private void writeRun(byte[] sealed, int offset, int length, long position) throws IOException {
            var buffer = ByteBuffer.wrap(sealed, offset, length);
            while (buffer.hasRemaining()) {
                writer.write(buffer, position + (buffer.position() - offset));
            }
        }

        /// Starts forcing what the checkpoint wrote so far, beside its work, once it wrote [#FORCE_EVERY] bytes since
        /// the last force started and that one is done, while the checkpoint lags; throws the failure of that one.
        /// Holding the lock.
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
                // On this thread if its own has not started yet, which an application's virtual threads may keep from a
                // carrier for as long as they compute; otherwise this returns at once
                running.run();
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

        /// Waits, keeping the interrupt status, while `waiting` holds, which is read holding the lock and changes only
        /// where the lock is notified.
        ///
        /// A task of the common pool waits as a managed blocker, so the pool may run another task meanwhile. The wait
        /// is never interrupted: the batch it waits for always ends, whether it is written or fails.
        private void await(BooleanSupplier waiting) {
            var blocker = new Blocker(waiting);
            try {
                ForkJoinPool.managedBlock(blocker);
            } catch (InterruptedException _) {
                blocker.interrupted = true;
            }
            if (blocker.interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        /// {@return the bytes written or queued so far}
        long written() {
            synchronized (lock) {
                return written + inFlightBytes + stagedBytes;
            }
        }

        /// Drops every queued unit, for a checkpoint that failed, once the batch being written, if any, ended; any write
        /// after it throws.
        void discard() {
            synchronized (lock) {
                discarded = true;
                lock.notifyAll();
            }
            // Before the force: the end of the batch may start one
            await(() -> flushing);
            try {
                awaitForce();
            } catch (IOException _) {
                // The checkpoint failed already; its pages go back
            }
            synchronized (lock) {
                for (var entry : staged.values()) {
                    cache.remove(entry.ref().page());
                }
                for (var entry : inFlight.values()) {
                    cache.remove(entry.ref().page());
                }
                staged = new TreeMap<>();
                stagedBytes = 0;
                inFlight = Map.of();
                inFlightBytes = 0;
            }
        }

        /// A wait on the writer's lock while a condition holds.
        private final class Blocker implements ForkJoinPool.ManagedBlocker {
            private final BooleanSupplier waiting;
            // Whether an interrupt came during the wait
            private boolean interrupted;

            private Blocker(BooleanSupplier waiting) {
                this.waiting = waiting;
            }

            @Override
            public boolean block() {
                synchronized (lock) {
                    while (waiting.getAsBoolean()) {
                        try {
                            lock.wait();
                        } catch (InterruptedException _) {
                            interrupted = true;
                        }
                    }
                }
                return true;
            }

            @Override
            public boolean isReleasable() {
                synchronized (lock) {
                    return !waiting.getAsBoolean();
                }
            }
        }
    }

    /// A queued unit.
    private record Staged(PageRef ref, byte[] unit) {
    }
}

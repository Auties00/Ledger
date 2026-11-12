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
import java.util.function.BooleanSupplier;

/// The data file of a store: its units, read through the page cache and verified against the reference that named
/// them, and the superblocks.
///
/// Reads run on the calling thread, each on a channel of its own from a small pool: Windows serializes positional
/// reads on one channel, and an interrupt during a read closes the channel it ran on, which then is replaced. Writes run
/// on the one thread that writes a checkpoint, through a [Writer].
final class PageFile implements AutoCloseable {
    static final String FILE = "ledger.data";
    /// The channels reads take turns on, and the most threads [#readAll(List, byte, int)] reads on.
    static final int READ_CHANNELS = 8;
    /// The most pages [#readAll(List, byte, int)] reads with one call.
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
    /// The average read time from which reads count as cold, in microseconds: reads the OS cache serves take 1 to 5
    /// microseconds on Linux and 5 to 20 on Windows, where those of the device of a CI runner took about 145, and a
    /// Windows read out of the cache that Defender or paging slows rarely averages past 50.
    static final int COLD_READ_MICROS = 80;
    /// The pages between two units that [#prefetch(PageRef\[\], int, int\[\])] still reads with one call, throwing the
    /// pages between away: a device takes a few more pages in the same request for little more than one.
    static final int GAP_PAGES = 8;
    // The bytes of a read that count as one sample of the read time: the copy of a long read out of the OS cache must
    // not pass for a read of the device
    private static final int SAMPLE_BYTES = 32 << 10;
    // The longest read a sample records, in microseconds: twice the bound, so a read of the OS cache that a pause, a
    // page fault or a scan of the antivirus stretched moves the average of quick reads less than halfway to the bound,
    // and only three in a row pass it, where one stretched read made a full scan of a warm store read ahead, on threads
    // of its own, for the next few misses, and the average falls below the bound three quick reads after the device's
    private static final int SAMPLE_LIMIT_MICROS = 2 * COLD_READ_MICROS;
    // Whether every page file opened from now on reads as cold, whatever it measures; for tests
    private static volatile boolean forceColdNewForTests;

    private final Path path;
    private final PageCipher cipher;
    private final PageCache cache;
    private final HeapPool pool;
    private final ArrayBlockingQueue<FileChannel> readers;
    private final FileChannel writer;
    // The file of large values, which leaves point into
    private volatile BlobFile blobs;
    private volatile boolean closed;
    // The moving average of the time a read of the file takes, in microseconds; racy, as a lost or stale sample only
    // moves the average less
    private int readMicros;
    // Whether reads count as cold whatever they measure; for tests
    private volatile boolean forcedCold;

    private PageFile(Path path, PageCipher cipher, PageCache cache, HeapPool pool) throws IOException {
        this.path = path;
        this.cipher = cipher;
        this.cache = cache;
        this.pool = pool;
        this.writer = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.readers = new ArrayBlockingQueue<>(READ_CHANNELS);
        this.forcedCold = forceColdNewForTests;
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
            var plain = open(ref, kind, sealed, 0, cached);
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

    /// {@return the plaintext of the unit `ref` names, sealed at `offset` of `sealed`, in an array of its own, or
    /// `null` if it fails authentication or is not the unit `ref` names}
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    /// @param cached whether to keep the unit as the cache does, ending with its content, rather than whole
    private byte[] open(PageRef ref, byte kind, byte[] sealed, int offset, boolean cached) {
        // The header is the additional data of the tag and the open copies it as it is, so what it says holds once the
        // unit authenticates: it is checked first, and tells the bytes to keep
        var content = Bytes.i32(sealed, offset + PageFormat.LENGTH);
        if (!ref.describes(sealed, offset) || !kindMatches(sealed[offset + PageFormat.KIND], kind)
                || !PageFormat.fits(ref.pages(), content)) {
            return null;
        }
        var length = ref.pages() * PageFormat.SIZE;
        return cipher.open(sealed, offset, ref.pages(), cached ? PageFormat.cachedLength(ref.pages(), content) : length, pool);
    }

    /// Reads the first `count` units `refs` names into one array of the store's pool, sealed as the file holds them,
    /// and returns it; [#openPrefetched(PageRef, byte, byte\[\], int)] opens each, and [#releasePrefetched(byte\[\])]
    /// gives the array back once none is needed.
    ///
    /// It reads ahead of a scan whose reads are [cold][#coldReads()], which may never use what it reads, so it reports
    /// no failure: a unit it could not read has an offset of -1, and the read that needs it fails as it would have
    /// without this one. Units at most [#GAP_PAGES] pages apart are read with one call, up to [#RUN_PAGES] pages, the
    /// pages between thrown away unopened, and the calls run on up to [#READ_CHANNELS] threads at once, the caller's
    /// among them: a scan that read its units out of the device one after another waited for each in turn, about 145
    /// microseconds each on the Windows runner. It opens nothing, so its threads hold no array of the pool but the one
    /// it returns, and a scan's reads ahead leave the pool no more arrays than the scan holds at once.
    ///
    /// @param refs references to units, none of them a delta record, each at most [#RUN_PAGES] pages
    /// @param offsets receives, for each unit, where it starts in the array, or -1 if it could not be read
    /// @return the array, of the store's pool, or `null` if `count` is 0
    byte[] prefetch(PageRef[] refs, int count, int[] offsets) {
        Arrays.fill(offsets, 0, count, -1);
        if (count == 0) {
            return null;
        }
        // The references by page, each with its index in the low half
        var order = new long[count];
        for (var i = 0; i < count; i++) {
            order[i] = (long) refs[i].page() << 32 | i;
        }
        Arrays.sort(order);
        // Where each run of `order` starts, and the end of the last one; the first page of each run and the end of it
        var starts = new int[count + 1];
        var firsts = new int[count];
        var ends = new int[count];
        var runs = 0;
        for (var i = 0; i < count; i++) {
            var ref = refs[(int) order[i]];
            var unitEnd = ref.page() + ref.pages();
            if (runs == 0 || ref.page() > ends[runs - 1] + GAP_PAGES
                    || Math.max(ends[runs - 1], unitEnd) - firsts[runs - 1] > RUN_PAGES) {
                starts[runs] = i;
                firsts[runs] = ref.page();
                ends[runs++] = unitEnd;
            } else {
                ends[runs - 1] = Math.max(ends[runs - 1], unitEnd);
            }
        }
        starts[runs] = count;
        // Where each run starts in the array
        var bases = new int[runs];
        var length = 0;
        for (var run = 0; run < runs; run++) {
            bases[run] = length;
            length += (ends[run] - firsts[run]) * PageFormat.SIZE;
        }
        var sealed = pool.acquire(length);
        try {
            ClaimedIndexes.run("ledger-prefetch", runs, Math.min(READ_CHANNELS, runs) - 1, run -> prefetchRun(refs,
                    order, starts[run], starts[run + 1], firsts[run], ends[run], sealed, bases[run], offsets));
        } catch (RuntimeException _) {
            // A thread that would not start: the units left unread are read by the reads that need them
        } catch (Throwable throwable) {
            // No thread holds the array once the run returns or throws
            pool.release(sealed);
            throw throwable;
        }
        return sealed;
    }

    /// Reads the units of `refs` at `order[from]` to `order[to]`, excluded, which are sorted by page and lie in the
    /// pages from `first` to `end`, excluded, with one call into `sealed` from `base` on, and records where each starts
    /// in `offsets`; a unit it cannot read keeps an offset of -1.
    private void prefetchRun(PageRef[] refs, long[] order, int from, int to, int first, int end, byte[] sealed,
                             int base, int[] offsets) {
        try {
            readFully(sealed, base, (end - first) * PageFormat.SIZE, PageFormat.offset(first));
            for (var i = from; i < to; i++) {
                var index = (int) order[i];
                offsets[index] = base + (refs[index].page() - first) * PageFormat.SIZE;
            }
        } catch (LedgerCorruptedException | LedgerIoException | IllegalStateException _) {
            if (to - from == 1) {
                return;
            }
            // A call that fails, or that the end of the file cuts short, fails none of its units: each is read alone,
            // into its own place
            for (var i = from; i < to; i++) {
                var index = (int) order[i];
                var ref = refs[index];
                var offset = base + (ref.page() - first) * PageFormat.SIZE;
                try {
                    readFully(sealed, offset, ref.pages() * PageFormat.SIZE, PageFormat.offset(ref.page()));
                    offsets[index] = offset;
                } catch (LedgerCorruptedException | LedgerIoException | IllegalStateException _) {
                    // Read by the read that needs it, which fails as it would have
                }
            }
        }
    }

    /// {@return the plaintext of the unit `ref` names, as the cache keeps it, from an array of
    /// [#prefetch(PageRef\[\], int, int\[\])] that holds it sealed at `offset`, in an array of its own and not cached,
    /// or `null` if it fails authentication or is not the unit `ref` names}
    ///
    /// It checks and opens the unit as [#load(PageRef, byte, boolean, boolean)] does, and leaves `sealed` as it was.
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    byte[] openPrefetched(PageRef ref, byte kind, byte[] sealed, int offset) {
        try {
            return open(ref, kind, sealed, offset, true);
        } catch (IllegalStateException _) {
            // A cipher that fails other than on the tag, as the read that needs the unit then does
            return null;
        }
    }

    /// Opens into `plain` the unit `ref` names, from an array of [#prefetch(PageRef\[\], int, int\[\])] that holds it
    /// sealed at `offset`, as [#openPrefetched(PageRef, byte, byte\[\], int)] does, and leaves `sealed` as it was.
    ///
    /// @param kind the kind the unit must have, or [#NODE] for a leaf or a branch
    /// @param plain an array of [#acquireOpened(PageRef)] for the unit
    /// @return the bytes of `plain` the cache keeps of the unit, from its header to the end of its content, or -1 if it
    /// fails authentication or is not the unit `ref` names, which leaves `plain` undefined
    int openPrefetched(PageRef ref, byte kind, byte[] sealed, int offset, byte[] plain) {
        var content = Bytes.i32(sealed, offset + PageFormat.LENGTH);
        if (!ref.describes(sealed, offset) || !kindMatches(sealed[offset + PageFormat.KIND], kind)
                || !PageFormat.fits(ref.pages(), content)) {
            return -1;
        }
        try {
            return cipher.open(sealed, offset, ref.pages(), plain) ? PageFormat.cachedLength(ref.pages(), content) : -1;
        } catch (IllegalStateException _) {
            return -1;
        }
    }

    /// {@return an array of the store's pool that the unit `ref` names opens into, which
    /// [#releasePrefetched(byte\[\])] gives back}
    byte[] acquireOpened(PageRef ref) {
        return pool.acquire(ref.pages() * PageFormat.SIZE - PageFormat.TAG);
    }

    /// Gives an array of [#prefetch(PageRef\[\], int, int\[\])] or [#acquireOpened(PageRef)] back to the store's pool;
    /// no one may read it afterwards.
    ///
    /// @param array the array, or `null` for none
    void releasePrefetched(byte[] array) {
        if (array != null) {
            pool.release(array);
        }
    }

    /// {@return whether reads of the file take long enough, on average, that they come from the device rather than the
    /// OS cache: [#COLD_READ_MICROS] microseconds or more}
    ///
    /// The average moves a quarter of the way to each sample, of at most twice the bound, so it passes the bound after
    /// three or four reads of the device and falls below it after three quick ones, and two stretched reads in a row of
    /// the OS cache leave it below.
    boolean coldReads() {
        return readMicros >= COLD_READ_MICROS || forcedCold;
    }

    /// Records in the average of [#coldReads()] a read of `length` bytes that took `nanos` nanoseconds.
    void sampleRead(long nanos, int length) {
        var micros = Math.clamp(nanos / 1_000 / Math.max(1, length / SAMPLE_BYTES), 0, SAMPLE_LIMIT_MICROS);
        var average = readMicros;
        var next = average + ((micros - average) >> 2);
        // Written only when it moves: the readers of the file on every thread share its line
        if (next != average) {
            readMicros = next;
        }
    }

    /// {@return the moving average of the read time, in microseconds; for tests}
    int readMicrosForTests() {
        return readMicros;
    }

    /// Makes reads count as cold whatever they measure, or as they measure; for tests.
    void forceColdReadsForTests(boolean cold) {
        forcedCold = cold;
    }

    /// Makes every page file opened from now on count its reads as cold whatever they measure, or as they measure;
    /// for tests.
    static void forceColdNewForTests(boolean cold) {
        forceColdNewForTests = cold;
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
        readFully(array, 0, length, position);
    }

    // Reads `length` bytes of the file from `position` into `array` from `offset` on
    private void readFully(byte[] array, int offset, int length, long position) {
        var channel = takeChannel();
        var interrupted = false;
        try {
            var buffer = ByteBuffer.wrap(array, offset, length);
            var start = System.nanoTime();
            while (buffer.hasRemaining()) {
                try {
                    if (channel.read(buffer, position + buffer.position() - offset) < 0) {
                        throw new LedgerCorruptedException(path + " ends before the unit at " + position, null);
                    }
                } catch (ClosedByInterruptException _) {
                    // The interrupt closed the channel; read on a new one and give the caller its interrupt back
                    interrupted |= Thread.interrupted();
                    channel = FileChannel.open(path, StandardOpenOption.READ);
                }
            }
            sampleRead(System.nanoTime() - start, length);
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
            synchronized (lock) {
                if (staged.put(page, new Staged(ref, unit)) != null) {
                    throw new IllegalStateException("A checkpoint wrote page " + page + " twice");
                }
                stagedBytes += (long) pages * PageFormat.SIZE;
                if (stagedBytes >= STAGED_LIMIT) {
                    flush();
                }
            }
            // Outside the writer's lock: the tasks of a checkpoint all pass through it, for every unit they read too,
            // so one waiting for the cache's lock, or rebuilding its table or evicting, would hold up the others
            if (cached) {
                cache.put(ref, unit, false);
            } else {
                cache.remove(page);
            }
            return ref;
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
        /// pool, where the tasks of a parallel update wait for this same lock: a split of a seal forked to the deque of a
        /// worker parked on a join, after the caller last looked there or above a task only a free worker can take from
        /// its base, would never run, and the checkpoint with it.
        ///
        /// It waits for the units and not for the threads ([ClaimedIndexes]): an application's virtual threads may hold
        /// every carrier for as long as they compute.
        private void seal(List<Staged> entries, byte[][] sealedUnits) {
            var helpers = entries.size() < PARALLEL_SEALS ? 0 : Runtime.getRuntime().availableProcessors() - 1;
            ClaimedIndexes.run("ledger-seal", entries.size(), helpers, i -> {
                var entry = entries.get(i);
                var sealed = pool.acquire(entry.ref().pages() * PageFormat.SIZE);
                sealedUnits[i] = sealed;
                cipher.seal(entry.unit(), entry.ref().pages(), sealed, 0);
            });
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

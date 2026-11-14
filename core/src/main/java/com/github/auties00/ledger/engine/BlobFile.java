package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/// The file of large values, `ledger.blobs`: values a commit writes once, in units of their own, instead of into its
/// log frame and again at the next checkpoint.
///
/// A commit seals each of its large values into free pages before it is given a sequence, and its log frame records
/// where the unit is; the log forces this file with the segment, in parallel, so a durable commit has durable values.
/// The checkpoint that holds the commit points its leaf at the unit, with no copy. The file holds no superblock: the
/// data file's records its free pages, which list as free every unit no checkpoint has taken, since recovery claims
/// again the units of the commits it replays.
///
/// Memory keeps no copy of a value of a paged key space that a unit holds: until a checkpoint takes the unit, its
/// version names the unit ([Version#ofUnit]), and a read opens it through the page cache, screened as a read of a
/// tree's value is ([#copyValue(PageRef, int)]), so a value read once costs a read of the file and an open, as one
/// after a checkpoint does, and only a value read twice stays in the cache. A unit a commit replaced before a
/// checkpoint took it therefore waits, as one a checkpoint freed does, for every reader that may still read it
/// ([#pendSuperseded(long, List)]).
///
/// Units follow [PageFormat], kind [PageFormat#EXTERNAL], sealed under a key of their own. A unit's nonce is the store's
/// epoch, which grows with every open and is made durable before the open's first unit, then a counter: no nonce
/// repeats under the key, and whatever refers to a unit records it, so a unit left in reused pages fails the check.
///
/// It is safe for concurrent use.
final class BlobFile implements AutoCloseable {
    /// The name of the file in the store's directory.
    static final String FILE = "ledger.blobs";
    /// The most bytes one call of a channel reads or writes: the JDK moves the bytes of a heap buffer through a
    /// temporary direct buffer of the call's size, which it keeps for the thread until the thread ends, so a read of a
    /// 16 MiB unit in one call left each reading thread 16 MiB of native memory.
    private static final int BUFFER = 1 << 20;
    /// The trailing free pages, 64 MiB of them, below which [#trim()] keeps the file's length.
    private static final int TRIM_SLACK = 16_384;

    /// The channels the threads of commits and reads write and read units through.
    private static final int CHANNELS = 4;

    private final Path path;
    // Forces and cuts the file, on the store's own threads only, so no interrupt of a caller closes it under them
    private final FileChannel channel;
    // The channels callers write and read units through, each used by one thread at a time; one an interrupt closed is
    // replaced
    private final ArrayBlockingQueue<FileChannel> channels;
    private final PageCipher cipher;
    private final PageCache cache;
    private final HeapPool pool;
    private final FreeSpace space;
    // The epoch of this open, set once recovery has read the earlier ones
    private volatile int epoch;
    private final AtomicLong counter;
    // Guards space's allocations and unadopted together, so a free list never lists a unit both ways
    private final Object lock;
    // The units no published checkpoint points to: in flight, or claimed by recovery
    private final HashSet<PageRef> unadopted;
    // Makes the epoch durable before the first unit of this open, or null once it is
    private volatile Runnable epochGate;
    // Whether a unit was sealed under this open's epoch, which a later open must then not reuse
    private volatile boolean epochTaken;
    // Set once the file closes: a caller then gets no channel, rather than one opened again that nothing closes
    private volatile boolean closed;
    // Runs before each read of a unit from the file, or null; for tests that race a read with what frees its unit
    private volatile Runnable readHookForTests;

    private BlobFile(Path path, FileChannel channel, PageCipher cipher, PageCache cache, HeapPool pool, FreeSpace space) throws IOException {
        this.path = path;
        this.channel = channel;
        this.channels = new ArrayBlockingQueue<>(CHANNELS);
        try {
            for (var i = 0; i < CHANNELS; i++) {
                channels.add(FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE));
            }
        } catch (IOException exception) {
            for (var open : channels) {
                open.close();
            }
            throw exception;
        }
        this.cipher = cipher;
        this.cache = cache;
        this.pool = pool;
        this.space = space;
        this.counter = new AtomicLong();
        this.lock = new Object();
        this.unadopted = new HashSet<>();
    }

    /// Opens the file in `directory`, creating it if it does not exist.
    ///
    /// @param pageCount the pages the last checkpoint knew the file to use, or 0 without one
    /// @param free the free pages among them
    static BlobFile open(Path directory, PageCipher cipher, PageCache cache, HeapPool pool, int pageCount, BitSet free) throws IOException {
        var path = directory.resolve(FILE);
        if (!Files.exists(path)) {
            var temporary = directory.resolve(FILE + ".tmp");
            try (var created = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                created.force(true);
            }
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
            Durability.forceDirectory(directory);
        }
        var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        var space = new FreeSpace(Math.max(pageCount, PageFormat.FIRST_UNIT_PAGE), free);
        return new BlobFile(path, channel, cipher, cache, pool, space);
    }

    /// Sets the epoch of this open, above every earlier one, once recovery has read them.
    void begin(int epoch) {
        this.epoch = epoch;
    }

    /// Sets what makes the epoch durable, which the first unit of this open waits for.
    void epochGate(Runnable gate) {
        this.epochGate = gate;
    }

    /// {@return the epoch of this open}
    int epoch() {
        return epoch;
    }

    /// {@return whether a unit was sealed under this open's epoch, even one written by a commit that failed}
    boolean epochTaken() {
        return epochTaken;
    }

    /// Seals the `length` bytes of `value` at `offset` into free pages and writes them, without forcing them.
    ///
    /// The unit is in flight until a checkpoint takes it or [#release(PageRef)] frees it.
    ///
    /// @return where the unit is
    /// @throws LedgerIoException if the file cannot be written; the pages are free again
    PageRef write(byte[] value, int offset, int length) {
        var gate = epochGate;
        if (gate != null) {
            gate.run();
            epochGate = null;
        }
        epochTaken = true;
        var pages = PageFormat.pagesFor(length);
        PageRef ref;
        synchronized (lock) {
            ref = new PageRef(space.allocate(pages), pages, counter.getAndIncrement(), epoch);
            unadopted.add(ref);
        }
        var header = new byte[PageFormat.HEADER];
        PageFile.header(header, PageFormat.EXTERNAL, 0, ref.page(), pages, length, ref.nonceHigh(), ref.nonceLow());
        var buffer = pool.acquire(BUFFER);
        var through = new FileChannel[]{takeChannel()};
        var interrupted = new boolean[1];
        try {
            var position = new long[]{PageFormat.offset(ref.page())};
            cipher.seal(header, value, offset, length, pages, buffer, (bytes, count) -> {
                var start = position[0];
                while (true) {
                    try {
                        var source = ByteBuffer.wrap(bytes, 0, count);
                        var at = start;
                        while (source.hasRemaining()) {
                            at += through[0].write(source, at);
                        }
                        position[0] = at;
                        return;
                    } catch (ClosedByInterruptException _) {
                        // The interrupt closed the channel: the same bytes again, at the same offset, on a new one,
                        // and the caller gets its interrupt back
                        interrupted[0] |= Thread.interrupted();
                        through[0] = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    }
                }
            });
        } catch (IOException exception) {
            release(ref);
            throw new LedgerIoException(path + " could not be written", exception);
        } finally {
            giveBack(through[0]);
            pool.release(buffer);
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
        return ref;
    }

    /// {@return the value of `length` bytes the unit `ref` holds, read-only, as a slice of the unit's plaintext}
    ///
    /// The unit comes from the page cache or the file, which screens it as a read of a tree's value: the cache takes
    /// it at its second read. A view goes to a visitor, which may keep it, so its array is never recycled.
    ///
    /// @throws LedgerCorruptedException if the unit fails authentication, is not the one `ref` names, or does not
    ///                                  hold `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    MemorySegment value(PageRef ref, int length) {
        var plain = read(ref, true, true, true);
        checkLength(ref, plain, length);
        return MemorySegment.ofArray(plain).asSlice(PageFormat.HEADER, length).asReadOnly();
    }

    /// {@return the value of `length` bytes the unit `ref` holds, in a new array}
    ///
    /// The unit comes from the page cache or the file, which screens it as a read of a tree's value: the cache takes
    /// it at its second read, if it [holds][PageCache#holds(long)] units of its size, so a value read once leaves
    /// nothing reachable. A unit above [HeapPool#MAX_POOLED] that the cache does not take is read and opened into
    /// arrays of the pool's large spares ([HeapPool#acquireLarge(int)]), which go back once the value is copied out,
    /// so the read allocates no array but the caller's; it is never kept weakly for a
    /// [recall][PageCache#recall(long, PageRef)], since a later read writes the spare.
    ///
    /// @throws LedgerCorruptedException if the unit fails authentication, is not the one `ref` names, or does not
    ///                                  hold `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    byte[] copyValue(PageRef ref, int length) {
        if ((long) ref.pages() * PageFormat.SIZE <= HeapPool.MAX_POOLED) {
            var plain = read(ref, true, true, true);
            checkLength(ref, plain, length);
            return Segments.toBytes(plain, PageFormat.HEADER, length);
        }
        var key = PageCache.key(PageCache.BLOBS, ref.page());
        var held = cache.get(key, ref);
        if (held == null) {
            held = cache.recall(key, ref);
        }
        if (held != null) {
            checkLength(ref, held, length);
            return Segments.toBytes(held, PageFormat.HEADER, length);
        }
        var plain = readUnit(ref, true, true);
        if (plain == null) {
            throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
        }
        try {
            checkLength(ref, plain, length);
        } catch (LedgerCorruptedException exception) {
            pool.releaseLarge(plain);
            throw exception;
        }
        if (cache.holds((long) ref.pages() * PageFormat.SIZE) && cache.admits(key, ref)) {
            // The cache holds it from now on, or drops it if it closed: never back to the spares, as readers may
            // still read it
            cache.put(key, ref, plain, true);
            return Segments.toBytes(plain, PageFormat.HEADER, length);
        }
        var copy = Segments.toBytes(plain, PageFormat.HEADER, length);
        pool.releaseLarge(plain);
        return copy;
    }

    // Throws if `plain`, the plaintext of the unit `ref` names, does not hold a value of `length` bytes
    private void checkLength(PageRef ref, byte[] plain, int length) {
        var content = Bytes.i32(plain, PageFormat.LENGTH);
        if (content != length) {
            throw new LedgerCorruptedException(path + " holds " + content + " bytes at page " + ref.page()
                    + " for a value of " + length, null);
        }
    }

    /// {@return the plaintext of the unit `ref` names, header included}
    ///
    /// @param cached whether to keep the unit in the page cache
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    /// @throws LedgerIoException if the file cannot be read
    byte[] read(PageRef ref, boolean cached) {
        return read(ref, cached, true);
    }

    /// {@return the plaintext of the unit `ref` names, from the cache or the file}
    ///
    /// A unit read for the cache may end with its content ([PageFormat#cachedLength(int, int)]); one read past it holds
    /// its whole pages.
    ///
    /// @param cached whether to keep the unit in the cache
    /// @param referenced whether a unit the cache takes counts as read, rather than as the first it evicts
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    byte[] read(PageRef ref, boolean cached, boolean referenced) {
        return read(ref, cached, referenced, false);
    }

    /// {@return the plaintext of the unit `ref` names, from the cache or the file}
    ///
    /// A unit of half a region of G1 or more that a screened read does not cache is not kept weakly for a
    /// [recall][PageCache#recall(long, PageRef)] either: G1 frees a humongous array no one reaches at its next young
    /// collection, and one reached weakly only after the next marking, which a store reading large values once may
    /// wait for with all of them committed.
    ///
    /// @param cached whether to keep the unit in the cache
    /// @param referenced whether a unit the cache takes counts as read, rather than as the first it evicts
    /// @param screened whether a unit read as read goes to the cache only if it
    ///                 [admits][PageCache#admits(long, PageRef)] it
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    byte[] read(PageRef ref, boolean cached, boolean referenced, boolean screened) {
        var key = PageCache.key(PageCache.BLOBS, ref.page());
        var hit = cache.get(key, ref);
        if (hit != null) {
            return hit;
        }
        var screen = cached && referenced && screened;
        if (cached && referenced) {
            var recalled = cache.recall(key, ref);
            if (recalled != null) {
                return recalled;
            }
        }
        var plain = readUnit(ref, cached, false);
        if (plain == null) {
            throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
        }
        if (screen) {
            if (cache.admits(key, ref)) {
                cache.put(key, ref, plain, true);
            } else if (!humongous(plain.length)) {
                cache.refuse(key, ref, plain);
            }
        } else if (cached) {
            cache.put(key, ref, plain, referenced);
        }
        return plain;
    }

    /// {@return whether G1 gives an array of `length` bytes regions of its own: half a region or more}
    private static boolean humongous(int length) {
        var region = HeapPool.region();
        return region > 0 && length >= region / 2;
    }

    /// {@return the plaintext of the unit `ref` names, its whole pages, or `null` if the file does not hold it whole}
    ///
    /// @throws LedgerIoException if the file cannot be read
    byte[] readUnit(PageRef ref) {
        return readUnit(ref, false, false);
    }

    /// {@return whether the file holds the unit `ref` names whole and authenticated, with `length` bytes of content}
    ///
    /// It keeps nothing: a unit above [HeapPool#MAX_POOLED] is read and opened into the pool's large spares, which go
    /// back at once.
    ///
    /// @throws LedgerIoException if the file cannot be read
    boolean authenticates(PageRef ref, long length) {
        var plain = readUnit(ref, false, true);
        if (plain == null) {
            return false;
        }
        var holds = Bytes.i32(plain, PageFormat.LENGTH) == length;
        pool.releaseLarge(plain);
        return holds;
    }

    /// {@return the plaintext of the unit `ref` names, or `null` if the file does not hold it whole}
    ///
    /// A unit above [HeapPool#MAX_POOLED] is read into an array of the pool's large spares, which goes back at the
    /// end. The channel reads at most [#BUFFER] bytes a call.
    ///
    /// @param cached whether the unit is for the cache, and may end with its content
    /// @param spare whether a unit above [HeapPool#MAX_POOLED] kept whole is opened into an array of the pool's large
    ///              spares, which the caller gives back through [HeapPool#releaseLarge(byte\[\])] unless it hands it to
    ///              the cache; such an array may be longer than the unit, and hold another unit's bytes past it
    /// @throws LedgerIoException if the file cannot be read
    private byte[] readUnit(PageRef ref, boolean cached, boolean spare) {
        var hook = readHookForTests;
        if (hook != null) {
            hook.run();
        }
        var pages = ref.pages();
        var length = pages * PageFormat.SIZE;
        var large = length > HeapPool.MAX_POOLED;
        var sealed = large ? pool.acquireLarge(length) : pool.acquire(length);
        var through = takeChannel();
        var interrupted = false;
        try {
            var target = ByteBuffer.wrap(sealed, 0, length);
            var position = PageFormat.offset(ref.page());
            while (target.position() < length) {
                target.limit(Math.min(length, target.position() + BUFFER));
                try {
                    if (through.read(target, position + target.position()) < 0) {
                        return null;
                    }
                } catch (ClosedByInterruptException _) {
                    // The interrupt closed the channel; read on a new one, from where the last read ended, and give
                    // the caller its interrupt back
                    interrupted |= Thread.interrupted();
                    through = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                }
            }
            // The header is the additional data of the tag and the open copies it as it is, so what it says holds once
            // the unit authenticates: it is checked first, and tells the bytes to keep
            var content = Bytes.i32(sealed, PageFormat.LENGTH);
            if (!ref.describes(sealed) || sealed[PageFormat.KIND] != PageFormat.EXTERNAL
                    || !PageFormat.fits(pages, content)) {
                return null;
            }
            var keep = cached ? PageFormat.cachedLength(pages, content) : length;
            if (spare && large && keep == length) {
                // Out of place, so the open runs the AES-GCM intrinsic that decrypts and authenticates in one pass
                var plain = pool.acquireLarge(length);
                if (cipher.open(sealed, 0, pages, plain)) {
                    return plain;
                }
                pool.releaseLarge(plain);
                return null;
            }
            return cipher.open(sealed, 0, pages, keep, pool);
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            giveBack(through);
            if (large) {
                pool.releaseLarge(sealed);
            } else {
                pool.release(sealed);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// {@return a channel for a caller's write or read, open, waiting for one if every one is in use}
    ///
    /// @throws IllegalStateException if the file closed
    private FileChannel takeChannel() {
        if (closed) {
            throw new IllegalStateException("The store is closed");
        }
        var interrupted = false;
        try {
            while (true) {
                try {
                    var taken = channels.take();
                    if (taken.isOpen()) {
                        return taken;
                    }
                    if (closed) {
                        channels.offer(taken);
                        throw new IllegalStateException("The store is closed");
                    }
                    // An interrupt closed it after its last use, and it could not be replaced then
                    try {
                        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                    } catch (IOException exception) {
                        channels.offer(taken);
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

    /// Gives back `through`, a channel [#takeChannel()] gave, and closes it if the file closed meanwhile.
    private void giveBack(FileChannel through) {
        channels.offer(through);
        if (closed) {
            try {
                through.close();
            } catch (IOException _) {
                // Nothing was left unforced through a caller's channel
            }
        }
    }

    /// Forces every unit written so far, and the length of the file.
    void force() throws IOException {
        channel.force(true);
    }

    /// Frees the pages of `ref` at once: a unit of a commit no reader can see, which no log frame names.
    void release(PageRef ref) {
        synchronized (lock) {
            unadopted.remove(ref);
            space.release(ref.page(), ref.pages());
        }
        cache.remove(PageCache.key(PageCache.BLOBS, ref.page()));
    }

    /// Records that a published checkpoint's tree points to `ref`, which a later one frees through [#pend].
    void adopt(PageRef ref) {
        synchronized (lock) {
            unadopted.remove(ref);
        }
    }

    /// Takes again the pages of `ref`, a unit a replayed commit wrote, while the store opens.
    ///
    /// @throws LedgerCorruptedException if another unit uses one of them
    void claim(PageRef ref) {
        synchronized (lock) {
            if (!space.claim(ref.page(), ref.pages())) {
                throw new LedgerCorruptedException(path + " has two values in page " + ref.page(), null);
            }
            unadopted.add(ref);
        }
    }

    /// Frees `units`, which a published checkpoint's tree no longer points to, once every reader is past `published`.
    void pend(long published, List<PageRef> units) {
        space.pend(published, units);
    }

    /// Frees `units`, which no published checkpoint's tree points to and whose versions commits replaced before a
    /// checkpoint took them, once every reader is past `published`: a reader below the commit that replaced one may
    /// still read it, as memory keeps no copy of a paged value ([#copyValue(PageRef, int)]).
    ///
    /// They leave the units in flight and join the pending ones at once, holding the lock [#durableRuns] takes, so a
    /// checkpoint lists each of them as free exactly once. The list is kept as it is, so no one may change it from now
    /// on.
    void pendSuperseded(long published, List<PageRef> units) {
        if (units.isEmpty()) {
            return;
        }
        synchronized (lock) {
            for (var unit : units) {
                unadopted.remove(unit);
            }
            space.pend(published, units);
        }
    }

    /// Frees what [#pend] holds for checkpoints published before `oldestReader`.
    void releasePassed(long oldestReader) {
        space.releasePassed(oldestReader);
    }

    /// {@return the lists of units [#releasePassed(long)] freed since the last call, which the page cache may still
    /// hold under [PageCache#BLOBS]; empty if none}
    List<List<PageRef>> takeReleased() {
        return space.takeReleased();
    }

    /// {@return the pages of the file, and the runs of them a store reopened at the checkpoint being written may use}
    ///
    /// They are the free and pending pages, `alsoFree`, and every unit no tree points to but those in `adopted`, the
    /// ones the checkpoint's trees take: a unit in flight comes back with the commit recovery replays. Both are taken
    /// at once, since commits keep writing units while the checkpoint runs: pages a later unit takes are free here.
    Runs durableRuns(Collection<PageRef> adopted, List<PageRef> alsoFree) {
        synchronized (lock) {
            var free = new ArrayList<>(alsoFree);
            for (var unit : unadopted) {
                if (!adopted.contains(unit)) {
                    free.add(unit);
                }
            }
            return new Runs(space.pageCount(), space.durableRuns(free));
        }
    }

    /// The free pages of the file as a checkpoint records them.
    ///
    /// @param pageCount the pages of the file
    /// @param runs the free runs among them, as pairs of first page and length
    record Runs(int pageCount, List<int[]> runs) {
    }

    /// {@return the pages of the file in use, those past it being free}
    int pageCount() {
        return space.pageCount();
    }

    /// Cuts the file before its trailing free pages, once they are at least half of it and [#TRIM_SLACK].
    ///
    /// The commits write the file while it runs, so a file cut after every checkpoint would grow again with every
    /// unit, and growing a file costs a force more than writing it: on NTFS, a unit past the end adds a metadata write
    /// to the force that makes it durable. Kept long, the file takes new units into the pages old ones freed.
    void trim() throws IOException {
        synchronized (lock) {
            var end = space.trim(Math.max(TRIM_SLACK, space.pageCount() / 2));
            if (end > 0) {
                channel.truncate(PageFormat.offset(end));
            }
        }
    }

    /// Makes `hook` run before each read of a unit from the file, or stops if `null`; for tests that race a read with
    /// the commits and checkpoints that free its unit.
    void readHookForTests(Runnable hook) {
        this.readHookForTests = hook;
    }

    /// {@return the array the page cache holds for the unit `ref` names, or `null`; for tests}
    byte[] cachedForTests(PageRef ref) {
        return cache.get(PageCache.key(PageCache.BLOBS, ref.page()), ref);
    }

    /// {@return the units no published checkpoint points to, for tests}
    int unadoptedForTests() {
        synchronized (lock) {
            return unadopted.size();
        }
    }

    @Override
    public void close() {
        // Before the channels close: a caller that takes one after sees it, and one that gives one back closes it
        closed = true;
        try {
            channel.close();
        } catch (IOException _) {
            // Every unit a commit relies on was forced with its log group
        }
        for (var open : channels) {
            try {
                open.close();
            } catch (IOException _) {
                // Nothing was left unforced through a caller's channel
            }
        }
    }
}

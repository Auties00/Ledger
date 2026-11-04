package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
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
import java.util.concurrent.atomic.AtomicLong;

/// The file of large values, `ledger.blobs`: values a commit writes once, in units of their own, instead of into its
/// log frame and again at the next checkpoint.
///
/// A commit seals each of its large values into free pages before it takes the writer lock, and its log frame records
/// where the unit is; the log forces this file with the segment, in parallel, so a durable commit has durable values.
/// The checkpoint that holds the commit points its leaf at the unit, with no copy. The file holds no superblock: the
/// data file's records its free pages, which list as free every unit no checkpoint has taken, since recovery claims
/// again the units of the commits it replays.
///
/// Units follow [PageFormat], kind [PageFormat#EXTERNAL], sealed under a key of their own. A unit's nonce is the store's
/// epoch, which grows with every open and is made durable before the open's first unit, then a counter: no nonce
/// repeats under the key, and whatever refers to a unit records it, so a unit left in reused pages fails the check.
///
/// It is safe for concurrent use.
final class BlobFile implements AutoCloseable {
    /// The name of the file in the store's directory.
    static final String FILE = "ledger.blobs";
    private static final int BUFFER = 1 << 20;

    private final Path path;
    private final FileChannel channel;
    // Empties the drive's cache for the file on macOS, or null
    private final FullSync.Handle fullSync;
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

    private BlobFile(Path path, FileChannel channel, PageCipher cipher, PageCache cache, HeapPool pool, FreeSpace space) throws IOException {
        this.path = path;
        this.channel = channel;
        this.fullSync = FullSync.open(path);
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
                FullSync.force(created, temporary, true);
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

    /// Seals `value` into free pages and writes it, without forcing it.
    ///
    /// The unit is in flight until a checkpoint takes it or [#release(PageRef)] frees it.
    ///
    /// @return where the unit is
    /// @throws LedgerIoException if the file cannot be written; the pages are free again
    PageRef write(MemorySegment value) {
        var gate = epochGate;
        if (gate != null) {
            gate.run();
            epochGate = null;
        }
        var pages = PageFormat.pagesFor(value.byteSize());
        PageRef ref;
        synchronized (lock) {
            ref = new PageRef(space.allocate(pages), pages, counter.getAndIncrement(), epoch);
            unadopted.add(ref);
        }
        var header = new byte[PageFormat.HEADER];
        PageFile.header(header, PageFormat.EXTERNAL, 0, ref.page(), pages, Math.toIntExact(value.byteSize()), ref.nonceHigh(), ref.nonceLow());
        var buffer = pool.acquire(BUFFER);
        try {
            var position = new long[]{PageFormat.offset(ref.page())};
            cipher.seal(header, value, pages, buffer, (bytes, length) -> {
                var source = ByteBuffer.wrap(bytes, 0, length);
                while (source.hasRemaining()) {
                    position[0] += channel.write(source, position[0]);
                }
            });
        } catch (IOException exception) {
            release(ref);
            throw new LedgerIoException(path + " could not be written", exception);
        } finally {
            pool.release(buffer);
        }
        return ref;
    }

    /// {@return the plaintext of the unit `ref` names, header included}
    ///
    /// @param cached whether to keep the unit in the page cache
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    /// @throws LedgerIoException if the file cannot be read
    byte[] read(PageRef ref, boolean cached) {
        var key = PageCache.key(PageCache.BLOBS, ref.page());
        var hit = cache.get(key, ref);
        if (hit != null) {
            return hit;
        }
        var plain = readUnit(ref);
        if (plain == null) {
            throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
        }
        if (cached) {
            cache.put(key, ref, plain, true);
        }
        return plain;
    }

    /// {@return the plaintext of the unit `ref` names, or `null` if the file does not hold it whole}
    ///
    /// @throws LedgerIoException if the file cannot be read
    byte[] readUnit(PageRef ref) {
        var length = ref.pages() * PageFormat.SIZE;
        var sealed = pool.acquire(length);
        try {
            var target = ByteBuffer.wrap(sealed, 0, length);
            var position = PageFormat.offset(ref.page());
            while (target.hasRemaining()) {
                if (channel.read(target, position + target.position()) < 0) {
                    return null;
                }
            }
            var plain = new byte[length];
            if (!cipher.open(sealed, 0, ref.pages(), plain) || !ref.describes(plain) || plain[PageFormat.KIND] != PageFormat.EXTERNAL) {
                return null;
            }
            return plain;
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            pool.release(sealed);
        }
    }

    /// Forces every unit written so far, and the length of the file.
    void force() throws IOException {
        if (fullSync != null) {
            fullSync.force(channel, true);
        } else {
            channel.force(true);
        }
    }

    /// Frees the pages of `ref` at once: a unit no commit made durable, or one a checkpoint superseded.
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

    /// Frees what [#pend] holds for checkpoints published before `oldestReader`.
    void releasePassed(long oldestReader) {
        space.releasePassed(oldestReader);
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

    /// Cuts the file before its trailing free pages, if any.
    void trim() throws IOException {
        synchronized (lock) {
            var end = space.trim();
            if (end > 0) {
                channel.truncate(PageFormat.offset(end));
            }
        }
    }

    /// {@return the units no published checkpoint points to, for tests}
    int unadoptedForTests() {
        synchronized (lock) {
            return unadopted.size();
        }
    }

    @Override
    public void close() {
        if (fullSync != null) {
            fullSync.close();
        }
        try {
            channel.close();
        } catch (IOException _) {
            // Every unit a commit relies on was forced with its log group
        }
    }
}

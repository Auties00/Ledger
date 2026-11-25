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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
/// tree's value is ([#copyValue(PageRef, int, int)], [#scanValue(PageRef, int, boolean, int)]), so a value read once
/// costs a read of the file and an open, as one after a checkpoint does, and only a value a point read read twice, or a
/// scan four times among its first units, stays in the cache. A unit a commit replaced before a
/// checkpoint took it therefore waits, as one a checkpoint freed does, for every reader that may still read it
/// ([#pendSuperseded(long, List)]).
///
/// Units follow [PageFormat], kind [PageFormat#EXTERNAL], sealed under a key of their own. A unit's nonce is the store's
/// epoch, which grows with every open and is made durable before the open's first unit, then a counter below 2^48: no
/// nonce repeats under the key, neither a unit's nor one of a record of a unit of format [PageFormat#RECORDS], and
/// whatever refers to a unit records it, so a unit left in reused pages fails the check. A store that encrypts writes a
/// unit whose content fits in one record ([PageFormat#FIRST_RECORD_CONTENT]) in format [PageFormat#CLOSE_TAG], whose
/// tag follows the content, so a read opens the content alone, straight into the array it returns, and a longer one in
/// format [PageFormat#RECORDS], whose records a read takes one at a time through an array of the pool of
/// [PageFormat#RECORD] bytes and opens each straight into its place in the array it returns: no read or write of a unit
/// takes an array of the unit's size but the plaintext. Its readers read units of the first format and of format
/// [PageFormat#CLOSE_TAG] of any length, which earlier builds wrote, as well.
///
/// It is safe for concurrent use.
final class BlobFile implements AutoCloseable {
    /// The name of the file in the store's directory.
    static final String FILE = "ledger.blobs";
    /// The most bytes one call of a channel reads or writes: the JDK moves the bytes of a heap buffer through a
    /// temporary direct buffer of the call's size, which it keeps for the thread until the thread ends, so a read of a
    /// 16 MiB unit in one call left each reading thread 16 MiB of native memory.
    private static final int BUFFER = 1 << 20;
    /// The shortest buffer a unit is sealed through.
    private static final int MIN_BUFFER = 64 << 10;
    /// The units one open may seal: the counter of a unit's nonce keeps the top 16 bits of its 8 bytes for the index of
    /// a record of a unit of format [PageFormat#RECORDS] ([PageCipher#RECORD_SHIFT]).
    static final long UNITS_PER_OPEN = 1L << PageCipher.RECORD_SHIFT;
    /// The trailing free pages, 64 MiB of them, below which [#trim()] keeps the file's length.
    private static final int TRIM_SLACK = 16_384;

    /// The most channels the threads of commits and reads write and read units through at once.
    private static final int CHANNELS = 4;

    private final Path path;
    // Forces and cuts the file, on the store's own threads only, so no interrupt of a caller closes it under them
    private final FileChannel channel;
    // The channels callers write and read units through, each used by one thread at a time and opened as callers at
    // once need them; one an interrupt closed is replaced
    private final ChannelPool channels;
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
    // Runs before each read of a unit from the file, or null; for tests that race a read with what frees its unit
    private volatile Runnable readHookForTests;
    // The format a store that encrypts writes its units in, as earlier builds did, or 0 for the one their length
    // takes; for tests of files that hold several
    private volatile byte formatForTests;

    private BlobFile(Path path, FileChannel channel, PageCipher cipher, PageCache cache, HeapPool pool, FreeSpace space) throws IOException {
        this.path = path;
        this.channel = channel;
        this.channels = new ChannelPool(path, CHANNELS, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
        try {
            return new BlobFile(path, channel, cipher, cache, pool, space);
        } catch (IOException | RuntimeException failure) {
            channel.close();
            throw failure;
        }
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
    /// A write that fails in any way, the file's, the cipher's or a closed store's, frees the unit's pages and gives
    /// back what it took to write it.
    ///
    /// A store that encrypts writes a unit of format [PageFormat#RECORDS] through an array of the pool of
    /// [PageFormat#RECORD] bytes, one call of the channel per record, and a shorter unit through one of its own size,
    /// at least [#MIN_BUFFER] bytes; one without encryption writes through one of the unit's size, at most [#BUFFER].
    ///
    /// @return where the unit is
    /// @throws LedgerIoException if the file cannot be written; the pages are free again
    /// @throws IllegalStateException if this open sealed [#UNITS_PER_OPEN] units, whose nonces leave no room for more
    PageRef write(byte[] value, int offset, int length) {
        var gate = epochGate;
        if (gate != null) {
            gate.run();
            epochGate = null;
        }
        epochTaken = true;
        var format = format(length);
        var pages = PageFormat.pagesFor(format, length);
        var unitBytes = (long) pages * PageFormat.SIZE;
        var buffer = pool.acquire(format == PageFormat.RECORDS ? PageFormat.RECORD
                : (int) Math.max(MIN_BUFFER, Math.min(BUFFER, unitBytes)));
        FileChannel taken;
        try {
            taken = channels.take();
        } catch (RuntimeException | Error failure) {
            pool.release(buffer);
            throw failure;
        }
        var through = new FileChannel[]{taken};
        var interrupted = new boolean[1];
        PageRef ref = null;
        try {
            synchronized (lock) {
                var count = counter.get();
                if (count >= UNITS_PER_OPEN) {
                    throw new IllegalStateException(path + " has sealed " + count + " units since the store opened: reopen it");
                }
                var unit = new PageRef(space.allocate(pages), pages, count, epoch);
                counter.set(count + 1);
                unadopted.add(unit);
                ref = unit;
            }
            var header = new byte[PageFormat.HEADER];
            PageFile.header(header, PageFormat.EXTERNAL, 0, ref.page(), pages, length, ref.nonceHigh(), ref.nonceLow());
            header[PageFormat.FORMAT] = format;
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
                        through[0] = channels.reopen();
                    }
                }
            });
        } catch (IOException exception) {
            release(ref);
            throw new LedgerIoException(path + " could not be written", exception);
        } catch (RuntimeException | Error failure) {
            if (ref != null) {
                release(ref);
            }
            throw failure;
        } finally {
            channels.give(through[0]);
            pool.release(buffer);
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
        return ref;
    }

    /// {@return the format of the unit of a value of `length` bytes: the first format without encryption; with it,
    /// [PageFormat#RECORDS] if the value takes more than one record, else [PageFormat#CLOSE_TAG]}
    private byte format(int length) {
        if (!cipher.encrypts()) {
            return PageFormat.VERSION;
        }
        var forced = formatForTests;
        if (forced != 0) {
            return forced;
        }
        return length > PageFormat.FIRST_RECORD_CONTENT ? PageFormat.RECORDS : PageFormat.CLOSE_TAG;
    }

    /// {@return whether `header`, at `offset` of its array, is that of the unit `ref` names: of its pages and nonce,
    /// of kind [PageFormat#EXTERNAL], of a format the units of this file take, and with a content its pages hold in
    /// that format}
    ///
    /// The header is the additional data of the unit's tags, so what it says holds once the unit authenticates; it
    /// is checked first, so that no read is sized by a header no unit of this file has.
    private boolean header(PageRef ref, byte[] header, int offset) {
        if (!ref.describes(header, offset) || header[offset + PageFormat.KIND] != PageFormat.EXTERNAL) {
            return false;
        }
        var format = header[offset + PageFormat.FORMAT];
        var known = cipher.encrypts()
                ? format == PageFormat.VERSION || format == PageFormat.CLOSE_TAG || format == PageFormat.RECORDS
                : format == PageFormat.VERSION;
        return known && PageFormat.fits(format, ref.pages(), Bytes.i32(header, offset + PageFormat.LENGTH));
    }

    /// {@return the value of `length` bytes the unit `ref` holds, read-only, as a slice of the unit's plaintext}
    ///
    /// The unit comes from the page cache or the file, which screens it as a point read of a tree's value: the cache
    /// takes it at its second read. A view goes to a visitor, which may keep it, so its array is never recycled.
    ///
    /// @throws LedgerCorruptedException if the unit fails authentication, is not the one `ref` names, or does not
    ///                                  hold `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    MemorySegment value(PageRef ref, int length) {
        return value(ref, length, true, true, PageCache.POINT);
    }

    /// {@return the value of `length` bytes the unit `ref` holds, read-only, as a slice of the unit's plaintext, read
    /// as a scan's cursor reads the unit of a value of its tree}
    ///
    /// A unit the scan still [admits][TreeCursor#ADMITTED] is screened as `screen` asks; past them, the read uses the
    /// unit the cache holds and adds one it reads only while the cache [has room][PageCache#hasRoom(long)] for it, as
    /// an entry the cache evicts before any other. A long scan of values no checkpoint took so evicts nothing, as one
    /// of the same values after a checkpoint does not.
    ///
    /// @param admitted whether the scan still admits the units it reads
    /// @param screen the screening of an admitted unit: [PageCache#UNSCREENED] or [PageCache#SCAN]
    /// @throws LedgerCorruptedException if the unit fails authentication, is not the one `ref` names, or does not
    ///                                  hold `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    MemorySegment scanValue(PageRef ref, int length, boolean admitted, int screen) {
        var cached = admitted || cache.hasRoom(PageFormat.cachedLength(ref.pages(), length));
        return value(ref, length, cached, admitted, screen);
    }

    // The value of `length` bytes the unit `ref` holds, read-only, as a slice of the plaintext read(ref, cached,
    // referenced, screen) gives
    private MemorySegment value(PageRef ref, int length, boolean cached, boolean referenced, int screen) {
        var plain = read(ref, cached, referenced, screen);
        checkLength(ref, plain, length);
        return MemorySegment.ofArray(plain).asSlice(PageFormat.HEADER, length).asReadOnly();
    }

    /// {@return the value of `length` bytes the unit `ref` holds, in a new array}
    ///
    /// The unit comes from the page cache or the file, which screens it as `screen` asks: a point read's is taken at
    /// its second read, if the cache [holds][PageCache#holds(long)] units of its size, so a value read once leaves
    /// nothing reachable. The cache's verdict comes before the read, so a unit above [HeapPool#MAX_POOLED] that the
    /// cache does not take is read for the caller's array alone, and never kept weakly for a
    /// [recall][PageCache#recall(long, PageRef, int)]: in a store that encrypts, a unit of format [PageFormat#RECORDS]
    /// is read a record at a time into an array of the pool, each record opened straight into its place in the caller's
    /// array, and a unit of an earlier format is read up to its tag into one of the pool's large spares
    /// ([HeapPool#acquireLarge(int)]), which goes back at the end, and its content opened straight into the caller's
    /// array ([#openValue(PageRef, int)]); in one without encryption, it is read straight into the caller's array, its
    /// header and the bytes past its content into an array of the pool, and its checksum checked over the three
    /// ([#readContent(PageRef, int)]). One the cache takes is opened into a large spare, which the cache keeps: a unit
    /// of format [PageFormat#RECORDS] a record at a time, one of an earlier format after it was read into a second
    /// spare, or, without encryption, the spare it was read into once its checksum matches. A spare the cache takes is
    /// zeroed past the unit's content first, so the cache holds no byte an earlier read, of this store or of another
    /// that shares the pool, left in it. A value that a version names and one that a leaf of a tree points to copy out
    /// the same way.
    ///
    /// @param screen the read's screening: [PageCache#UNSCREENED], [PageCache#POINT] or [PageCache#SCAN]
    /// @throws LedgerCorruptedException if the unit fails authentication, is not the one `ref` names, or does not
    ///                                  hold `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    byte[] copyValue(PageRef ref, int length, int screen) {
        if ((long) ref.pages() * PageFormat.SIZE <= HeapPool.MAX_POOLED) {
            var plain = read(ref, true, true, screen);
            checkLength(ref, plain, length);
            return Segments.toBytes(plain, PageFormat.HEADER, length);
        }
        var key = PageCache.key(PageCache.BLOBS, ref.page());
        var held = cache.get(key, ref);
        if (held == null) {
            held = cache.recall(key, ref, screen);
        }
        if (held != null) {
            checkLength(ref, held, length);
            return Segments.toBytes(held, PageFormat.HEADER, length);
        }
        var unitBytes = (long) ref.pages() * PageFormat.SIZE;
        // The cache's verdict comes first: a unit it does not take needs no array but the caller's
        if (!cache.holds(unitBytes) || !cache.admits(key, ref, screen)) {
            return cipher.encrypts() ? openValue(ref, length) : readContent(ref, length);
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
        // A spare may be longer than the unit, and hold past the unit's content the bytes of an earlier read, of this
        // store or of another that shares the pool: the cache keeps only bytes of this unit
        Arrays.fill(plain, PageFormat.HEADER + length, plain.length, (byte) 0);
        // The cache holds it from now on, or drops it if it closed: never back to the spares, as readers may still
        // read it
        cache.put(key, ref, plain, true);
        return Segments.toBytes(plain, PageFormat.HEADER, length);
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
        return read(ref, cached, referenced, PageCache.UNSCREENED);
    }

    /// {@return the plaintext of the unit `ref` names, from the cache or the file}
    ///
    /// A unit of half a region of G1 or more that a screened read does not cache is not kept weakly for a
    /// [recall][PageCache#recall(long, PageRef, int)] either: G1 frees a humongous array no one reaches at its next young
    /// collection, and one reached weakly only after the next marking, which a store reading large values once may
    /// wait for with all of them committed.
    ///
    /// @param cached whether to keep the unit in the cache
    /// @param referenced whether a unit the cache takes counts as read, rather than as the first it evicts
    /// @param screen the screening of a unit read as read, which goes to the cache only if it
    ///               [admits][PageCache#admits(long, PageRef, int)] it: [PageCache#UNSCREENED], [PageCache#POINT] or
    ///               [PageCache#SCAN]
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    byte[] read(PageRef ref, boolean cached, boolean referenced, int screen) {
        var key = PageCache.key(PageCache.BLOBS, ref.page());
        var hit = cache.get(key, ref);
        if (hit != null) {
            return hit;
        }
        var screened = cached && referenced ? screen : PageCache.UNSCREENED;
        if (cached && referenced) {
            var recalled = cache.recall(key, ref, screen);
            if (recalled != null) {
                return recalled;
            }
        }
        var plain = readUnit(ref, cached, false);
        if (plain == null) {
            throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
        }
        if (screened != PageCache.UNSCREENED) {
            if (cache.admits(key, ref, screened)) {
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
    /// It keeps nothing: a unit of format [PageFormat#RECORDS] is read a record at a time into an array of the pool
    /// and opened into a second, and a unit of an earlier format above [HeapPool#MAX_POOLED] is read and opened into
    /// the pool's large spares, which go back at once.
    ///
    /// @throws LedgerIoException if the file cannot be read
    boolean authenticates(PageRef ref, long length) {
        // A unit of format RECORDS is longer than a record
        if (cipher.encrypts() && (long) ref.pages() * PageFormat.SIZE > PageFormat.RECORD) {
            var records = authenticatesRecords(ref, length);
            if (records >= 0) {
                return records == 1;
            }
        }
        var plain = readUnit(ref, false, true);
        if (plain == null) {
            return false;
        }
        var holds = Bytes.i32(plain, PageFormat.LENGTH) == length;
        pool.releaseLarge(plain);
        return holds;
    }

    /// {@return 1 if the file holds the unit of format [PageFormat#RECORDS] `ref` names whole and authenticated, with
    /// `length` bytes of content, 0 if it does not hold it so, and -1 if the header the file holds names another
    /// format, whose unit [#readUnit(PageRef, boolean, boolean)] reads}
    ///
    /// It reads the unit's header, then its records one at a time into an array of the pool, and opens each into a
    /// second, which keeps nothing: no array of the unit's size.
    ///
    /// @throws LedgerIoException if the file cannot be read
    private int authenticatesRecords(PageRef ref, long length) {
        var hook = readHookForTests;
        if (hook != null) {
            hook.run();
        }
        var position = PageFormat.offset(ref.page());
        var header = new byte[PageFormat.HEADER];
        var through = new FileChannel[]{channels.take()};
        var interrupted = new boolean[1];
        try {
            if (!readFully(through, header, 0, PageFormat.HEADER, position, interrupted) || !header(ref, header, 0)) {
                return 0;
            }
            if (header[PageFormat.FORMAT] != PageFormat.RECORDS) {
                return -1;
            }
            return Bytes.i32(header, PageFormat.LENGTH) == length && openRecords(through, header, position, null, 0, interrupted) ? 1 : 0;
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            channels.give(through[0]);
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Reads the records of the unit of format [PageFormat#RECORDS] whose header is `header`, at `position` of the
    /// file, one at a time into an array of the pool of [PageFormat#RECORD] bytes, one call of the channel each, and
    /// opens each into `plain` at `plainOffset` plus its offset in the content, or, if `plain` is `null`, into a
    /// second array of the pool, which keeps nothing. Both arrays go back at the end.
    ///
    /// @param header the unit's header, which [#header(PageRef, byte\[\], int)] accepted
    /// @param plain the array the content goes to, holding the content's bytes from `plainOffset`, or `null`
    /// @return whether the file holds every record and each authenticates; `plain` may then hold bytes of some
    /// @throws IOException if the file cannot be read
    private boolean openRecords(FileChannel[] through, byte[] header, long position, byte[] plain, int plainOffset,
                                boolean[] interrupted) throws IOException {
        var content = Bytes.i32(header, PageFormat.LENGTH);
        if (plain != null && (plainOffset < 0 || plain.length - plainOffset < content)) {
            throw new IllegalArgumentException("An array of " + plain.length + " bytes cannot hold " + content
                    + " bytes from " + plainOffset);
        }
        var records = PageFormat.records(content);
        var sealed = pool.acquire(PageFormat.RECORD);
        var scratch = plain == null ? pool.acquire(PageFormat.RECORD) : null;
        try (var opener = cipher.records()) {
            for (var record = 0; record < records; record++) {
                var length = PageFormat.recordLength(record, content);
                var into = plain == null ? scratch : plain;
                var at = plain == null ? 0 : plainOffset + PageFormat.recordContent(record);
                if (!readFully(through, sealed, 0, length + PageFormat.TAG, position + PageFormat.recordPosition(record), interrupted)
                        || !opener.open(header, record, sealed, 0, length, into, at)) {
                    return false;
                }
            }
            return true;
        } finally {
            pool.release(sealed);
            if (scratch != null) {
                pool.release(scratch);
            }
        }
    }

    /// Opens the records of the unit of format [PageFormat#RECORDS] sealed whole in `sealed`, whose header
    /// [#header(PageRef, byte\[\], int)] accepted, each into its place in `plain`, the array of the unit's plaintext,
    /// header included.
    ///
    /// @return whether every record authenticates; `plain` may then hold bytes of some
    private boolean openRecords(byte[] sealed, byte[] plain) {
        var content = Bytes.i32(sealed, PageFormat.LENGTH);
        if (plain.length - PageFormat.HEADER < content) {
            throw new IllegalArgumentException("An array of " + plain.length + " bytes cannot hold a unit of " + content
                    + " bytes of content");
        }
        var records = PageFormat.records(content);
        try (var opener = cipher.records()) {
            for (var record = 0; record < records; record++) {
                var length = PageFormat.recordLength(record, content);
                if (!opener.open(sealed, record, sealed, (int) PageFormat.recordPosition(record), length, plain,
                        PageFormat.HEADER + PageFormat.recordContent(record))) {
                    return false;
                }
            }
            return true;
        }
    }

    /// {@return the plaintext of the unit `ref` names, or `null` if the file does not hold it whole}
    ///
    /// In a store that encrypts, a unit longer than [PageFormat#RECORD], which may be of format [PageFormat#RECORDS],
    /// is read from its header on ([#readFromHeader(PageRef, boolean, boolean)]), so that one of that format takes no
    /// array of its size but the plaintext. Any other unit is read whole into an array of the pool, one of its large
    /// spares above [HeapPool#MAX_POOLED], which goes back at the end unless it is the array returned (`spare`), and
    /// opened out of it. Every array taken goes back if no channel can be taken. The channel reads at most [#BUFFER]
    /// bytes a call.
    ///
    /// A unit of format [PageFormat#CLOSE_TAG] opens its content alone, and one of format [PageFormat#RECORDS] its
    /// records, each into its place: an array of its own holds zeros past the content, as the plaintext of a unit of
    /// the first format does up to its tag.
    ///
    /// @param cached whether the unit is for the cache, and may end with its content
    /// @param spare whether a unit above [HeapPool#MAX_POOLED] kept whole comes back in an array of the pool's large
    ///              spares, which the caller gives back through [HeapPool#releaseLarge(byte\[\])] unless it hands it to
    ///              the cache: in a store that encrypts the unit is opened into it, in one without encryption the spare
    ///              it was read into is returned once its checksum matches; such an array may be longer than the unit,
    ///              and hold past the unit's content, after its tag or past the end of its pages, the bytes of another
    ///              read, which the caller zeroes before the cache takes it
    /// @throws LedgerIoException if the file cannot be read
    private byte[] readUnit(PageRef ref, boolean cached, boolean spare) {
        var hook = readHookForTests;
        if (hook != null) {
            hook.run();
        }
        var pages = ref.pages();
        var length = pages * PageFormat.SIZE;
        if (cipher.encrypts() && length > PageFormat.RECORD) {
            return readFromHeader(ref, cached, spare);
        }
        var large = length > HeapPool.MAX_POOLED;
        var sealed = large ? pool.acquireLarge(length) : pool.acquire(length);
        FileChannel taken;
        try {
            taken = channels.take();
        } catch (RuntimeException | Error failure) {
            if (large) {
                pool.releaseLarge(sealed);
            } else {
                pool.release(sealed);
            }
            throw failure;
        }
        var through = new FileChannel[]{taken};
        var interrupted = new boolean[1];
        // Whether sealed goes to the caller as the plaintext, so it does not go back to the pool
        var handed = false;
        try {
            if (!readFully(through, sealed, 0, length, PageFormat.offset(ref.page()), interrupted)) {
                return null;
            }
            // The header is the additional data of the tag and the open copies it as it is, so what it says holds once
            // the unit authenticates: it is checked first, and tells the bytes to keep
            if (!header(ref, sealed, 0)) {
                return null;
            }
            var content = Bytes.i32(sealed, PageFormat.LENGTH);
            var keep = cached ? PageFormat.cachedLength(pages, content) : length;
            var format = sealed[PageFormat.FORMAT];
            if (format == PageFormat.CLOSE_TAG || format == PageFormat.RECORDS) {
                // Out of place, so the open runs the AES-GCM intrinsic that decrypts and authenticates in one pass
                var plain = new byte[keep];
                System.arraycopy(sealed, 0, plain, 0, PageFormat.HEADER);
                var opened = format == PageFormat.RECORDS ? openRecords(sealed, plain)
                        : cipher.openContent(sealed, 0, content, plain, PageFormat.HEADER);
                return opened ? plain : null;
            }
            if (spare && large && keep == length) {
                // Without encryption the bytes read are the plaintext, once their checksum matches
                if (!PageCipher.checksumMatches(sealed, 0, length - PageFormat.TAG)) {
                    return null;
                }
                handed = true;
                return sealed;
            }
            return cipher.open(sealed, 0, pages, keep, pool);
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            channels.give(through[0]);
            if (!handed) {
                if (large) {
                    pool.releaseLarge(sealed);
                } else {
                    pool.release(sealed);
                }
            }
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// {@return the plaintext of the unit `ref` names, longer than [PageFormat#RECORD], in a store that encrypts, or
    /// `null` if the file does not hold it whole}
    ///
    /// The unit's header is read and checked first. A unit of format [PageFormat#RECORDS] is then read a record at a
    /// time into an array of the pool, each record opened straight into its place in the array returned, which is
    /// the only array of the unit's size the read takes. A unit of an earlier format is read into an array of the
    /// pool, one of its large spares above [HeapPool#MAX_POOLED], which goes back at the end, and opened out of it.
    ///
    /// @param cached whether the unit is for the cache, and may end with its content
    /// @param spare whether a unit above [HeapPool#MAX_POOLED] kept whole comes back in an array of the pool's large
    ///              spares, as [#readUnit(PageRef, boolean, boolean)] says
    /// @throws LedgerIoException if the file cannot be read
    private byte[] readFromHeader(PageRef ref, boolean cached, boolean spare) {
        var pages = ref.pages();
        var length = pages * PageFormat.SIZE;
        var large = length > HeapPool.MAX_POOLED;
        var position = PageFormat.offset(ref.page());
        var header = new byte[PageFormat.HEADER];
        var through = new FileChannel[]{channels.take()};
        var interrupted = new boolean[1];
        byte[] sealed = null;
        byte[] plain = null;
        var toSpare = false;
        // Whether plain goes to the caller, so a spare does not go back to the pool
        var handed = false;
        try {
            if (!readFully(through, header, 0, PageFormat.HEADER, position, interrupted) || !header(ref, header, 0)) {
                return null;
            }
            var content = Bytes.i32(header, PageFormat.LENGTH);
            var keep = cached ? PageFormat.cachedLength(pages, content) : length;
            toSpare = spare && large && keep == length;
            var format = header[PageFormat.FORMAT];
            if (format == PageFormat.RECORDS) {
                // A spare is left as it was past the content
                plain = toSpare ? pool.acquireLarge(length) : new byte[keep];
                System.arraycopy(header, 0, plain, 0, PageFormat.HEADER);
                handed = openRecords(through, header, position, plain, PageFormat.HEADER, interrupted);
                return handed ? plain : null;
            }
            var closeTag = format == PageFormat.CLOSE_TAG;
            sealed = large ? pool.acquireLarge(length) : pool.acquire(length);
            System.arraycopy(header, 0, sealed, 0, PageFormat.HEADER);
            if (!readFully(through, sealed, PageFormat.HEADER, closeTag ? content + PageFormat.TAG : length - PageFormat.HEADER,
                    position + PageFormat.HEADER, interrupted)) {
                return null;
            }
            if (!toSpare && !closeTag) {
                return cipher.open(sealed, 0, pages, keep, pool);
            }
            // Out of place, so the open runs the AES-GCM intrinsic that decrypts and authenticates in one pass; a spare
            // is left as it was past the content
            plain = toSpare ? pool.acquireLarge(length) : new byte[keep];
            System.arraycopy(header, 0, plain, 0, PageFormat.HEADER);
            handed = closeTag ? cipher.openContent(sealed, 0, content, plain, PageFormat.HEADER) : cipher.open(sealed, 0, pages, plain);
            return handed ? plain : null;
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            channels.give(through[0]);
            if (sealed != null) {
                if (large) {
                    pool.releaseLarge(sealed);
                } else {
                    pool.release(sealed);
                }
            }
            if (plain != null && toSpare && !handed) {
                pool.releaseLarge(plain);
            }
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// {@return the value of `length` bytes the unit `ref` names, above [HeapPool#MAX_POOLED], in a new array, read from
    /// the file of a store that encrypts}
    ///
    /// The unit's header is read and checked first. A unit of format [PageFormat#RECORDS] is then read a record at a
    /// time into an array of the pool, each record opened straight into its place in the array returned: the read takes
    /// no array of the unit's size but the value, and returns it only once its last record authenticated. A unit of
    /// format [PageFormat#CLOSE_TAG] is read up to its tag into one of the pool's large spares, which goes back at the
    /// end, and its content opened straight into the array returned: the read takes one spare and copies nothing. A unit
    /// of the first format, whose tag ends its pages, is read whole and opened into a second spare, out of which the
    /// value is copied; so is one of format [PageFormat#CLOSE_TAG] whose header names another length. A unit whose
    /// header names another length is reported once it authenticates, the records of one of format
    /// [PageFormat#RECORDS] opened into an array of the pool.
    ///
    /// @throws LedgerCorruptedException if the unit fails authentication, is not the one `ref` names, or does not hold
    ///                                  `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    private byte[] openValue(PageRef ref, int length) {
        var hook = readHookForTests;
        if (hook != null) {
            hook.run();
        }
        var pages = ref.pages();
        var unitBytes = pages * PageFormat.SIZE;
        var position = PageFormat.offset(ref.page());
        var header = new byte[PageFormat.HEADER];
        var through = new FileChannel[]{channels.take()};
        var interrupted = new boolean[1];
        byte[] sealed = null;
        try {
            if (!readFully(through, header, 0, PageFormat.HEADER, position, interrupted) || !header(ref, header, 0)) {
                throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
            }
            // The header is the additional data of the tags, so what it says holds once the unit authenticates
            var content = Bytes.i32(header, PageFormat.LENGTH);
            var format = header[PageFormat.FORMAT];
            if (format == PageFormat.RECORDS) {
                var value = content == length ? new byte[length] : null;
                if (!openRecords(through, header, position, value, 0, interrupted)) {
                    throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
                }
                if (value == null) {
                    throw new LedgerCorruptedException(path + " holds " + content + " bytes at page " + ref.page()
                            + " for a value of " + length, null);
                }
                return value;
            }
            sealed = pool.acquireLarge(unitBytes);
            System.arraycopy(header, 0, sealed, 0, PageFormat.HEADER);
            var closeTag = format == PageFormat.CLOSE_TAG;
            if (closeTag && content == length) {
                var value = new byte[length];
                if (!readFully(through, sealed, PageFormat.HEADER, length + PageFormat.TAG, position + PageFormat.HEADER, interrupted)
                        || !cipher.openContent(sealed, 0, length, value, 0)) {
                    throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
                }
                return value;
            }
            var rest = closeTag ? content + PageFormat.TAG : unitBytes - PageFormat.HEADER;
            if (!readFully(through, sealed, PageFormat.HEADER, rest, position + PageFormat.HEADER, interrupted)) {
                throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
            }
            // Out of place, so the open runs the AES-GCM intrinsic that decrypts and authenticates in one pass
            var plain = pool.acquireLarge(unitBytes);
            try {
                if (!(closeTag ? cipher.openContent(sealed, 0, content, plain, PageFormat.HEADER) : cipher.open(sealed, 0, pages, plain))) {
                    throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
                }
                if (content != length) {
                    throw new LedgerCorruptedException(path + " holds " + content + " bytes at page " + ref.page()
                            + " for a value of " + length, null);
                }
                return Segments.toBytes(plain, PageFormat.HEADER, length);
            } finally {
                pool.releaseLarge(plain);
            }
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            channels.give(through[0]);
            if (sealed != null) {
                pool.releaseLarge(sealed);
            }
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// {@return the value of `length` bytes the unit `ref` names, in a new array, read from the file of a store
    /// without encryption}
    ///
    /// The content goes from the file straight into the array returned, and the unit's header and the bytes past its
    /// content into an array of the pool, which goes back at the end: the read allocates the caller's array alone and
    /// copies each byte once, where reading the whole unit into one array, as an open does, copies the content out of
    /// it again. The header is checked before the content is read, the checksum over the three pieces after.
    ///
    /// @throws LedgerCorruptedException if the unit fails its checksum, is not the one `ref` names, or does not hold
    ///                                  `length` bytes
    /// @throws LedgerIoException if the file cannot be read
    private byte[] readContent(PageRef ref, int length) {
        var hook = readHookForTests;
        if (hook != null) {
            hook.run();
        }
        var pages = ref.pages();
        var position = PageFormat.offset(ref.page());
        // The header at 0, then the bytes after the content: its padding and the checksum
        var rest = pool.acquire(PageFormat.HEADER + PageFormat.SIZE + PageFormat.TAG);
        FileChannel taken;
        try {
            taken = channels.take();
        } catch (RuntimeException | Error failure) {
            pool.release(rest);
            throw failure;
        }
        var through = new FileChannel[]{taken};
        var interrupted = new boolean[1];
        try {
            if (!readFully(through, rest, 0, PageFormat.HEADER, position, interrupted)) {
                throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
            }
            var content = Bytes.i32(rest, PageFormat.LENGTH);
            if (!header(ref, rest, 0)) {
                throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
            }
            if (content != length) {
                throw new LedgerCorruptedException(path + " holds " + content + " bytes at page " + ref.page()
                        + " for a value of " + length, null);
            }
            var after = pages * PageFormat.SIZE - PageFormat.HEADER - length;
            if (PageFormat.HEADER + after > rest.length) {
                // A unit of more pages than its content needs
                var longer = pool.acquire(PageFormat.HEADER + after);
                System.arraycopy(rest, 0, longer, 0, PageFormat.HEADER);
                pool.release(rest);
                rest = longer;
            }
            var value = new byte[length];
            if (!readFully(through, value, 0, length, position + PageFormat.HEADER, interrupted)
                    || !readFully(through, rest, PageFormat.HEADER, after, position + PageFormat.HEADER + length, interrupted)
                    || !PageCipher.checksumMatches(value, length, rest, after - PageFormat.TAG)) {
                throw new LedgerCorruptedException(path + " fails authentication at page " + ref.page(), null);
            }
            return value;
        } catch (IOException exception) {
            throw new LedgerIoException(path + " could not be read", exception);
        } finally {
            channels.give(through[0]);
            pool.release(rest);
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Reads the `length` bytes of the file at `position` into `array` at `offset`, at most [#BUFFER] bytes a call,
    /// through the channel of `through`, which a new one replaces if an interrupt closes it, setting `interrupted`.
    ///
    /// @return whether the file holds them all
    private boolean readFully(FileChannel[] through, byte[] array, int offset, int length, long position, boolean[] interrupted)
            throws IOException {
        var target = ByteBuffer.wrap(array, offset, length);
        var end = offset + length;
        while (target.position() < end) {
            target.limit(Math.min(end, target.position() + BUFFER));
            try {
                if (through[0].read(target, position + target.position() - offset) < 0) {
                    return false;
                }
            } catch (ClosedByInterruptException _) {
                // The interrupt closed the channel; read on a new one, from where the last read ended, and give the
                // caller its interrupt back
                interrupted[0] |= Thread.interrupted();
                through[0] = channels.reopen();
            }
        }
        return true;
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
    /// still read it, as memory keeps no copy of a paged value ([#copyValue(PageRef, int, int)]).
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

    /// Makes a store that encrypts write the units that follow in `format` whatever their length, as earlier builds
    /// wrote them: the first format, whose tag ends the unit's pages, or [PageFormat#CLOSE_TAG]; or, if `format` is 0,
    /// in the format their length takes again; for tests of files that hold several.
    void formatForTests(byte format) {
        if (format != 0 && format != PageFormat.VERSION && format != PageFormat.CLOSE_TAG && format != PageFormat.RECORDS) {
            throw new IllegalArgumentException("No unit of large values is of format " + format);
        }
        this.formatForTests = format;
    }

    /// Sets the counter the next unit's nonce takes to `count`; for tests of the units one open may seal.
    void counterForTests(long count) {
        synchronized (lock) {
            counter.set(count);
        }
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

    /// {@return the channels callers write and read through, held or idle, for tests}
    int channelsForTests() {
        return channels.openedForTests();
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException _) {
            // Every unit a commit relies on was forced with its log group
        }
        channels.close();
    }
}

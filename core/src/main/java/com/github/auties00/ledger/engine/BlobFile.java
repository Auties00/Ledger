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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// The file of large values, `ledger.blobs`: values a commit writes once, in units of their own, instead of into its
/// log frame and again at the next checkpoint.
///
/// A commit seals each of its large values into free pages before it is given a sequence, the values of a commit that
/// holds many too large for a leaf together, mostly into one run of them ([#writeAll(List)]), and its log frame records
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
    /// The trailing free pages, 64 MiB of them, below which [#trim()] keeps the file's length.
    private static final int TRIM_SLACK = 16_384;
    /// The channels the threads of reads read units through, as many as the data file has: positional reads of one
    /// channel wait for each other on Windows.
    private static final int READ_CHANNELS = 8;
    /// The channels the threads of commits write units through, apart from those of the reads, so a commit that writes
    /// megabytes holds no reader back.
    private static final int WRITE_CHANNELS = 4;
    /// The most threads that seal and write the units of one [#writeAll(List)].
    private static final int WRITERS = 4;
    /// The bytes of units of a [#writeAll(List)] for each thread that seals and writes them: below it, a thread of its
    /// own would cost more than it takes off the commit.
    private static final long WRITER_BYTES = 512 << 10;
    /// The name of the threads that seal and write a share of the units of a [#writeAll(List)].
    static final String WRITER_NAME = "ledger-blob-writer";

    private final Path path;
    // Forces and cuts the file, on the store's own threads only, so no interrupt of a caller closes it under them
    private final FileChannel channel;
    private final Channels readers;
    private final Channels writers;
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
    // The commits that write units and have not ended; guarded by lock
    private int commits;
    // Whether the store is closing, so no commit starts writing units; guarded by lock
    private boolean closing;

    private BlobFile(Path path, FileChannel channel, PageCipher cipher, PageCache cache, HeapPool pool, FreeSpace space) {
        this.path = path;
        this.channel = channel;
        this.readers = new Channels(path, READ_CHANNELS);
        this.writers = new Channels(path, WRITE_CHANNELS);
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

    /// Records that a commit is about to write units, which [#awaitCommits()] waits for until [#exitCommit()].
    ///
    /// What a commit still writes once its store has closed lands in pages the store's last checkpoint lists as free,
    /// which the next open gives to values it acknowledges: so no commit writes a unit outside this.
    ///
    /// @throws IllegalStateException if the store is closing
    void enterCommit() {
        synchronized (lock) {
            if (closing) {
                throw new IllegalStateException("The store is closed");
            }
            commits++;
        }
    }

    /// Records that a commit [#enterCommit()] recorded has ended, whether it succeeded or not.
    void exitCommit() {
        synchronized (lock) {
            if (--commits == 0 && closing) {
                lock.notifyAll();
            }
        }
    }

    /// Stops commits from starting to write units, and waits for those that have started to end.
    ///
    /// They end on their own, so this is called before the store closes anything, while the log still takes their
    /// frames. It waits through interrupts, which the thread gets back.
    void awaitCommits() {
        var interrupted = false;
        synchronized (lock) {
            closing = true;
            while (commits > 0) {
                try {
                    lock.wait();
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Seals `value` into free pages and writes it, without forcing it.
    ///
    /// The unit is in flight until a checkpoint takes it or [#release(PageRef)] frees it.
    ///
    /// @return where the unit is
    /// @throws LedgerIoException if the file cannot be written; the pages are free again
    PageRef write(MemorySegment value) {
        passEpochGate();
        var pages = PageFormat.pagesFor(value.byteSize());
        PageRef ref;
        synchronized (lock) {
            ref = new PageRef(space.allocate(pages), pages, counter.getAndIncrement(), epoch);
            unadopted.add(ref);
        }
        var buffer = pool.acquire(BUFFER);
        var interrupted = new boolean[1];
        try {
            stream(value, ref, new byte[PageFormat.HEADER], buffer, interrupted);
        } catch (IOException exception) {
            release(ref);
            throw new LedgerIoException(path + " could not be written", exception);
        } catch (RuntimeException | Error throwable) {
            release(ref);
            throw throwable;
        } finally {
            pool.release(buffer);
            if (interrupted[0]) {
                Thread.currentThread().interrupt();
            }
        }
        return ref;
    }

    /// Seals each of `values` into a unit of its own and writes them, without forcing them.
    ///
    /// The units take one run of free pages where one holds them all, in the order of `values`, and are sealed back to
    /// back into buffers of a megabyte, each written with one call: a batch of thousands of values of a few kilobytes
    /// costs a system call a megabyte rather than one a unit, and more only for the shorter runs it fills where no run
    /// holds it all ([FreeSpace#allocateAll(int\[\], int)]). From twice [#WRITER_BYTES] of units, up to [#WRITERS]
    /// threads seal and write them, the calling thread among them, each a share of whole units, so no unit's seal is
    /// split between threads. The call returns once every thread is done, failed or not, so no write of a failed call
    /// lands in the pages it gave back, which another unit may take at once.
    ///
    /// The units are in flight until a checkpoint takes them or [#release(PageRef)] frees them.
    ///
    /// @return where each unit is, in the order of `values`
    /// @throws LedgerIoException if the file cannot be written; the pages of every unit are free again
    PageRef[] writeAll(List<MemorySegment> values) {
        if (values.isEmpty()) {
            return new PageRef[0];
        }
        passEpochGate();
        var refs = new PageRef[values.size()];
        var sizes = new int[refs.length];
        var total = 0L;
        for (var i = 0; i < sizes.length; i++) {
            sizes[i] = PageFormat.pagesFor(values.get(i).byteSize());
            total += sizes[i];
        }
        synchronized (lock) {
            var firsts = space.allocateAll(sizes, Math.toIntExact(total));
            for (var i = 0; i < refs.length; i++) {
                refs[i] = new PageRef(firsts[i], sizes[i], counter.getAndIncrement(), epoch);
                unadopted.add(refs[i]);
            }
        }
        var bounds = shares(refs, total);
        var failures = new Throwable[bounds.length - 1];
        var threads = new Thread[failures.length];
        for (var share = 1; share < threads.length; share++) {
            var from = bounds[share];
            var to = bounds[share + 1];
            var index = share;
            try {
                threads[share] = Thread.ofVirtual().name(WRITER_NAME).start(() -> {
                    try {
                        // No one else holds this thread, so nothing interrupts it
                        writeShare(values, refs, from, to, new boolean[1]);
                    } catch (Throwable failure) {
                        failures[index] = failure;
                    }
                });
            } catch (Throwable failure) {
                failures[share] = failure;
            }
        }
        var interrupted = new boolean[1];
        try {
            writeShare(values, refs, bounds[0], bounds[1], interrupted);
        } catch (Throwable failure) {
            failures[0] = failure;
        }
        for (var thread : threads) {
            if (thread != null) {
                join(thread, interrupted);
            }
        }
        if (interrupted[0]) {
            Thread.currentThread().interrupt();
        }
        Throwable failure = null;
        for (var each : failures) {
            if (each == null) {
                continue;
            }
            if (failure == null) {
                failure = each;
            } else {
                failure.addSuppressed(each);
            }
        }
        if (failure != null) {
            for (var ref : refs) {
                release(ref);
            }
            switch (failure) {
                case IOException exception -> throw new LedgerIoException(path + " could not be written", exception);
                case RuntimeException exception -> throw exception;
                case Error error -> throw error;
                default -> throw new IllegalStateException("A unit could not be written", failure);
            }
        }
        return refs;
    }

    /// {@return the bounds of the shares of `refs` the threads of a [#writeAll(List)] seal and write, the first one's
    /// start then each one's end: whole units, about as many pages in each, one share for each [#WRITER_BYTES]}
    ///
    /// @param pages the pages of every unit of `refs`
    private static int[] shares(PageRef[] refs, long pages) {
        var shares = (int) Math.clamp(pages * PageFormat.SIZE / WRITER_BYTES, 1, Math.min(WRITERS, refs.length));
        var bounds = new int[shares + 1];
        var cuts = 1;
        var before = 0L;
        for (var i = 0; i < refs.length - 1 && cuts < shares; i++) {
            before += refs[i].pages();
            if (before * shares >= pages * cuts) {
                bounds[cuts++] = i + 1;
            }
        }
        bounds[cuts] = refs.length;
        return cuts == shares ? bounds : Arrays.copyOf(bounds, cuts + 1);
    }

    /// Seals the units `from` to `to` of `refs`, of the values at the same indexes of `values`, and writes them: back
    /// to back into a buffer, written whenever the next unit does not fit or does not follow the last in the file; a
    /// unit larger than the buffer streams through it.
    ///
    /// @param interrupted set if an interrupt of the thread closed a channel it wrote through
    private void writeShare(List<MemorySegment> values, PageRef[] refs, int from, int to, boolean[] interrupted) throws IOException {
        var bytes = 0L;
        for (var i = from; i < to; i++) {
            bytes += (long) refs[i].pages() * PageFormat.SIZE;
        }
        // A share smaller than a buffer takes no more than it needs, which every unit of the share then fits
        var buffer = pool.acquire((int) Math.min(BUFFER, bytes));
        var header = new byte[PageFormat.HEADER];
        try {
            var used = 0;
            var start = 0L;
            for (var i = from; i < to; i++) {
                var ref = refs[i];
                var value = values.get(i);
                var length = (long) ref.pages() * PageFormat.SIZE;
                var position = PageFormat.offset(ref.page());
                if (length > buffer.length) {
                    writeAt(buffer, used, start, interrupted);
                    used = 0;
                    stream(value, ref, header, buffer, interrupted);
                    continue;
                }
                if (used + length > buffer.length || position != start + used) {
                    writeAt(buffer, used, start, interrupted);
                    used = 0;
                }
                if (used == 0) {
                    start = position;
                }
                header(header, ref, value);
                cipher.seal(header, value, ref.pages(), buffer, used);
                used += (int) length;
            }
            writeAt(buffer, used, start, interrupted);
        } finally {
            pool.release(buffer);
        }
    }

    /// Seals `value` into the unit `ref` through `buffer`, of at least 64 KiB, writing each part where it belongs.
    ///
    /// @param header receives the unit's header
    /// @param interrupted set if an interrupt of the thread closed a channel it wrote through
    private void stream(MemorySegment value, PageRef ref, byte[] header, byte[] buffer, boolean[] interrupted) throws IOException {
        header(header, ref, value);
        var position = new long[]{PageFormat.offset(ref.page())};
        cipher.seal(header, value, ref.pages(), buffer, (bytes, length) -> {
            writeAt(bytes, length, position[0], interrupted);
            position[0] += length;
        });
    }

    /// Fills `header` with the header of the unit `ref`, which holds `value`.
    private static void header(byte[] header, PageRef ref, MemorySegment value) {
        PageFile.header(header, PageFormat.EXTERNAL, 0, ref.page(), ref.pages(), Math.toIntExact(value.byteSize()),
                ref.nonceHigh(), ref.nonceLow());
    }

    /// Writes the first `length` bytes of `bytes` at `position`, through a channel of the commits.
    ///
    /// An interrupt that closes the channel meanwhile has the same bytes written again at the same offset, through a
    /// new one, never sealed again: their nonce is taken.
    ///
    /// @param interrupted set if an interrupt of the thread closed the channel, for the caller to give it back
    private void writeAt(byte[] bytes, int length, long position, boolean[] interrupted) throws IOException {
        if (length == 0) {
            return;
        }
        var through = writers.take();
        try {
            while (true) {
                try {
                    var source = ByteBuffer.wrap(bytes, 0, length);
                    var at = position;
                    while (source.hasRemaining()) {
                        at += through.write(source, at);
                    }
                    return;
                } catch (ClosedByInterruptException _) {
                    interrupted[0] |= Thread.interrupted();
                    through = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                }
            }
        } finally {
            writers.offer(through);
        }
    }

    /// Makes the epoch durable before the first unit of this open, then records that units take it: no unit is sealed
    /// under an epoch a later open may reuse.
    private void passEpochGate() {
        var gate = epochGate;
        if (gate != null) {
            gate.run();
            epochGate = null;
        }
        epochTaken = true;
    }

    /// Waits for `thread` to end, through interrupts, which `interrupted` records for the caller to give back.
    private static void join(Thread thread, boolean[] interrupted) {
        while (true) {
            try {
                thread.join();
                return;
            } catch (InterruptedException _) {
                interrupted[0] = true;
            }
        }
    }

    /// {@return the plaintext of the unit `ref` names, header included}
    ///
    /// @param cached whether to keep the unit in the page cache
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    /// @throws LedgerIoException if the file cannot be read
    /// @throws IllegalStateException if the store has closed, as a read of the data file then does
    byte[] read(PageRef ref, boolean cached) {
        return read(ref, cached, true);
    }

    /// {@return the plaintext of the unit `ref` names, from the cache or the file}
    ///
    /// @param cached whether to keep the unit in the cache
    /// @param referenced whether a unit the cache takes counts as read, rather than as the first it evicts
    /// @throws LedgerCorruptedException if the unit fails authentication or is not the one `ref` names
    byte[] read(PageRef ref, boolean cached, boolean referenced) {
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
            cache.put(key, ref, plain, referenced);
        }
        return plain;
    }

    /// {@return the plaintext of the unit `ref` names, or `null` if the file does not hold it whole}
    ///
    /// @throws LedgerIoException if the file cannot be read
    /// @throws IllegalStateException if the store has closed
    byte[] readUnit(PageRef ref) {
        var length = ref.pages() * PageFormat.SIZE;
        var sealed = pool.acquire(length);
        FileChannel through = null;
        var interrupted = false;
        try {
            through = readers.take();
            var target = ByteBuffer.wrap(sealed, 0, length);
            var position = PageFormat.offset(ref.page());
            while (target.hasRemaining()) {
                try {
                    if (through.read(target, position + target.position()) < 0) {
                        return null;
                    }
                } catch (ClosedByInterruptException _) {
                    // The interrupt closed the channel; read on a new one and give the caller its interrupt back
                    interrupted |= Thread.interrupted();
                    through = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
            if (through != null) {
                readers.offer(through);
            }
            pool.release(sealed);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Forces every unit written so far, and the length of the file.
    void force() throws IOException {
        channel.force(true);
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

    /// {@return the units no published checkpoint points to, for tests}
    int unadoptedForTests() {
        synchronized (lock) {
            return unadopted.size();
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException _) {
            // Every unit a commit relies on was forced with its log group
        }
        readers.close();
        writers.close();
    }

    /// Channels of the file, each used by one thread at a time, opened as threads need them, up to a limit; one an
    /// interrupt closed is replaced when it is next taken. Once closed they refuse new uses, and a channel a thread
    /// was using closes when it comes back.
    private static final class Channels {
        private static final long RETRY_MILLIS = 50;

        private final Path path;
        private final int limit;
        private final ArrayBlockingQueue<FileChannel> idle;
        // The channels opened, idle or in use, at most limit
        private final AtomicInteger opened;
        private volatile boolean closed;

        Channels(Path path, int limit) {
            this.path = path;
            this.limit = limit;
            this.idle = new ArrayBlockingQueue<>(limit);
            this.opened = new AtomicInteger();
        }

        /// {@return an open channel, waiting through interrupts, which the thread gets back, while `limit` are in use}
        ///
        /// @throws LedgerIoException if a channel cannot be opened
        /// @throws IllegalStateException if the channels are closed
        FileChannel take() {
            var interrupted = false;
            try {
                while (true) {
                    if (closed) {
                        throw new IllegalStateException("The store is closed");
                    }
                    var taken = idle.poll();
                    if (taken == null) {
                        if (opened.getAndUpdate(count -> count < limit ? count + 1 : count) < limit) {
                            return open(null);
                        }
                        try {
                            // Timed: a thread that fails to open its channel gives nothing back for this one to take
                            taken = idle.poll(RETRY_MILLIS, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException _) {
                            interrupted = true;
                        }
                    }
                    if (taken != null) {
                        // An interrupt may have closed it after its last use, and it could not be replaced then
                        return taken.isOpen() ? taken : open(taken);
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        /// Gives back a channel [#take()] returned, even one an interrupt closed.
        void offer(FileChannel channel) {
            idle.offer(channel);
            // Read after the offer: a close that came before it, and so may not have seen this channel, is seen here
            if (closed) {
                closeIdle();
            }
        }

        /// {@return a new channel, in place of `closed`, or of none}
        private FileChannel open(FileChannel closed) {
            try {
                return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException exception) {
                if (closed != null) {
                    idle.offer(closed);
                } else {
                    opened.decrementAndGet();
                }
                throw new LedgerIoException(path + " could not be opened", exception);
            }
        }

        /// Closes the channels no thread uses, and those threads use as they come back.
        void close() {
            closed = true;
            closeIdle();
        }

        /// Closes the channels no thread uses now.
        private void closeIdle() {
            FileChannel open;
            while ((open = idle.poll()) != null) {
                try {
                    open.close();
                } catch (IOException _) {
                    // Nothing was left unforced through a caller's channel: forces go through the file's own
                }
            }
        }
    }
}

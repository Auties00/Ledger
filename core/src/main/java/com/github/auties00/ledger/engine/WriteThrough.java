package com.github.auties00.ledger.engine;

import com.sun.nio.file.ExtendedOpenOption;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.LongFunction;

/// The writes of the log's groups through to stable storage, so a group that one write holds needs no force after it,
/// used by default only where the write holds what the force would.
///
/// A write and then a force are two round trips to the device and two wake-ups of the writer, and on Windows the
/// force empties the volume's cache as well; a write through is one, which a device that supports forced unit access
/// serves without emptying its cache, so it does not wait behind the pages of a checkpoint there either. Each segment
/// is opened a second time for it:
///
/// - on Linux with `O_DIRECT` and `O_DSYNC`: the kernel writes a block that changes no metadata as one write with
///   forced unit access where the device supports it, and otherwise as the write and the flush of the device's cache
///   that a force is, which is the guarantee of `fdatasync`. Where the file system refuses `O_DIRECT` (blocks larger
///   than [LogWriter#ALIGNMENT], tmpfs before Linux 6.6, some FUSE file systems), with `O_DSYNC` alone: the same
///   guarantee in one call.
/// - on Windows with `FILE_FLAG_NO_BUFFERING` and `FILE_FLAG_WRITE_THROUGH`, through the Windows API, only while the
///   module has native access: for that pair Windows documents that it also requests a write-through of the drive's
///   cache, and for `FILE_FLAG_WRITE_THROUGH` alone it does not. It adds that not all hardware supports the request,
///   and a force asks the device to empty its cache instead, so [Mode#AUTO] takes this way only on a device that
///   reports the request holds ([WriteCache#provesWriteThrough()]), and forces on any other. The JDK opens a channel
///   with [StandardOpenOption#DSYNC] with `FILE_FLAG_WRITE_THROUGH` alone, and `ExtendedOpenOption.DIRECT` leaves the
///   handle the channel writes through buffered.
/// - on macOS never: `O_DSYNC` leaves the data in the drive's cache, which only the `fcntl(F_FULLFSYNC)` of a force
///   empties.
///
/// On a file system other than the default one a group is written through only when [Mode#DSYNC] or [Mode#DIRECT]
/// asks for it, through a channel opened with [StandardOpenOption#DSYNC]: a provider that ignored the option would
/// leave the group in a cache, so the store does not take the path on a provider it knows nothing of.
///
/// The system property `ledger.log.writeThrough` sets the [Mode] of every store the process opens: `auto`, the
/// default, `off`, `dsync` or `direct`.
///
/// A way that fails moves the writer to the next one down, `O_DSYNC` after `O_DIRECT`, and from the last to forces; a
/// buffer or a handle the native memory has no room for is such a failure, since a force needs neither. The first group
/// written through a way is what tries it, with no durable write of its own first: a group it refuses is written again.
/// Only the flush that holds the log writer writes through an instance, so one thread at a time; its buffer, aligned to
/// [LogWriter#ALIGNMENT] as `O_DIRECT` and `FILE_FLAG_NO_BUFFERING` need, serves every segment of the writer. The thread
/// preparing a segment may ask which way is left beside it, so the round trip of asking the device holds no commit up.
///
/// A group of several blocks that commits wait for may be written as up to [#MAX_CHUNKS] chunks of whole blocks at
/// once, which a device with more than one request in flight serves in about the time of one, where one write of
/// the group takes the transfer of every block in turn; the write returns, durable, once every chunk has. How the
/// chunks are made durable follows the device ([Chunks]):
///
/// - on Windows each chunk is an overlapped write of the unbuffered handle, issued by the flush and waited for on
///   an event of its own, so a completion of one chunk is never taken for another's;
/// - on Linux each chunk is a positional write of the segment, the first on the flush's thread and each other on a
///   platform thread of the writer's ([#MAX_CHUNKS] less one, started by the first group that needs them, ended by
///   [#release()] or after [#IDLE_NANOS] without a chunk). Where the device writes with forced unit access, or has no
///   volatile cache, the chunks go through the segment's `O_DIRECT` and `O_DSYNC` channel, each durable as it returns;
///   elsewhere `O_DSYNC` would empty the device's cache once a chunk, and the device empties it one request at a
///   time, so the chunks go through a channel opened with `O_DIRECT` alone and one force follows them all.
///
/// A loss of power during the chunks leaves any of the group's blocks written, as one write of several blocks may
/// already do: a device keeps a write whole only within a block. The group's first frame, which alone marks it as a
/// group's, and the frames after it, are only taken as a prefix at recovery, so a later chunk on disk without an
/// earlier one is a torn group, which nobody was told was durable.
///
/// The system property `ledger.log.chunks` sets the [Chunks] of every store the process opens: `auto`, the default,
/// `off`, `dsync` or `direct`.
final class WriteThrough {
    private static final String OS = System.getProperty("os.name", "");
    /// The most chunks a group is written in: four requests in flight are what devices queue cheaply, and more would
    /// leave a chunk of about one block in the groups of a few dozen commits.
    static final int MAX_CHUNKS = 4;
    /// The largest group written in chunks: past it a group is bound by the device's bandwidth rather than the time of
    /// a request, and the kernel splits a write that large itself.
    static final int MAX_CHUNKED_BYTES = 128 << 10;
    /// How long a thread writing chunks waits for one before it ends, in nanoseconds; the next chunked group starts it
    /// again.
    static final long IDLE_NANOS = 5_000_000_000L;
    /// How long a thread writing chunks looks for its next chunk before it waits for it parked, in nanoseconds, after
    /// a chunk and after a group about to be written woke it, beyond the time that group asked for: about the time the
    /// frames of a group of a few blocks take to seal, and the time a flush takes from one group to the next.
    private static final long CHUNK_SPIN = 20_000;
    /// The longest the flush looks for the chunks of the other threads to return before it waits for them parked, in
    /// nanoseconds; below it, the time its own chunk took and [#CHUNK_SPIN] more, as the chunks were issued together
    /// and a device serving them in parallel ends them about together.
    private static final long MAX_JOIN_SPIN = 250_000;
    // The handles whose overlapped writes a failed call left with no known end, kept from the collector with the memory
    // those writes may still complete into
    private static final Set<Object> UNSETTLED = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    /// How a store writes a group of several blocks that commits wait for.
    enum Chunks {
        /// In chunks where the way groups are written through serves them in parallel: on Windows through the unbuffered
        /// handle; on Linux through `O_DIRECT` with each chunk durable on its own on a device that writes with forced
        /// unit access or has no volatile cache, and otherwise with one force after them. A segment opened with
        /// `O_DSYNC` alone writes in one write, as its chunks would each empty the device's cache.
        AUTO,
        /// In one write.
        OFF,
        /// In chunks, each durable on its own, whatever the device.
        DSYNC,
        /// In chunks followed by one force, through a channel without `O_DSYNC`; on Windows as [#DSYNC].
        DIRECT;

        /// {@return the chunks the system property `ledger.log.chunks` names, [#AUTO] if it names none}
        ///
        /// @throws IllegalArgumentException if it names no chunks
        static Chunks fromProperty() {
            var value = System.getProperty("ledger.log.chunks");
            if (value == null || value.isBlank()) {
                return AUTO;
            }
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("ledger.log.chunks must be auto, off, dsync or direct: " + value, exception);
            }
        }
    }

    /// How a handle makes the chunks of a group durable.
    enum Chunking {
        /// It writes a group in one write.
        NONE,
        /// Each chunk is durable once its write returns.
        THROUGH,
        /// The chunks are written past the caches and then forced together.
        FORCED
    }

    /// How a store writes the groups that one write holds.
    enum Mode {
        /// Through, on the default file system, where the write holds what a force holds: `O_DIRECT` and `O_DSYNC` on
        /// Linux; unbuffered write-through on Windows while the module has native access, on a device that reports the
        /// request holds. Every other group, and every group elsewhere, is forced.
        AUTO,
        /// Every group is forced.
        OFF,
        /// Through a channel opened with [StandardOpenOption#DSYNC], on any file system and any platform but macOS. On
        /// Windows this is `FILE_FLAG_WRITE_THROUGH` alone, which Windows documents as writing through its own cache
        /// and not the drive's, so a loss of power can take a durable commit from a device with a volatile cache.
        DSYNC,
        /// As [#AUTO] on the default file system, but on Windows without asking the device, so a loss of power can take
        /// a durable commit from a device with a volatile cache that ignores the request; on any other platform but
        /// macOS with `O_DIRECT` and `O_DSYNC`; on another file system as [#DSYNC].
        DIRECT;

        /// {@return the mode the system property `ledger.log.writeThrough` names, [#AUTO] if it names none}
        ///
        /// @throws IllegalArgumentException if it names no mode
        static Mode fromProperty() {
            var value = System.getProperty("ledger.log.writeThrough");
            if (value == null || value.isBlank()) {
                return AUTO;
            }
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("ledger.log.writeThrough must be auto, off, dsync or direct: " + value, exception);
            }
        }
    }

    /// The ways to write through, from the one tried first.
    enum Kind {
        /// `FILE_FLAG_NO_BUFFERING` and `FILE_FLAG_WRITE_THROUGH` through the Windows API.
        UNBUFFERED,
        /// A channel opened with [StandardOpenOption#DSYNC] and `ExtendedOpenOption.DIRECT`.
        DIRECT,
        /// A channel opened with [StandardOpenOption#DSYNC].
        DSYNC
    }

    private final Object lock;
    // The way segments are opened in now, or null once groups are forced; guarded by the lock
    private Kind kind;
    // What must answer before the first segment is opened through, or null once it has or if nothing need; guarded by
    // the lock
    private BooleanSupplier proof;
    // How groups of several blocks are written
    private final Chunks chunks;
    // Whether the device under the segments writes a write through with no flush of a volatile cache, once asked;
    // guarded by the lock
    private Boolean deviceWritesThrough;
    // The buffer a group is copied into before it is written, aligned to LogWriter.ALIGNMENT, and a view of it
    private MemorySegment buffer;
    private ByteBuffer bufferView;
    // The views of the buffer the chunks of a group are written from, one a chunk, made with the buffer
    private ByteBuffer[] chunkViews;
    // The threads that write the chunks past the first, or null until a group needs them
    private ChunkWriters writers;
    // The handles opened, for tests; written by the flush that holds the log writer
    private volatile int opens;
    /// Allocates the buffer in place of the arena, by its size; for tests that refuse it.
    LongFunction<MemorySegment> allocatorForTests;
    /// Whether every group of several blocks written through is written in chunks, those of a commit writing its own
    /// group and those no commit waits for included; for tests, whose commits write their own groups.
    volatile boolean chunkEveryGroupForTests;

    /// Creates the way to write through that starts at `kind`, or forces if it is `null`, and writes groups of several
    /// blocks as `ledger.log.chunks` says.
    ///
    /// @param proof what must answer `true` before the first segment is opened through, asked once, on the thread of
    ///              the flush that needs it; or `null` if nothing need
    WriteThrough(Kind kind, BooleanSupplier proof) {
        this(kind, proof, Chunks.fromProperty());
    }

    /// Creates the way to write through that starts at `kind`, or forces if it is `null`, and writes groups of several
    /// blocks as `chunks` says.
    ///
    /// @param proof what must answer `true` before the first segment is opened through, asked once, on the thread of
    ///              the flush that needs it; or `null` if nothing need
    WriteThrough(Kind kind, BooleanSupplier proof, Chunks chunks) {
        this.lock = new Object();
        this.kind = kind;
        this.proof = proof;
        this.chunks = chunks;
    }

    /// {@return how a store in `directory` writes through in `mode`}
    ///
    /// The device is asked no sooner than a segment needs the answer, so a store that only opens and reads sends it
    /// nothing.
    static WriteThrough of(Mode mode, Path directory) {
        var kind = kindOf(mode, directory.getFileSystem() == FileSystems.getDefault());
        return new WriteThrough(kind, kind == Kind.UNBUFFERED && mode == Mode.AUTO ? () -> WriteCache.proves(directory) : null);
    }

    /// {@return how a handle of `kind` on the segment at `path` makes the chunks of a group durable}
    ///
    /// The first handle of `O_DIRECT` that needs it asks the device under the segment, through the files the kernel
    /// shows of it, which takes no round trip to the device.
    private Chunking chunkingOf(Kind kind, Path path) {
        return switch (chunks) {
            case OFF -> Chunking.NONE;
            case DSYNC -> Chunking.THROUGH;
            case DIRECT -> kind == Kind.UNBUFFERED ? Chunking.THROUGH : Chunking.FORCED;
            case AUTO -> switch (kind) {
                case UNBUFFERED -> Chunking.THROUGH;
                case DIRECT -> deviceWritesThrough(path) ? Chunking.THROUGH : Chunking.FORCED;
                case DSYNC -> Chunking.NONE;
            };
        };
    }

    /// {@return whether the device under `path` writes an `O_DSYNC` write with no flush of a volatile cache, asked
    /// once}
    private boolean deviceWritesThrough(Path path) {
        synchronized (lock) {
            if (deviceWritesThrough == null) {
                deviceWritesThrough = DeviceQueue.writesThrough(path);
            }
            return deviceWritesThrough;
        }
    }

    /// Ends the threads that write chunks, which the next chunked group starts again; called once the log writes no
    /// more, as at a close.
    void release() {
        var running = writers;
        if (running != null) {
            running.release();
        }
    }

    /// {@return the threads writing chunks that are running, for tests}
    int chunkWritersForTests() {
        var running = writers;
        return running == null ? 0 : running.running();
    }

    /// {@return the threads writing chunks that look for one now rather than wait parked, for tests}
    int lookingChunkWritersForTests() {
        var running = writers;
        return running == null ? 0 : running.looking();
    }

    /// {@return the chunks posted to a thread that was looking for one, which needed no wake, for tests}
    long postsToLookingForTests() {
        var running = writers;
        return running == null ? 0 : running.postsToLooking();
    }

    private static Kind kindOf(Mode mode, boolean defaultFileSystem) {
        if (mode == Mode.OFF || (defaultFileSystem && OS.startsWith("Mac"))) {
            return null;
        }
        if (!defaultFileSystem) {
            return mode == Mode.AUTO ? null : Kind.DSYNC;
        }
        if (mode == Mode.DSYNC) {
            return Kind.DSYNC;
        }
        if (OS.startsWith("Windows")) {
            return WriteThrough.class.getModule().isNativeAccessEnabled() ? Kind.UNBUFFERED : null;
        }
        return OS.equals("Linux") || mode == Mode.DIRECT ? Kind.DIRECT : null;
    }

    /// {@return whether segments are opened to write through}
    ///
    /// The first call asks the device, if this way needs it to, which may take a round trip to it, and moves the writer
    /// to forces unless it answers that a write through holds.
    boolean enabled() {
        synchronized (lock) {
            if (proof != null) {
                var asked = proof;
                proof = null;
                if (!asked.getAsBoolean()) {
                    kind = null;
                }
            }
            return kind != null;
        }
    }

    /// {@return the way later segments are opened in, or `null` if their groups are forced; it asks no device, so a way
    /// still waiting for the answer counts}
    Kind kind() {
        synchronized (lock) {
            return kind;
        }
    }

    /// Moves to the next way down after the current one failed, or to forces after the last.
    void downgrade() {
        synchronized (lock) {
            kind = kind == Kind.DIRECT ? Kind.DSYNC : null;
        }
    }

    /// Writes the groups of every later segment into the cache and forces them.
    void disable() {
        synchronized (lock) {
            kind = null;
        }
    }

    /// {@return the handles opened so far, for tests}
    int opensForTests() {
        return opens;
    }

    /// Opens the segment at `path` to write through, the current way.
    ///
    /// @throws IOException if it cannot be opened that way, native memory for its handle included
    /// @throws IllegalStateException if no way is left
    Handle open(Path path) throws IOException {
        if (!enabled()) {
            throw new IllegalStateException("The log forces its groups");
        }
        opens++;
        var way = kind();
        try {
            var chunking = chunkingOf(way, path);
            return switch (way) {
                case UNBUFFERED -> UnbufferedHandle.open(path, this, chunking != Chunking.NONE);
                case DIRECT -> new ChannelHandle(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.DSYNC,
                        ExtendedOpenOption.DIRECT), this, path, true, chunking);
                case DSYNC -> new ChannelHandle(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.DSYNC), this,
                        path, false, chunking);
            };
        } catch (OutOfMemoryError exception) {
            throw new IOException("A log segment could not be opened to write through", exception);
        }
    }

    /// {@return the chunks a group of `length` bytes is written in where it may be: up to [#MAX_CHUNKS], of whole
    /// blocks, or 1 past [#MAX_CHUNKED_BYTES]}
    static int chunksFor(long length) {
        if (length > MAX_CHUNKED_BYTES) {
            return 1;
        }
        return (int) Math.clamp(length / LogWriter.ALIGNMENT, 1, MAX_CHUNKS);
    }

    /// {@return the offset in a group of `length` bytes, a multiple of [LogWriter#ALIGNMENT], where chunk `index` of
    /// `count` starts; chunk `count` is the end}
    static int chunkStart(int length, int count, int index) {
        var blocks = length / LogWriter.ALIGNMENT;
        return (int) ((long) blocks * index / count) * LogWriter.ALIGNMENT;
    }

    /// {@return the threads that write chunks past the first, made by the first group that needs them}
    private ChunkWriters writers() {
        var running = writers;
        if (running == null) {
            running = new ChunkWriters();
            writers = running;
        }
        return running;
    }

    /// {@return the buffer holding the first `length` bytes of `bytes`, at least `capacity` bytes long and aligned to
    /// [LogWriter#ALIGNMENT]}
    ///
    /// It follows the size of the log's output, which grows while groups need more and goes back once they no longer
    /// do, so it is replaced only when that size changes.
    ///
    /// @throws IOException if native memory has no room for it, as under a limit of direct memory
    private MemorySegment copy(byte[] bytes, int length, int capacity) throws IOException {
        var size = (long) (capacity + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT;
        if (buffer == null || buffer.byteSize() != size) {
            try {
                // Given back to the collector with the view, once a later size replaces both
                var allocator = allocatorForTests;
                var replaced = allocator != null ? allocator.apply(size) : Arena.ofAuto().allocate(size, LogWriter.ALIGNMENT);
                bufferView = replaced.asByteBuffer();
                chunkViews = null;
                buffer = replaced;
            } catch (OutOfMemoryError exception) {
                throw new IOException("The buffer of a write through could not be allocated", exception);
            }
        }
        MemorySegment.copy(bytes, 0, buffer, ValueLayout.JAVA_BYTE, 0, length);
        return buffer;
    }

    /// {@return the view of the buffer chunk `index` is written from, its position and limit those of the chunk}
    private ByteBuffer chunkView(int index, int start, int end) {
        var views = chunkViews;
        if (views == null) {
            views = new ByteBuffer[MAX_CHUNKS];
            chunkViews = views;
        }
        var view = views[index];
        if (view == null) {
            view = buffer.asByteBuffer();
            views[index] = view;
        }
        return view.limit(end).position(start);
    }

    /// A segment opened to write through.
    sealed interface Handle permits ChannelHandle, UnbufferedHandle {
        /// Writes the first `length` bytes of `bytes` at `position`, a multiple of [LogWriter#ALIGNMENT] as `length`
        /// is, all of them, durable once it returns.
        ///
        /// @param capacity the size of `bytes`, which the copy the write takes them from follows
        /// @throws java.nio.channels.ClosedByInterruptException if an interrupt closed the handle meanwhile, which
        ///         left some, all or none of the bytes written; only a channel's write is interruptible
        /// @throws IOException if the write failed, which may have left some of the bytes written, or if the buffer the
        ///                     bytes are copied into could not be allocated
        default void write(byte[] bytes, int length, int capacity, long position) throws IOException {
            write(bytes, length, capacity, position, 1);
        }

        /// Writes as [#write(byte\[\], int, int, long)] does, in up to `chunks` chunks of whole blocks at once if
        /// the handle writes chunks, and returns once every chunk is durable.
        ///
        /// A chunk that fails has the others waited for before this throws, so nothing writes from the buffer once it
        /// has, and the same bytes may be written again at the same offsets, as one write may be.
        ///
        /// @return the chunks it wrote the bytes in, 1 for one write
        /// @throws java.nio.channels.ClosedByInterruptException if an interrupt closed the handle meanwhile, of the
        ///         caller or of a thread writing a chunk, which left some, all or none of the bytes written
        /// @throws IOException if a chunk failed otherwise, which may have left some of the bytes written
        int write(byte[] bytes, int length, int capacity, long position, int chunks) throws IOException;

        /// Readies the threads that write `chunks` chunks of the next group, so they look for their chunks for `nanos`
        /// nanoseconds and [#CHUNK_SPIN] more rather than wake once it is written: while its frames are sealed, or
        /// while the flush waits for the commits that make it.
        default void prime(int chunks, long nanos) {
        }

        /// {@return whether the handle is open; an interrupt of a write closes a channel}
        boolean isOpen();

        /// Closes the handle.
        void close() throws IOException;
    }

    /// A channel opened with [StandardOpenOption#DSYNC], and `ExtendedOpenOption.DIRECT` on Linux.
    ///
    /// The JDK lends a virtual thread's carrier to another for the write of such a channel, as it does for a force.
    /// Chunks forced together go through a second channel on the segment, opened without `DSYNC` by the first group
    /// that needs it and closed with the handle.
    private static final class ChannelHandle implements Handle {
        private final FileChannel channel;
        private final WriteThrough owner;
        private final Path path;
        private final boolean direct;
        private final Chunking chunking;
        // The segment opened without DSYNC, which chunks forced together are written through, or null until a group
        // needs it
        private FileChannel unsynced;
        // Whether the segment could not be opened without DSYNC, so its groups are written in one write
        private boolean unsyncedRefused;

        ChannelHandle(FileChannel channel, WriteThrough owner, Path path, boolean direct, Chunking chunking) {
            this.channel = channel;
            this.owner = owner;
            this.path = path;
            this.direct = direct;
            this.chunking = chunking;
        }

        @Override
        public int write(byte[] bytes, int length, int capacity, long position, int chunks) throws IOException {
            owner.copy(bytes, length, capacity);
            var count = chunking == Chunking.NONE ? 1 : Math.min(chunks, length / LogWriter.ALIGNMENT);
            var target = channel;
            if (count > 1 && chunking == Chunking.FORCED) {
                target = unsynced();
                if (target == null) {
                    target = channel;
                    count = 1;
                }
            }
            if (count <= 1) {
                // The aligned copy, which a channel opened with DIRECT would otherwise make on each write
                var source = owner.bufferView.clear().limit(length);
                var at = position;
                while (source.hasRemaining()) {
                    at += channel.write(source, at);
                }
                return 1;
            }
            owner.writers().write(owner, target, length, position, count);
            if (target != channel) {
                target.force(false);
            }
            return count;
        }

        @Override
        public void prime(int chunks, long nanos) {
            if (chunking != Chunking.NONE && chunks > 1 && !unsyncedRefused) {
                owner.writers().prime(chunks - 1, nanos);
            }
        }

        /// {@return the segment opened without `DSYNC`, opening it the first time, or `null` if it cannot be}
        private FileChannel unsynced() {
            if (unsynced == null && !unsyncedRefused) {
                try {
                    unsynced = direct ? FileChannel.open(path, StandardOpenOption.WRITE, ExtendedOpenOption.DIRECT)
                            : FileChannel.open(path, StandardOpenOption.WRITE);
                } catch (IOException | RuntimeException _) {
                    // The channel with DSYNC writes every group whole, as without chunks
                    unsyncedRefused = true;
                }
            }
            return unsynced;
        }

        @Override
        public boolean isOpen() {
            var second = unsynced;
            return channel.isOpen() && (second == null || second.isOpen());
        }

        @Override
        public void close() throws IOException {
            try {
                channel.close();
            } finally {
                if (unsynced != null) {
                    unsynced.close();
                }
            }
        }
    }

    /// The platform threads that write the chunks of a group past the first, beside the flush, which writes the first
    /// and waits for them; one group at a time, as one flush at a time writes through.
    ///
    /// Each thread takes one chunk at a time from a slot of its own, and is never interrupted: an interrupt of a
    /// write would close the channel every chunk writes through. A thread with no chunk for [#IDLE_NANOS] ends, as
    /// does one idle at [#release()], and the next chunk for its slot starts another.
    private static final class ChunkWriters {
        private final ChunkWriter[] writers;
        // The chunks of the group being written that have not returned; the flush waits for 0
        private final AtomicInteger remaining;
        // The flush waiting for the chunks
        private volatile Thread joiner;

        ChunkWriters() {
            this.writers = new ChunkWriter[MAX_CHUNKS - 1];
            for (var index = 0; index < writers.length; index++) {
                writers[index] = new ChunkWriter(this, index + 1);
            }
            this.remaining = new AtomicInteger();
        }

        /// Wakes the first `count` threads, if idle, to look for a chunk for `nanos` nanoseconds and [#CHUNK_SPIN] more.
        void prime(int count, long nanos) {
            for (var index = 0; index < Math.min(count, writers.length); index++) {
                writers[index].prime(nanos);
            }
        }

        /// Writes the first `length` bytes of the owner's buffer at `position` of `target` in `count` chunks, the first
        /// on this thread, and returns once every chunk has returned.
        ///
        /// The wait for the chunks is not interruptible: an interrupt meanwhile is kept for the caller.
        ///
        /// @throws ClosedByInterruptException if a chunk found the channel closed by an interrupt, which leaves it
        ///         closed
        /// @throws IOException if a chunk failed otherwise
        void write(WriteThrough owner, FileChannel target, int length, long position, int count) throws IOException {
            joiner = Thread.currentThread();
            remaining.set(count - 1);
            for (var index = 1; index < count; index++) {
                var start = chunkStart(length, count, index);
                var view = owner.chunkView(index, start, chunkStart(length, count, index + 1));
                var writer = writers[index - 1];
                if (!writer.post(target, view, position + start)) {
                    // No thread could be started for it, so this one writes it after its own
                    writer.failure = null;
                    writer.inline = true;
                }
            }
            Throwable own = null;
            var started = System.nanoTime();
            try {
                writeChunk(target, owner.chunkView(0, 0, chunkStart(length, count, 1)), position);
            } catch (Throwable throwable) {
                own = throwable;
            }
            var took = System.nanoTime() - started;
            for (var index = 1; index < count; index++) {
                var writer = writers[index - 1];
                if (writer.inline) {
                    writer.inline = false;
                    try {
                        writeChunk(target, writer.view, writer.position);
                    } catch (Throwable throwable) {
                        writer.failure = throwable;
                    }
                    writer.view = null;
                    remaining.decrementAndGet();
                }
            }
            await(Math.min(took + CHUNK_SPIN, MAX_JOIN_SPIN));
            Throwable failure = own;
            var closed = own == null || own instanceof ClosedChannelException;
            for (var index = 1; index < count; index++) {
                var writer = writers[index - 1];
                var chunkFailure = writer.failure;
                writer.failure = null;
                if (chunkFailure == null) {
                    continue;
                }
                closed &= chunkFailure instanceof ClosedChannelException;
                if (failure == null) {
                    failure = chunkFailure;
                } else if (failure != chunkFailure) {
                    failure.addSuppressed(chunkFailure);
                }
            }
            if (failure == null) {
                return;
            }
            if (closed && !target.isOpen()) {
                // Rewritten whole through a channel opened again, as every chunk may have found this one closed
                if (failure instanceof ClosedByInterruptException interrupted) {
                    throw interrupted;
                }
                var interrupted = new ClosedByInterruptException();
                interrupted.initCause(failure);
                throw interrupted;
            }
            throw failure instanceof IOException exception ? exception
                    : new IOException("A chunk of a log group could not be written through", failure);
        }

        /// Waits for every chunk posted to return, uninterruptibly, looking for them for `spin` nanoseconds before it
        /// parks: a wake of this thread after the last would add its latency to the group's.
        private void await(long spin) {
            if (remaining.get() > 0) {
                var until = System.nanoTime() + spin;
                while (remaining.get() > 0 && System.nanoTime() - until < 0) {
                    Thread.onSpinWait();
                }
            }
            var interrupted = false;
            while (remaining.get() > 0) {
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        /// Counts a chunk of a thread as returned, and wakes the flush once it is the last; a flush still looking for
        /// it sees it without the wake.
        void returned() {
            if (remaining.decrementAndGet() == 0) {
                LockSupport.unpark(joiner);
            }
        }

        /// Ends every thread idle now; the next chunk for its slot starts another.
        void release() {
            for (var writer : writers) {
                writer.release();
            }
        }

        /// {@return the chunks posted to a thread that was looking for one rather than parked}
        long postsToLooking() {
            var count = 0L;
            for (var writer : writers) {
                count += writer.postsToLooking;
            }
            return count;
        }

        /// {@return the threads looking for a chunk now rather than parked or writing one}
        int looking() {
            var count = 0;
            for (var writer : writers) {
                if (writer.looking) {
                    count++;
                }
            }
            return count;
        }

        /// {@return the threads running}
        int running() {
            var count = 0;
            for (var writer : writers) {
                var thread = writer.thread;
                if (writer.state.get() != ChunkWriter.GONE && thread != null && thread.isAlive()) {
                    count++;
                }
            }
            return count;
        }

        /// Writes the bytes from the position to the limit of `view` at `position` of `target`, all of them.
        static void writeChunk(FileChannel target, ByteBuffer view, long position) throws IOException {
            var at = position;
            while (view.hasRemaining()) {
                at += target.write(view, at);
            }
        }
    }

    /// The slot of one thread that writes chunks, and the thread.
    private static final class ChunkWriter implements Runnable {
        // No thread runs for the slot, one waits for a chunk, or a chunk is posted to it
        static final int GONE = 0;
        static final int IDLE = 1;
        static final int POSTED = 2;
        private final ChunkWriters group;
        private final int index;
        final AtomicInteger state;
        // The thread that serves the slot while it is not GONE
        volatile Thread thread;
        // Whether a group about to be written woke the thread, which then looks for its chunk before it parks, and
        // for how long beyond CHUNK_SPIN, in nanoseconds; the length is written before the flag
        private volatile boolean primed;
        private volatile long primedNanos;
        // Whether the thread looks for a chunk, spinning, so a chunk posted meanwhile needs no wake: the thread clears
        // it before it reads the state it parks on, and the flush posts before it reads it, so one sees the other
        private volatile boolean looking;
        // The chunks posted while the thread was looking, for tests; written by the flush
        private volatile long postsToLooking;
        // Whether idle threads end at once
        private volatile boolean released;
        // The chunk posted: written before the state turns POSTED, read by the thread once it sees it
        private FileChannel target;
        ByteBuffer view;
        long position;
        // What the last chunk threw, written before it counts as returned, read by the flush once all have
        Throwable failure;
        // Whether the flush writes the posted chunk itself, after no thread could be started for it
        boolean inline;

        ChunkWriter(ChunkWriters group, int index) {
            this.group = group;
            this.index = index;
            this.state = new AtomicInteger(GONE);
        }

        /// Posts the chunk of `view` at `position` of `target`, starting the slot's thread if none runs.
        ///
        /// @return whether a thread will write it; `false` if none could be started, with the chunk left posted
        boolean post(FileChannel target, ByteBuffer view, long position) {
            this.target = target;
            this.view = view;
            this.position = position;
            while (true) {
                var current = state.get();
                if (current == IDLE) {
                    if (state.compareAndSet(IDLE, POSTED)) {
                        if (looking) {
                            postsToLooking++;
                        } else {
                            LockSupport.unpark(thread);
                        }
                        return true;
                    }
                } else if (current == GONE) {
                    if (state.compareAndSet(GONE, POSTED)) {
                        released = false;
                        try {
                            var started = Thread.ofPlatform().daemon().name("ledger-io-chunk-" + index).unstarted(this);
                            thread = started;
                            started.start();
                            return true;
                        } catch (Throwable _) {
                            // As for an OutOfMemoryError of a thread the system had no room for
                            thread = null;
                            state.set(GONE);
                            return false;
                        }
                    }
                } else {
                    throw new IllegalStateException("A chunk was posted before the last one returned");
                }
            }
        }

        /// Wakes the thread if it is idle, to look for a chunk for `nanos` nanoseconds and [#CHUNK_SPIN] more.
        void prime(long nanos) {
            if (state.get() == IDLE) {
                primedNanos = nanos;
                primed = true;
                if (!looking) {
                    LockSupport.unpark(thread);
                }
            }
        }

        /// Ends the thread if it is idle, at once.
        void release() {
            released = true;
            var current = thread;
            if (current != null) {
                LockSupport.unpark(current);
            }
        }

        @Override
        public void run() {
            // The time the thread looks for a chunk until, as System.nanoTime reads it
            var lookUntil = System.nanoTime();
            while (true) {
                if (state.get() == POSTED) {
                    write();
                    // The group a prime announced is written: the next follows within about the time a flush takes
                    // from one group to the next
                    lookUntil = System.nanoTime() + CHUNK_SPIN;
                    continue;
                }
                if (released && state.compareAndSet(IDLE, GONE)) {
                    return;
                }
                if (primed) {
                    primed = false;
                    lookUntil = later(lookUntil, System.nanoTime() + Math.max(0, primedNanos) + CHUNK_SPIN);
                }
                if (System.nanoTime() - lookUntil < 0) {
                    looking = true;
                    while (state.get() == IDLE && !released && !primed && System.nanoTime() - lookUntil < 0) {
                        Thread.onSpinWait();
                    }
                    looking = false;
                    continue;
                }
                var parked = System.nanoTime();
                LockSupport.parkNanos(this, IDLE_NANOS);
                // An interrupt, which nothing should send, would close the channel under the next chunk
                Thread.interrupted();
                if (state.get() == IDLE && System.nanoTime() - parked >= IDLE_NANOS && state.compareAndSet(IDLE, GONE)) {
                    return;
                }
            }
        }

        /// {@return the later of two times `System.nanoTime` read}
        private static long later(long first, long second) {
            return second - first > 0 ? second : first;
        }

        /// Writes the chunk posted, then counts it as returned.
        private void write() {
            Throwable thrown = null;
            try {
                ChunkWriters.writeChunk(target, view, position);
            } catch (Throwable throwable) {
                thrown = throwable;
                // An interrupt of this thread closed the channel; the next chunk must not find it set
                Thread.interrupted();
            }
            failure = thrown;
            target = null;
            view = null;
            state.set(IDLE);
            group.returned();
        }
    }

    /// A handle that Windows opened with `FILE_FLAG_NO_BUFFERING` and `FILE_FLAG_WRITE_THROUGH`, which writes from the
    /// aligned buffer at the offsets its writes name.
    ///
    /// A write is not interruptible, and holds a virtual thread's carrier while the device takes it: the JDK lends a
    /// carrier to another thread only for its own blocking calls.
    ///
    /// A handle that writes chunks is opened for overlapped writes, and every write of it is one: each chunk has an
    /// `OVERLAPPED` and a manual-reset event of its own, which its completion alone sets, so the wait for one chunk
    /// never returns on the completion of another, as a wait on the handle itself could. A write waits for every chunk
    /// it issued, whatever failed, before it returns or throws, so no write is left running into the buffer or into
    /// the structures once the handle is closed.
    private static final class UnbufferedHandle implements Handle {
        private static final int GENERIC_WRITE = 0x4000_0000;
        // FILE_SHARE_READ, FILE_SHARE_WRITE and FILE_SHARE_DELETE, as the JDK shares the files it opens
        static final int SHARE_ALL = 0x7;
        static final int OPEN_EXISTING = 3;
        private static final int FILE_FLAG_WRITE_THROUGH = 0x8000_0000;
        private static final int FILE_FLAG_NO_BUFFERING = 0x2000_0000;
        private static final int FILE_FLAG_OVERLAPPED = 0x4000_0000;
        // What WriteFile leaves for an overlapped write it issued and that has not completed
        private static final int ERROR_IO_PENDING = 997;
        // OVERLAPPED: two pointer-sized words, then the low and high halves of the offset, then an event handle
        private static final long OVERLAPPED_BYTES = 32;
        private static final long OFFSET_LOW = 16;
        private static final long OFFSET_HIGH = 20;
        private static final long EVENT = 24;

        private final MemorySegment handle;
        private final WriteThrough owner;
        private final MemorySegment state;
        // One OVERLAPPED for a handle that writes in one synchronous write, one a chunk for an overlapped handle
        private final MemorySegment[] overlapped;
        // The manual-reset event of each OVERLAPPED of an overlapped handle, or null
        private final MemorySegment[] events;
        private final MemorySegment written;
        // Per chunk of the write under way: where in the buffer it starts and ends, how much of it is done, and whether
        // a write of it was issued and not waited for yet
        private final int[] chunkDone;
        private final int[] chunkEnd;
        private final boolean[] issued;
        private boolean open;

        private UnbufferedHandle(MemorySegment handle, WriteThrough owner, Arena arena, MemorySegment state, MemorySegment[] events) {
            this.handle = handle;
            this.owner = owner;
            this.state = state;
            this.events = events;
            this.overlapped = new MemorySegment[events == null ? 1 : events.length];
            for (var index = 0; index < overlapped.length; index++) {
                overlapped[index] = arena.allocate(OVERLAPPED_BYTES, Long.BYTES);
            }
            this.written = arena.allocate(ValueLayout.JAVA_INT);
            this.chunkDone = new int[MAX_CHUNKS];
            this.chunkEnd = new int[MAX_CHUNKS];
            this.issued = new boolean[MAX_CHUNKS];
            this.open = true;
        }

        /// {@return the segment at `path` opened unbuffered and written through, for overlapped writes of up to
        /// [#MAX_CHUNKS] chunks at once if `overlapped`}
        ///
        /// Writes through it leave the file's times as they are: Windows writes the metadata changes a write through
        /// makes through as well, which for a time of last write would be one more write to the device each time.
        static UnbufferedHandle open(Path path, WriteThrough owner, boolean overlapped) throws IOException {
            var arena = Arena.ofAuto();
            var state = arena.allocate(Kernel32.CALL_STATE);
            var name = arena.allocateFrom(win32Path(path), StandardCharsets.UTF_16LE);
            var flags = FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING | (overlapped ? FILE_FLAG_OVERLAPPED : 0);
            MemorySegment handle;
            try {
                handle = (MemorySegment) Kernel32.CREATE_FILE.invokeExact(state, name, GENERIC_WRITE, SHARE_ALL,
                        MemorySegment.NULL, OPEN_EXISTING, flags, MemorySegment.NULL);
            } catch (Throwable throwable) {
                throw new IOException("The log segment could not be opened unbuffered: " + path, throwable);
            }
            if (handle.address() == -1L) {
                throw new IOException("The log segment could not be opened unbuffered: " + path + ", error " + Kernel32.lastError(state));
            }
            MemorySegment[] events = null;
            if (overlapped) {
                events = new MemorySegment[MAX_CHUNKS];
                try {
                    for (var index = 0; index < events.length; index++) {
                        var event = (MemorySegment) Kernel32.CREATE_EVENT.invokeExact(state, MemorySegment.NULL, 1, 0, MemorySegment.NULL);
                        if (event.address() == 0) {
                            throw new IOException("An event for the writes of a log segment could not be created: error " + Kernel32.lastError(state));
                        }
                        events[index] = event;
                    }
                } catch (Throwable throwable) {
                    closeQuietly(state, handle);
                    for (var event : events) {
                        if (event != null) {
                            closeQuietly(state, event);
                        }
                    }
                    throw throwable instanceof IOException exception ? exception
                            : new IOException("An event for the writes of a log segment could not be created", throwable);
                }
            }
            var opened = new UnbufferedHandle(handle, owner, arena, state, events);
            // A FILETIME of all ones, which stops the writes of the handle from setting that time
            var unchanged = arena.allocate(ValueLayout.JAVA_LONG);
            unchanged.set(ValueLayout.JAVA_LONG, 0, -1L);
            try {
                var _ = (int) Kernel32.SET_FILE_TIME.invokeExact(handle, MemorySegment.NULL, unchanged, unchanged);
            } catch (Throwable _) {
                // The times then change as the writes make them, which costs time and loses nothing
            }
            return opened;
        }

        /// {@return `path` as Windows takes it whatever its length: absolute, behind the prefix that lifts the limit
        /// of 260 characters}
        static String win32Path(Path path) {
            var absolute = path.toAbsolutePath().normalize().toString();
            if (absolute.startsWith("\\\\?\\")) {
                return absolute;
            }
            return absolute.startsWith("\\\\") ? "\\\\?\\UNC\\" + absolute.substring(2) : "\\\\?\\" + absolute;
        }

        @Override
        public int write(byte[] bytes, int length, int capacity, long position, int chunks) throws IOException {
            var source = owner.copy(bytes, length, capacity);
            if (events != null) {
                var count = Math.clamp(Math.min(chunks, length / LogWriter.ALIGNMENT), 1, events.length);
                writeOverlapped(source, length, position, count);
                return count;
            }
            var block = overlapped[0];
            var done = 0;
            while (done < length) {
                var at = position + done;
                block.fill((byte) 0);
                block.set(ValueLayout.JAVA_INT, OFFSET_LOW, (int) at);
                block.set(ValueLayout.JAVA_INT, OFFSET_HIGH, (int) (at >>> 32));
                int succeeded;
                try {
                    succeeded = (int) Kernel32.WRITE_FILE.invokeExact(state, handle, source.asSlice(done), length - done, written, block);
                } catch (Throwable throwable) {
                    throw new IOException("A log group could not be written through", throwable);
                }
                if (succeeded == 0) {
                    throw new IOException("A log group could not be written through: error " + Kernel32.lastError(state));
                }
                var count = written.get(ValueLayout.JAVA_INT, 0);
                if (count <= 0) {
                    throw new IOException("A write through of a log group wrote nothing");
                }
                done += count;
            }
            return 1;
        }

        /// Writes the first `length` bytes of `source` at `position` as `count` overlapped writes at once, issuing the
        /// rest of a chunk that a write cut short again, until every chunk is done or one fails.
        ///
        /// @throws IOException if a chunk failed, once every chunk issued has completed
        private void writeOverlapped(MemorySegment source, int length, long position, int count) throws IOException {
            for (var index = 0; index < count; index++) {
                chunkDone[index] = chunkStart(length, count, index);
                chunkEnd[index] = chunkStart(length, count, index + 1);
            }
            IOException failure = null;
            var unfinished = true;
            while (unfinished && failure == null) {
                for (var index = 0; index < count && failure == null; index++) {
                    if (chunkDone[index] < chunkEnd[index]) {
                        failure = issue(source, position, index);
                    }
                }
                unfinished = false;
                for (var index = 0; index < count; index++) {
                    if (!issued[index]) {
                        continue;
                    }
                    issued[index] = false;
                    var outcome = complete(index);
                    if (outcome != null) {
                        if (failure == null) {
                            failure = outcome;
                        } else {
                            failure.addSuppressed(outcome);
                        }
                    }
                    unfinished |= chunkDone[index] < chunkEnd[index];
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        /// Issues the overlapped write of what is left of chunk `index`.
        ///
        /// @return the failure of the write, which Windows did not issue, or `null` if it is issued or done
        private IOException issue(MemorySegment source, long position, int index) {
            var block = overlapped[index];
            var from = chunkDone[index];
            var at = position + from;
            block.fill((byte) 0);
            block.set(ValueLayout.JAVA_INT, OFFSET_LOW, (int) at);
            block.set(ValueLayout.JAVA_INT, OFFSET_HIGH, (int) (at >>> 32));
            block.set(ValueLayout.ADDRESS, EVENT, events[index]);
            int succeeded;
            try {
                // No count of the bytes written: an overlapped write reports it to the wait
                succeeded = (int) Kernel32.WRITE_FILE.invokeExact(state, handle, source.asSlice(from, chunkEnd[index] - from),
                        chunkEnd[index] - from, MemorySegment.NULL, block);
            } catch (Throwable throwable) {
                // Whether the write was issued is unknown, so the memory it may complete into stays reachable
                UNSETTLED.add(this);
                return new IOException("A chunk of a log group could not be written through", throwable);
            }
            if (succeeded == 0) {
                var error = Kernel32.lastError(state);
                if (error != ERROR_IO_PENDING) {
                    return new IOException("A chunk of a log group could not be written through: error " + error);
                }
            }
            // Completed or pending alike, the wait reports how much it wrote
            issued[index] = true;
            return null;
        }

        /// Waits for the overlapped write of chunk `index` to complete, and counts what it wrote.
        ///
        /// @return its failure, or `null` if it wrote some of the chunk
        private IOException complete(int index) {
            int succeeded;
            try {
                succeeded = (int) Kernel32.GET_OVERLAPPED_RESULT.invokeExact(state, handle, overlapped[index], written, 1);
            } catch (Throwable throwable) {
                UNSETTLED.add(this);
                return new IOException("A chunk of a log group could not be waited for", throwable);
            }
            if (succeeded == 0) {
                return new IOException("A chunk of a log group could not be written through: error " + Kernel32.lastError(state));
            }
            var count = written.get(ValueLayout.JAVA_INT, 0);
            if (count <= 0) {
                return new IOException("A write through of a chunk of a log group wrote nothing");
            }
            chunkDone[index] = Math.min(chunkEnd[index], chunkDone[index] + count);
            return null;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            if (!open) {
                return;
            }
            open = false;
            int closed;
            try {
                closed = (int) Kernel32.CLOSE_HANDLE.invokeExact(state, handle);
            } catch (Throwable throwable) {
                throw new IOException("A log segment's unbuffered handle could not be closed", throwable);
            } finally {
                if (events != null) {
                    for (var event : events) {
                        closeQuietly(state, event);
                    }
                }
            }
            if (closed == 0) {
                throw new IOException("A log segment's unbuffered handle could not be closed: error " + Kernel32.lastError(state));
            }
        }

        /// Closes `handle`, ignoring a failure: nothing was written through it that a group depends on.
        private static void closeQuietly(MemorySegment state, MemorySegment handle) {
            try {
                var _ = (int) Kernel32.CLOSE_HANDLE.invokeExact(state, handle);
            } catch (Throwable _) {
                // Only a handle left open to the end of the process
            }
        }
    }

    /// What the queue of a Linux block device reports of how it writes, in `/sys/dev/block/<major>:<minor>/queue`, or
    /// that of the disk a partition is on.
    ///
    /// @spec https://www.kernel.org/doc/html/latest/block/queue-sysfs.html Queue sysfs files
    static final class DeviceQueue {
        private DeviceQueue() {
        }

        /// {@return whether the device that holds `path` writes an `O_DSYNC` write without emptying a volatile cache:
        /// it writes with forced unit access, or its cache writes through; `false` if it cannot be told, as for a file
        /// system on no single block device}
        static boolean writesThrough(Path path) {
            if (!OS.equals("Linux") || path.getFileSystem() != FileSystems.getDefault()) {
                return false;
            }
            try {
                var queue = queueOf((long) Files.getAttribute(path, "unix:dev"));
                if (queue == null) {
                    return false;
                }
                return read(queue.resolve("fua")).equals("1") || read(queue.resolve("write_cache")).equals("write through");
            } catch (IOException | RuntimeException _) {
                return false;
            }
        }

        /// {@return the directory of the queue of the device numbered `device`, or `null` if it has none}
        static Path queueOf(long device) throws IOException {
            var numbers = majorMinor(device);
            var block = Path.of("/sys/dev/block/" + numbers[0] + ":" + numbers[1]);
            if (!Files.exists(block)) {
                return null;
            }
            var real = block.toRealPath();
            var queue = real.resolve("queue");
            if (Files.isDirectory(queue)) {
                return queue;
            }
            // A partition, whose disk holds the queue
            var parent = real.getParent();
            return parent != null && Files.isDirectory(parent.resolve("queue")) ? parent.resolve("queue") : null;
        }

        /// {@return the major and minor numbers of the device numbered `device`, split as glibc's `makedev` joins them}
        static long[] majorMinor(long device) {
            var major = ((device >>> 8) & 0xFFFL) | ((device >>> 32) & 0xFFFF_F000L);
            var minor = (device & 0xFFL) | ((device >>> 12) & 0xFFFF_FF00L);
            return new long[]{major, minor};
        }

        /// {@return the first line of the file at `path`, trimmed}
        private static String read(Path path) throws IOException {
            var lines = Files.readAllLines(path);
            return lines.isEmpty() ? "" : lines.getFirst().trim();
        }
    }

    /// What the device under a Windows volume reports of its write cache.
    ///
    /// @param enabled the `WRITE_CACHE_ENABLE` it reports
    /// @param writeThrough the `WRITE_THROUGH` it reports
    /// @param nonVolatile whether it reports that the cache keeps its contents without power
    /// @spec https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_write_cache_property STORAGE_WRITE_CACHE_PROPERTY
    record WriteCache(int enabled, int writeThrough, boolean nonVolatile) {
        // WriteCacheDisabled, of WriteCacheEnableUnknown, WriteCacheDisabled and WriteCacheEnabled
        private static final int DISABLED = 1;
        // WriteThroughSupported, of WriteThroughUnknown, WriteThroughNotSupported and WriteThroughSupported
        private static final int SUPPORTED = 2;
        // Offsets in STORAGE_WRITE_CACHE_PROPERTY
        private static final long ENABLED = 12;
        private static final long WRITE_THROUGH = 20;
        private static final long NON_VOLATILE = 26;
        private static final int PROPERTY_BYTES = 28;
        // IOCTL_STORAGE_QUERY_PROPERTY, and the STORAGE_PROPERTY_QUERY it takes: StorageDeviceWriteCacheProperty, then
        // PropertyStandardQuery, 0, then a byte of parameters this property reads none of
        private static final int QUERY_PROPERTY = 0x2D_1400;
        private static final int QUERY_BYTES = 12;
        private static final int WRITE_CACHE_PROPERTY = 4;
        // GetFinalPathNameByHandleW: VOLUME_NAME_GUID, and the longest path Windows has, in characters
        private static final int VOLUME_NAME_GUID = 1;
        private static final int PATH_CHARS = 32_768;
        private static final int FILE_FLAG_BACKUP_SEMANTICS = 0x0200_0000;

        /// {@return whether a write through holds what a force holds on this device}
        ///
        /// It holds where the device honours forced unit access, which is what the request becomes, and where nothing
        /// volatile lies between a write and the medium. An unknown answer proves nothing.
        boolean provesWriteThrough() {
            return writeThrough == SUPPORTED || enabled == DISABLED || nonVolatile;
        }

        /// {@return whether the device under `directory` reports that a write through holds; `false` if it cannot be
        /// asked, as on a network share}
        static boolean proves(Path directory) {
            var cache = of(directory);
            return cache != null && cache.provesWriteThrough();
        }

        /// {@return what the device of the volume that really holds `directory`, through its links and mount points,
        /// reports, or `null` if it cannot be asked or does not answer}
        ///
        /// It takes no privileges: both handles are opened for no access, and the query needs none. The answer may take
        /// a round trip to the device.
        static WriteCache of(Path directory) {
            try (var arena = Arena.ofConfined()) {
                var state = arena.allocate(Kernel32.CALL_STATE);
                var volume = volumeOf(directory, arena, state);
                if (volume == null) {
                    return null;
                }
                var device = (MemorySegment) Kernel32.CREATE_FILE.invokeExact(state, arena.allocateFrom(volume, StandardCharsets.UTF_16LE),
                        0, UnbufferedHandle.SHARE_ALL, MemorySegment.NULL, UnbufferedHandle.OPEN_EXISTING, 0, MemorySegment.NULL);
                if (device.address() == -1L) {
                    return null;
                }
                try {
                    var query = arena.allocate(QUERY_BYTES, Integer.BYTES);
                    query.set(ValueLayout.JAVA_INT, 0, WRITE_CACHE_PROPERTY);
                    var property = arena.allocate(PROPERTY_BYTES, Integer.BYTES);
                    var returned = arena.allocate(ValueLayout.JAVA_INT);
                    var answered = (int) Kernel32.DEVICE_IO_CONTROL.invokeExact(state, device, QUERY_PROPERTY, query, QUERY_BYTES,
                            property, PROPERTY_BYTES, returned, MemorySegment.NULL);
                    return answered == 0 ? null : parse(property, returned.get(ValueLayout.JAVA_INT, 0));
                } finally {
                    var _ = (int) Kernel32.CLOSE_HANDLE.invokeExact(state, device);
                }
            } catch (Throwable _) {
                // Nothing proved, so the log forces, which needs no answer
                return null;
            }
        }

        /// {@return the name of the volume that holds `directory`, without the separator after it, or `null` if it has
        /// none}
        private static String volumeOf(Path directory, Arena arena, MemorySegment state) throws Throwable {
            var opened = (MemorySegment) Kernel32.CREATE_FILE.invokeExact(state,
                    arena.allocateFrom(UnbufferedHandle.win32Path(directory), StandardCharsets.UTF_16LE), 0, UnbufferedHandle.SHARE_ALL,
                    MemorySegment.NULL, UnbufferedHandle.OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, MemorySegment.NULL);
            if (opened.address() == -1L) {
                return null;
            }
            try {
                var name = arena.allocate((long) PATH_CHARS * Character.BYTES, Character.BYTES);
                var length = (int) Kernel32.GET_FINAL_PATH.invokeExact(state, opened, name, PATH_CHARS, VOLUME_NAME_GUID);
                if (length <= 0 || length >= PATH_CHARS) {
                    return null;
                }
                // Starts with the name: \\?\Volume{guid}\
                var path = name.getString(0, StandardCharsets.UTF_16LE);
                var end = path.indexOf('\\', "\\\\?\\".length());
                return end < 0 ? path : path.substring(0, end);
            } finally {
                var _ = (int) Kernel32.CLOSE_HANDLE.invokeExact(state, opened);
            }
        }

        /// {@return the answer in the first `returned` bytes of `property`, or `null` if they do not hold it}
        ///
        /// A device that returns fewer bytes than the structure has says nothing of the fields it left out.
        static WriteCache parse(MemorySegment property, int returned) {
            if (returned < WRITE_THROUGH + Integer.BYTES) {
                return null;
            }
            return new WriteCache(property.get(ValueLayout.JAVA_INT, ENABLED), property.get(ValueLayout.JAVA_INT, WRITE_THROUGH),
                    returned > NON_VOLATILE && property.get(ValueLayout.JAVA_BYTE, NON_VOLATILE) != 0);
        }
    }

    /// The functions of `kernel32` an unbuffered handle and a query of a device call, looked up the first time one
    /// runs.
    private static final class Kernel32 {
        static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
        private static final VarHandle LAST_ERROR = CALL_STATE.varHandle(MemoryLayout.PathElement.groupElement("GetLastError"));
        static final MethodHandle CREATE_FILE;
        static final MethodHandle WRITE_FILE;
        static final MethodHandle CREATE_EVENT;
        static final MethodHandle GET_OVERLAPPED_RESULT;
        static final MethodHandle CLOSE_HANDLE;
        static final MethodHandle SET_FILE_TIME;
        static final MethodHandle DEVICE_IO_CONTROL;
        static final MethodHandle GET_FINAL_PATH;

        static {
            var linker = Linker.nativeLinker();
            var library = SymbolLookup.libraryLookup("kernel32", Arena.global());
            var lastError = Linker.Option.captureCallState("GetLastError");
            var address = ValueLayout.ADDRESS;
            var integer = ValueLayout.JAVA_INT;
            CREATE_FILE = linker.downcallHandle(library.findOrThrow("CreateFileW"),
                    FunctionDescriptor.of(address, address, integer, integer, address, integer, integer, address), lastError);
            WRITE_FILE = linker.downcallHandle(library.findOrThrow("WriteFile"),
                    FunctionDescriptor.of(integer, address, address, integer, address, address), lastError);
            CREATE_EVENT = linker.downcallHandle(library.findOrThrow("CreateEventW"),
                    FunctionDescriptor.of(address, address, integer, integer, address), lastError);
            GET_OVERLAPPED_RESULT = linker.downcallHandle(library.findOrThrow("GetOverlappedResult"),
                    FunctionDescriptor.of(integer, address, address, address, integer), lastError);
            CLOSE_HANDLE =linker.downcallHandle(library.findOrThrow("CloseHandle"),
                    FunctionDescriptor.of(integer, address), lastError);
            SET_FILE_TIME = linker.downcallHandle(library.findOrThrow("SetFileTime"),
                    FunctionDescriptor.of(integer, address, address, address, address));
            DEVICE_IO_CONTROL = linker.downcallHandle(library.findOrThrow("DeviceIoControl"),
                    FunctionDescriptor.of(integer, address, integer, address, integer, address, integer, address, address), lastError);
            GET_FINAL_PATH = linker.downcallHandle(library.findOrThrow("GetFinalPathNameByHandleW"),
                    FunctionDescriptor.of(integer, address, address, integer, integer), lastError);
        }

        private Kernel32() {
        }

        /// {@return the error code the last call that captured it left in `state`}
        static int lastError(MemorySegment state) {
            return (int) LAST_ERROR.get(state, 0L);
        }
    }
}

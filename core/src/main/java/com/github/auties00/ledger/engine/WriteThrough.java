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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
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
final class WriteThrough {
    private static final String OS = System.getProperty("os.name", "");

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
    // The buffer a group is copied into before it is written, aligned to LogWriter.ALIGNMENT, and a view of it
    private MemorySegment buffer;
    private ByteBuffer bufferView;
    // The handles opened, for tests; written by the flush that holds the log writer
    private volatile int opens;
    /// Allocates the buffer in place of the arena, by its size; for tests that refuse it.
    LongFunction<MemorySegment> allocatorForTests;

    /// Creates the way to write through that starts at `kind`, or forces if it is `null`.
    ///
    /// @param proof what must answer `true` before the first segment is opened through, asked once, on the thread of
    ///              the flush that needs it; or `null` if nothing need
    WriteThrough(Kind kind, BooleanSupplier proof) {
        this.lock = new Object();
        this.kind = kind;
        this.proof = proof;
    }

    /// {@return how a store in `directory` writes through in `mode`}
    ///
    /// The device is asked no sooner than a segment needs the answer, so a store that only opens and reads sends it
    /// nothing.
    static WriteThrough of(Mode mode, Path directory) {
        var kind = kindOf(mode, directory.getFileSystem() == FileSystems.getDefault());
        return new WriteThrough(kind, kind == Kind.UNBUFFERED && mode == Mode.AUTO ? () -> WriteCache.proves(directory) : null);
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
        try {
            return switch (kind()) {
                case UNBUFFERED -> UnbufferedHandle.open(path, this);
                case DIRECT -> new ChannelHandle(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.DSYNC,
                        ExtendedOpenOption.DIRECT), this);
                case DSYNC -> new ChannelHandle(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.DSYNC), this);
            };
        } catch (OutOfMemoryError exception) {
            throw new IOException("A log segment could not be opened to write through", exception);
        }
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
                buffer = replaced;
            } catch (OutOfMemoryError exception) {
                throw new IOException("The buffer of a write through could not be allocated", exception);
            }
        }
        MemorySegment.copy(bytes, 0, buffer, ValueLayout.JAVA_BYTE, 0, length);
        return buffer;
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
        void write(byte[] bytes, int length, int capacity, long position) throws IOException;

        /// {@return whether the handle is open; an interrupt of a write closes a channel}
        boolean isOpen();

        /// Closes the handle.
        void close() throws IOException;
    }

    /// A channel opened with [StandardOpenOption#DSYNC], and `ExtendedOpenOption.DIRECT` on Linux.
    ///
    /// The JDK lends a virtual thread's carrier to another for the write of such a channel, as it does for a force.
    private static final class ChannelHandle implements Handle {
        private final FileChannel channel;
        private final WriteThrough owner;

        ChannelHandle(FileChannel channel, WriteThrough owner) {
            this.channel = channel;
            this.owner = owner;
        }

        @Override
        public void write(byte[] bytes, int length, int capacity, long position) throws IOException {
            owner.copy(bytes, length, capacity);
            // The aligned copy, which a channel opened with DIRECT would otherwise make on each write
            var source = owner.bufferView.clear().limit(length);
            var at = position;
            while (source.hasRemaining()) {
                at += channel.write(source, at);
            }
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    /// A handle that Windows opened with `FILE_FLAG_NO_BUFFERING` and `FILE_FLAG_WRITE_THROUGH`, which writes from the
    /// aligned buffer at the offsets its writes name.
    ///
    /// A write is not interruptible, and holds a virtual thread's carrier while the device takes it: the JDK lends a
    /// carrier to another thread only for its own blocking calls.
    private static final class UnbufferedHandle implements Handle {
        private static final int GENERIC_WRITE = 0x4000_0000;
        // FILE_SHARE_READ, FILE_SHARE_WRITE and FILE_SHARE_DELETE, as the JDK shares the files it opens
        static final int SHARE_ALL = 0x7;
        static final int OPEN_EXISTING = 3;
        private static final int FILE_FLAG_WRITE_THROUGH = 0x8000_0000;
        private static final int FILE_FLAG_NO_BUFFERING = 0x2000_0000;
        // OVERLAPPED: two pointer-sized words, then the low and high halves of the offset, then an event handle
        private static final long OVERLAPPED_BYTES = 32;
        private static final long OFFSET_LOW = 16;
        private static final long OFFSET_HIGH = 20;

        private final MemorySegment handle;
        private final WriteThrough owner;
        private final MemorySegment state;
        private final MemorySegment overlapped;
        private final MemorySegment written;
        private boolean open;

        private UnbufferedHandle(MemorySegment handle, WriteThrough owner, Arena arena, MemorySegment state) {
            this.handle = handle;
            this.owner = owner;
            this.state = state;
            this.overlapped = arena.allocate(OVERLAPPED_BYTES, Long.BYTES);
            this.written = arena.allocate(ValueLayout.JAVA_INT);
            this.open = true;
        }

        /// {@return the segment at `path` opened unbuffered and written through}
        ///
        /// Writes through it leave the file's times as they are: Windows writes the metadata changes a write through
        /// makes through as well, which for a time of last write would be one more write to the device each time.
        static UnbufferedHandle open(Path path, WriteThrough owner) throws IOException {
            var arena = Arena.ofAuto();
            var state = arena.allocate(Kernel32.CALL_STATE);
            var name = arena.allocateFrom(win32Path(path), StandardCharsets.UTF_16LE);
            MemorySegment handle;
            try {
                handle = (MemorySegment) Kernel32.CREATE_FILE.invokeExact(state, name, GENERIC_WRITE, SHARE_ALL,
                        MemorySegment.NULL, OPEN_EXISTING, FILE_FLAG_WRITE_THROUGH | FILE_FLAG_NO_BUFFERING, MemorySegment.NULL);
            } catch (Throwable throwable) {
                throw new IOException("The log segment could not be opened unbuffered: " + path, throwable);
            }
            if (handle.address() == -1L) {
                throw new IOException("The log segment could not be opened unbuffered: " + path + ", error " + Kernel32.lastError(state));
            }
            var opened = new UnbufferedHandle(handle, owner, arena, state);
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
        public void write(byte[] bytes, int length, int capacity, long position) throws IOException {
            var source = owner.copy(bytes, length, capacity);
            var done = 0;
            while (done < length) {
                var at = position + done;
                overlapped.fill((byte) 0);
                overlapped.set(ValueLayout.JAVA_INT, OFFSET_LOW, (int) at);
                overlapped.set(ValueLayout.JAVA_INT, OFFSET_HIGH, (int) (at >>> 32));
                int succeeded;
                try {
                    succeeded = (int) Kernel32.WRITE_FILE.invokeExact(state, handle, source.asSlice(done), length - done, written, overlapped);
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
            }
            if (closed == 0) {
                throw new IOException("A log segment's unbuffered handle could not be closed: error " + Kernel32.lastError(state));
            }
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
            CLOSE_HANDLE = linker.downcallHandle(library.findOrThrow("CloseHandle"),
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

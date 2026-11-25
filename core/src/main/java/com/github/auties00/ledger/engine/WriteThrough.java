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
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongFunction;
import java.util.function.Supplier;

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
///   guarantee in one call. The segment is opened as a channel, which platform threads write through. While the module
///   has native access, on x86-64 and AArch64 and on the JDK's own provider of the default file system, a virtual
///   thread writes through a descriptor the C library opened the same way beside the channel ([PosixHandle]): its write
///   holds the carrier for the one round trip to the device, where the channel's would lend the carrier to another by
///   waking a carrier that then finds nothing to do. At most [Posix#LIMIT] virtual threads of the process hold their
///   carriers so at a time, so a slow or hung device never holds every carrier; the next writes through the channel. A
///   scheduler of one carrier has none to spare, so there every virtual thread writes through the channel.
///   Through the descriptor the kernel checks the alignment of a write against the device's logical block, not against
///   the block size the file system reports as the channel does, so it may take a write the channel would refuse,
///   durable all the same under `O_DSYNC`. A write the channel refuses before it has taken one of the writer's is
///   therefore a refusal of the way, whatever the descriptor took ([Refused]): the writer moves down to `O_DSYNC`
///   alone, which every thread writes through, rather than to forces.
/// - on Windows with `FILE_FLAG_NO_BUFFERING` and `FILE_FLAG_WRITE_THROUGH`, through the Windows API, only while the
///   module has native access: for that pair Windows documents that it also requests a write-through of the drive's
///   cache, and for `FILE_FLAG_WRITE_THROUGH` alone it does not. It adds that not all hardware supports the request,
///   and a force asks the device to empty its cache instead, so [Mode#AUTO] takes this way only on a device that
///   reports the request holds ([WriteCache#provesWriteThrough()]), and forces on any other. The device of a volume
///   is asked once for the life of the process where its answer rests on what the device is, forced unit access or a
///   cache that keeps its contents without power, or where it proves nothing, which only costs forces; an answer that
///   rests on a cache an administrator turned off, which a setting turns back on, is asked again by each store. The
///   JDK opens a channel with [StandardOpenOption#DSYNC] with `FILE_FLAG_WRITE_THROUGH` alone, and
///   `ExtendedOpenOption.DIRECT` leaves the handle the channel writes through buffered.
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
/// preparing a segment may ask which way is left beside it, so the round trip of asking the device holds no commit up,
/// and on Windows opens the first segment of a store just opened ([#opensEarly()]), which the first group then writes
/// through without the cold set-up of the way.
final class WriteThrough {
    private static final String OS = System.getProperty("os.name", "");
    /// The writes in a row taking at most half of the buffer after which it goes back to the size they took.
    private static final int SHRINK_AFTER = 256;

    /// How a store writes the groups that one write holds.
    enum Mode {
        /// Through, on the default file system, where the write holds what a force holds: `O_DIRECT` and `O_DSYNC` on
        /// Linux; unbuffered write-through on Windows while the module has native access, on a device that reports the
        /// request holds. Every other group, and every group elsewhere, is forced.
        ///
        /// On Windows the answer of a volume's device holds for the rest of the process when it rests on forced unit
        /// access or on a cache that keeps its contents without power, or when it proves nothing, so a change to the
        /// settings of the device's cache made while the process runs reaches the stores of the next process. Where
        /// only a write cache that is turned off proves it, each store asks again, so turning that cache back on
        /// reaches the next store opened.
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
        /// A channel opened with [StandardOpenOption#DSYNC] and `ExtendedOpenOption.DIRECT`, and where
        /// [Posix#serves(Path)] a descriptor the C library opened with `O_DIRECT` and `O_DSYNC` beside it, for the
        /// writes of virtual threads.
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
    // The writes in a row, up to SHRINK_AFTER, that took at most half of the buffer, and the most bytes one of them took
    private int smallWrites;
    private long smallPeak;
    // Whether the channel of a PosixHandle of this way took a write, so a write it refuses afterwards fails as any
    // other rather than as a refusal of the way; owned by the flush that holds the log writer
    private boolean channelProven;
    // What answers the proof without asking a device, null where it would have to; null if nothing does
    private final Supplier<Boolean> answered;
    // The handles opened and closed, for tests; guarded by the lock, as a handle may be opened beside the flush that
    // writes through it
    private int opens;
    private int closes;
    /// Allocates the buffer in place of the arena, by its size; for tests that refuse it.
    LongFunction<MemorySegment> allocatorForTests;
    /// Makes [#opensEarly()] answer `true` whatever the way, for tests of the opening on a file system other than
    /// Windows' own.
    volatile boolean opensEarlyForTests;
    /// Makes the channel of every [PosixHandle] of this way refuse its writes as one that never took a write does, as on
    /// a file system whose reported block size is larger than [LogWriter#ALIGNMENT], for tests.
    volatile boolean channelRefusesForTests;

    /// Creates the way to write through that starts at `kind`, or forces if it is `null`.
    ///
    /// @param proof what must answer `true` before the first segment is opened through, asked once, on the thread of
    ///              the flush that needs it; or `null` if nothing need
    WriteThrough(Kind kind, BooleanSupplier proof) {
        this(kind, proof, null);
    }

    /// Creates the way to write through that starts at `kind`, or forces if it is `null`.
    ///
    /// @param proof    what must answer `true` before the first segment is opened through, asked once; or `null` if
    ///                 nothing need
    /// @param answered what gives the answer of `proof` where it is known without asking a device, and `null` where
    ///                 it is not, for [#openAnswered(Path, BooleanSupplier)]; or `null` if nothing does
    WriteThrough(Kind kind, BooleanSupplier proof, Supplier<Boolean> answered) {
        this.lock = new Object();
        this.kind = kind;
        this.proof = proof;
        this.answered = answered;
    }

    /// {@return how a store in `directory` writes through in `mode`}
    ///
    /// The device is asked no sooner than a segment needs the answer, so a store that only opens and reads sends it
    /// nothing.
    static WriteThrough of(Mode mode, Path directory) {
        var kind = kindOf(mode, directory.getFileSystem() == FileSystems.getDefault());
        if (kind == Kind.UNBUFFERED && mode == Mode.AUTO) {
            var device = new DeviceProof(directory);
            return new WriteThrough(kind, device, device);
        }
        return new WriteThrough(kind, null);
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

    /// {@return the bytes of the buffer groups are copied into, or 0 before the first, for tests}
    long bufferSizeForTests() {
        var current = buffer;
        return current == null ? 0 : current.byteSize();
    }

    /// {@return the handles opened so far, for tests}
    int opensForTests() {
        synchronized (lock) {
            return opens;
        }
    }

    /// {@return the handles closed so far, for tests}
    int closesForTests() {
        synchronized (lock) {
            return closes;
        }
    }

    /// Counts a handle of this way closed, for tests.
    private void closed() {
        synchronized (lock) {
            closes++;
        }
    }

    /// {@return whether a segment is opened to write through before its first group needs it, on the thread that
    /// prepares segments: only unbuffered on Windows, where the first open of a store costs its first group more than
    /// a write through takes, and only where the opening can end in a handle, so a device that must prove the way has
    /// answered a store of this process before}
    ///
    /// Before the first unbuffered write of a store, the volume under it is named and the handle opened, cold native
    /// calls that held the first durable commit after an open for 1.5 to 2 milliseconds where the write itself takes a
    /// tenth of one; a force in its place took 1 to 4.5. A channel opened with `O_DIRECT` takes 0.04 to 0.2. The
    /// opening asks no device ([#openAnswered(Path, BooleanSupplier)]), so in a process that keeps no answer it could
    /// only name the volume and link the native calls, which the first group then does anyway to ask the device: about
    /// 330 classes loaded and 5 MB allocated beside the open, on a thread started for it, that end in no handle.
    boolean opensEarly() {
        if (opensEarlyForTests) {
            return true;
        }
        synchronized (lock) {
            if (kind != Kind.UNBUFFERED) {
                return false;
            }
            if (proof == null) {
                return true;
            }
            return answered instanceof DeviceProof device ? device.mayBeKnown() : answered != null;
        }
    }

    /// Opens the segment at `path` to write through as [#open(Path)] does, if no device need be asked first and the
    /// handle is still wanted once the volume is named.
    ///
    /// It runs beside the flush that holds the log writer: a proof still pending is answered only from what the
    /// process already knows of the device, and the round trip of a question to it is left to the first group.
    ///
    /// @param unwanted whether no one will take the handle, asked again after the volume is named, the slow part of
    ///                 the opening, so an opening given up meanwhile opens no file
    /// @return the handle, or `null` if no way is left, the device would have to be asked or the handle is unwanted
    /// @throws IOException if the segment cannot be opened the current way, native memory for its handle included
    Handle openAnswered(Path path, BooleanSupplier unwanted) throws IOException {
        BooleanSupplier pending;
        synchronized (lock) {
            if (kind == null) {
                return null;
            }
            pending = proof;
        }
        if (pending != null) {
            // Outside the lock: naming the volume is native calls, which a flush asking for the way must not wait on
            var answer = answered == null ? null : answered.get();
            if (answer == null) {
                return null;
            }
            synchronized (lock) {
                if (proof == pending) {
                    proof = null;
                    if (!answer) {
                        kind = null;
                    }
                }
            }
        }
        return !unwanted.getAsBoolean() && enabled() ? open(path) : null;
    }

    /// Opens the segment at `path` to write through, the current way.
    ///
    /// @throws IOException if it cannot be opened that way, native memory for its handle included
    /// @throws IllegalStateException if no way is left
    Handle open(Path path) throws IOException {
        if (!enabled()) {
            throw new IllegalStateException("The log forces its groups");
        }
        synchronized (lock) {
            opens++;
        }
        try {
            return switch (kind()) {
                case UNBUFFERED -> UnbufferedHandle.open(path, this);
                case DIRECT -> {
                    var channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, ExtendedOpenOption.DIRECT);
                    yield Posix.serves(path) ? new PosixHandle(channel, path, this) : new ChannelHandle(channel, this);
                }
                case DSYNC -> new ChannelHandle(FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.DSYNC), this);
            };
        } catch (OutOfMemoryError exception) {
            throw new IOException("A log segment could not be opened to write through", exception);
        }
    }

    /// {@return the buffer holding the first `length` bytes of `bytes`, a multiple of [LogWriter#ALIGNMENT], aligned to
    /// [LogWriter#ALIGNMENT]}
    ///
    /// It follows the groups written through rather than the log's output, which a writer gives back at every empty
    /// queue and takes back for any group larger than its smallest size: it grows when a group needs more, by a quarter
    /// at least, so groups of slowly growing sizes replace it a bounded number of times, and goes back to the size they
    /// took after [#SHRINK_AFTER] groups in a row that took at most half of it. Groups of sizes that alternate, or a
    /// large one now and then among small ones, so keep one buffer, and the native memory of replaced ones, which only
    /// the collector gives back, does not pile up. A smaller buffer native memory has no room for leaves the larger one
    /// in place.
    ///
    /// @throws IOException if native memory has no room for a buffer that holds the group, as under a limit of direct
    ///                     memory
    private MemorySegment copy(byte[] bytes, int length) throws IOException {
        var size = buffer == null ? 0 : buffer.byteSize();
        if (length > size) {
            reserve(length);
        } else if (2L * length <= size) {
            smallPeak = Math.max(smallPeak, length);
            if (++smallWrites >= SHRINK_AFTER) {
                var shrunk = Math.max(LogWriter.ALIGNMENT, smallPeak);
                smallWrites = 0;
                smallPeak = 0;
                try {
                    replace(shrunk);
                } catch (IOException _) {
                    // The larger buffer still holds the group
                }
            }
        } else {
            smallWrites = 0;
            smallPeak = 0;
        }
        MemorySegment.copy(bytes, 0, buffer, ValueLayout.JAVA_BYTE, 0, length);
        return buffer;
    }

    /// Grows the buffer to hold a group of `length` bytes as [#copy(byte\[\], int)] grows it, so a group's write that
    /// finds it grown only copies.
    ///
    /// @throws IOException if native memory has no room for a buffer that holds the group
    void reserve(int length) throws IOException {
        var size = buffer == null ? 0 : buffer.byteSize();
        if (length > size) {
            var grown = Math.max(length, size + (size >> 2));
            replace(Math.min(Math.max(length, LogWriter.OUTPUT), (grown + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT));
            smallWrites = 0;
            smallPeak = 0;
        }
    }

    /// Replaces the buffer with one of `size` bytes, the old one given back to the collector with its view.
    ///
    /// @throws IOException if native memory has no room for it
    private void replace(long size) throws IOException {
        try {
            var allocator = allocatorForTests;
            var replaced = allocator != null ? allocator.apply(size) : Arena.ofAuto().allocate(size, LogWriter.ALIGNMENT);
            bufferView = replaced.asByteBuffer();
            buffer = replaced;
        } catch (OutOfMemoryError exception) {
            throw new IOException("The buffer of a write through could not be allocated", exception);
        }
    }

    /// A segment opened to write through.
    sealed interface Handle permits ChannelHandle, UnbufferedHandle, PosixHandle {
        /// Writes the first `length` bytes of `bytes` at `position`, a multiple of [LogWriter#ALIGNMENT] as `length`
        /// is, all of them, durable once it returns.
        ///
        /// @throws java.nio.channels.ClosedByInterruptException if an interrupt closed the handle meanwhile, which
        ///         left some, all or none of the bytes written; only the write of a channel is interruptible, which a
        ///         [PosixHandle] makes for a platform thread and for a virtual one past [Posix#LIMIT]
        /// @throws ClosedChannelException if the handle was closed before
        /// @throws Refused if a [PosixHandle]'s channel failed the write before it took any of the way, which the way
        ///                 then cannot be held to, whatever its descriptor took
        /// @throws IOException if the write failed, which may have left some of the bytes written, or if the buffer the
        ///                     bytes are copied into could not be allocated
        void write(byte[] bytes, int length, long position) throws IOException;

        /// {@return whether the handle is open; an interrupt of a write closes a channel}
        boolean isOpen();

        /// Closes the handle.
        void close() throws IOException;
    }

    /// A write that a [PosixHandle]'s channel failed before the channel took any write of the way: the writer moves to
    /// the next way down, as after a refusal of its first write, even when the descriptor took the writes before.
    ///
    /// The descriptor and the channel check the alignment of a write against different blocks, so a write through the
    /// descriptor proves nothing of the channel, which the writes of platform threads, and of virtual threads past
    /// [Posix#LIMIT], take.
    static final class Refused extends IOException {
        Refused(IOException cause) {
            super("The channel of a log segment refused a write through", cause);
        }
    }

    /// A channel opened with [StandardOpenOption#DSYNC], and `ExtendedOpenOption.DIRECT` where no [PosixHandle]
    /// serves the segment: off Linux, without native access, on an architecture whose flags are not known, or on a
    /// file system other than the JDK's default one.
    ///
    /// The JDK lends a virtual thread's carrier to another for the write of such a channel, as it does for a force.
    static final class ChannelHandle implements Handle {
        private final FileChannel channel;
        private final WriteThrough owner;

        ChannelHandle(FileChannel channel, WriteThrough owner) {
            this.channel = channel;
            this.owner = owner;
        }

        @Override
        public void write(byte[] bytes, int length, long position) throws IOException {
            write(channel, owner, bytes, length, position);
        }

        /// Writes the first `length` bytes of `bytes` at `position` through `channel`, from the aligned buffer of
        /// `owner`.
        static void write(FileChannel channel, WriteThrough owner, byte[] bytes, int length, long position) throws IOException {
            owner.copy(bytes, length);
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
            if (channel.isOpen()) {
                owner.closed();
            }
            channel.close();
        }
    }

    /// A segment Linux opened with `O_DIRECT` and `O_DSYNC` twice: as a channel, and as a descriptor of the C library
    /// that virtual threads write through with `pwrite`.
    ///
    /// The channel's write lends a virtual thread's carrier to another, waking a carrier to take its place that finds
    /// nothing to do once the write returns. The write through the descriptor holds the carrier for its one round trip
    /// to the device instead, as an [UnbufferedHandle]'s does, and is not interruptible. A virtual thread writes through
    /// the descriptor while fewer than [Posix#LIMIT] others of the process do, and through the channel past them, so a
    /// slow or hung device holds a bounded number of carriers; a platform thread, which lends no carrier, always writes
    /// through the channel.
    ///
    /// The descriptor is opened at the first write a virtual thread makes once the process has [linked][Posix#linked()]
    /// the calls of the C library, which the first such write of the process starts on a thread of its own: until then
    /// the channel takes the writes. A descriptor that fails to open, or a write through it that fails, leaves the rest
    /// of the handle's writes to the channel, which writes the failed write's group again from its start, at the same
    /// offsets. A write the channel fails before it took one of the way's throws [Refused]. Only the flush that holds
    /// the log writer writes and closes a handle, so no write runs while its descriptor closes; a handle dropped without
    /// a close has its descriptor closed by [Libc#CLEANER], and no descriptor is closed twice, as a second close could
    /// close a file the number was given to since.
    static final class PosixHandle implements Handle {
        private static final int EINTR = 4;
        private final FileChannel channel;
        private final Path path;
        private final WriteThrough owner;
        // The descriptor, or -1 while none is open
        private int descriptor;
        // Whether the descriptor was opened once, so one that failed to open or to write is not opened again
        private boolean tried;
        // The call state of the descriptor's calls, and its close, which the cleanable runs at most once
        private MemorySegment state;
        private Descriptor closing;
        private Cleaner.Cleanable closer;
        private boolean open;
        // The writes of virtual threads through the channel, for tests
        private int channelWrites;

        PosixHandle(FileChannel channel, Path path, WriteThrough owner) {
            this.channel = channel;
            this.path = path;
            this.owner = owner;
            this.descriptor = -1;
            this.open = true;
        }

        @Override
        public void write(byte[] bytes, int length, long position) throws IOException {
            if (!open) {
                throw new ClosedChannelException();
            }
            if (Thread.currentThread().isVirtual()) {
                int writing;
                if (descriptor() && (writing = Posix.enter()) > 0) {
                    boolean written;
                    try {
                        var hook = Posix.writingForTests;
                        if (hook != null) {
                            hook.accept(writing);
                        }
                        written = writeDescriptor(owner.copy(bytes, length), length, position);
                    } finally {
                        Posix.exit();
                    }
                    if (written) {
                        return;
                    }
                }
                channelWrites++;
            }
            if (owner.channelRefusesForTests) {
                throw new Refused(new IOException("A channel that refuses every write, for tests"));
            }
            try {
                ChannelHandle.write(channel, owner, bytes, length, position);
            } catch (ClosedChannelException exception) {
                throw exception;
            } catch (IOException exception) {
                if (!owner.channelProven) {
                    throw new Refused(exception);
                }
                throw exception;
            }
            owner.channelProven = true;
        }

        /// {@return whether the descriptor is open, opening it the first time the calls are linked}
        private boolean descriptor() {
            if (descriptor >= 0) {
                return true;
            }
            if (tried || !Posix.linked()) {
                return false;
            }
            tried = true;
            openDescriptor();
            return descriptor >= 0;
        }

        /// Opens the descriptor and registers its close, leaving none open if either fails.
        private void openDescriptor() {
            var name = Posix.nameOf(path);
            if (name == null) {
                return;
            }
            try {
                var called = Arena.ofAuto().allocate(Libc.CALL_STATE);
                int opened;
                try (var arena = Arena.ofConfined()) {
                    // Zeroed, so the name ends with the terminator the C library looks for
                    var text = arena.allocate(name.length + 1L);
                    MemorySegment.copy(name, 0, text, ValueLayout.JAVA_BYTE, 0, name.length);
                    opened = (int) Libc.OPEN.invokeExact(called, text, Posix.FLAGS, 0);
                }
                if (opened < 0) {
                    return;
                }
                var close = new Descriptor(opened, called);
                try {
                    closer = Libc.CLEANER.register(this, close);
                } catch (Throwable _) {
                    close.run();
                    return;
                }
                state = called;
                closing = close;
                descriptor = opened;
            } catch (Throwable _) {
                // The channel takes the writes
            }
        }

        /// {@return whether the first `length` bytes of `source` were written at `position` through the descriptor; if
        /// not, the descriptor is closed, and the channel takes this group and every later one}
        private boolean writeDescriptor(MemorySegment source, int length, long position) {
            var done = 0L;
            while (done < length) {
                // A local of its own: a conditional passed straight to invokeExact would be typed Object, as its target
                MemorySegment from = done == 0 ? source : source.asSlice(done);
                long count;
                try {
                    count = (long) Libc.PWRITE.invokeExact(state, descriptor, from, length - done, position + done);
                } catch (Throwable _) {
                    count = 0;
                } finally {
                    // The cleaner closes the descriptor only once the handle is unreachable, never during its write
                    Reference.reachabilityFence(this);
                }
                if (count > 0) {
                    done += count;
                } else if (count == 0 || Libc.errno(state) != EINTR) {
                    // A regular file reports an interrupt only of a write that wrote nothing, so one is repeated whole
                    dropDescriptor();
                    return false;
                }
            }
            return true;
        }

        /// Closes the descriptor, whatever its close returns, for the channel to take the writes.
        private void dropDescriptor() {
            descriptor = -1;
            var cleanable = closer;
            closer = null;
            closing = null;
            if (cleanable != null) {
                cleanable.clean();
            }
        }

        /// {@return the descriptor, or -1 while none is open, for tests}
        int descriptorForTests() {
            return descriptor;
        }

        /// {@return the writes of virtual threads made through the channel, for tests}
        int channelWritesForTests() {
            return channelWrites;
        }

        @Override
        public boolean isOpen() {
            return open && channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            if (!open) {
                return;
            }
            open = false;
            owner.closed();
            var close = closing;
            try {
                dropDescriptor();
            } finally {
                channel.close();
            }
            if (close != null && close.result != 0) {
                throw new IOException("A log segment's descriptor could not be closed: errno " + close.errno);
            }
        }
    }

    /// The close of a [PosixHandle]'s descriptor, run at most once by its cleanable: by the handle's close, or by
    /// [Libc#CLEANER] once the handle is unreachable.
    ///
    /// It holds the number and its call state, never the handle, which would keep the handle reachable.
    private static final class Descriptor implements Runnable {
        private final int number;
        private final MemorySegment state;
        // What the close returned and the errno it left, read by the thread whose clean ran it
        private int result;
        private int errno;

        private Descriptor(int number, MemorySegment state) {
            this.number = number;
            this.state = state;
        }

        @Override
        public void run() {
            try {
                // Linux frees the number even when the close reports an error, so it is never repeated
                result = (int) Libc.CLOSE.invokeExact(state, number);
                if (result != 0) {
                    errno = Libc.errno(state);
                }
            } catch (Throwable _) {
                result = -1;
            }
        }
    }

    /// Where a [PosixHandle] serves a segment, the linking of the calls of the C library it makes, and the virtual
    /// threads of the process that hold their carriers in its writes.
    static final class Posix {
        private static final int O_WRONLY = 1;
        private static final int O_DSYNC = 0x1000;
        private static final int O_CLOEXEC = 0x8_0000;
        /// The flags the descriptors are opened with: `O_WRONLY`, `O_DSYNC`, `O_CLOEXEC` and the architecture's
        /// `O_DIRECT`, which is 0x4000 on x86-64 and 0x10000 on AArch64.
        static final int FLAGS;
        /// Whether the process can write through descriptors of the C library: Linux on x86-64 or AArch64, with native
        /// access, the JDK's own provider of the default file system, whose file names reach the C library in the
        /// encoding `sun.jnu.encoding` names, that encoding known, and a [#LIMIT] above zero.
        static final boolean ELIGIBLE;
        /// The most virtual threads of the process that write through descriptors at a time, [limited][#limitOf(int)]
        /// by the carriers of the scheduler.
        static final int LIMIT;
        // The encoding of file names
        private static final Charset NAMES;
        // The linking of the calls: not started, under way, done, failed
        private static final int UNLINKED = 0;
        private static final int LINKING = 1;
        private static final int LINKED = 2;
        private static final int FAILED = 3;
        private static final AtomicInteger LINKS = new AtomicInteger();
        // The virtual threads writing through descriptors
        private static final AtomicInteger WRITING = new AtomicInteger();
        /// Called by each virtual thread let write through its descriptor with the number now writing, itself included,
        /// before it writes; for tests.
        static volatile IntConsumer writingForTests;

        static {
            var direct = switch (System.getProperty("os.arch", "")) {
                case "amd64", "x86_64" -> 0x4000;
                case "aarch64" -> 0x1_0000;
                default -> 0;
            };
            Charset names;
            try {
                names = Charset.forName(System.getProperty("sun.jnu.encoding", ""));
            } catch (IllegalArgumentException _) {
                names = null;
            }
            NAMES = names;
            FLAGS = O_WRONLY | O_DSYNC | O_CLOEXEC | direct;
            var carriers = Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors());
            LIMIT = limitOf(carriers);
            ELIGIBLE = LIMIT > 0 && OS.equals("Linux") && direct != 0 && names != null
                    && WriteThrough.class.getModule().isNativeAccessEnabled()
                    && FileSystems.getDefault().provider().getClass().getName().equals("sun.nio.fs.LinuxFileSystemProvider");
        }

        private Posix() {
        }

        /// {@return the most virtual threads that write through descriptors at a time on a scheduler of `carriers`
        /// carriers: one fewer, so a device that takes long to answer leaves a carrier to the rest, and none with one
        /// carrier, which has no carrier to spare}
        static int limitOf(int carriers) {
            return Math.max(0, carriers - 1);
        }

        /// {@return whether a [PosixHandle] serves the segment at `path`: the process is [#ELIGIBLE] and `path` is of
        /// the default file system}
        static boolean serves(Path path) {
            return ELIGIBLE && path.getFileSystem() == FileSystems.getDefault();
        }

        /// {@return the bytes of the absolute name of `path` the C library opens the file by, or `null` if they cannot
        /// be had}
        ///
        /// A name whose text does not encode back into the bytes of the path, as one read from a directory in another
        /// encoding, would name another file, so it has none.
        static byte[] nameOf(Path path) {
            try {
                var absolute = path.toAbsolutePath();
                var text = absolute.toString();
                if (!Path.of(text).equals(absolute)) {
                    return null;
                }
                var encoded = NAMES.newEncoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .encode(CharBuffer.wrap(text));
                var bytes = new byte[encoded.remaining()];
                encoded.get(bytes);
                return bytes;
            } catch (InvalidPathException | CharacterCodingException | java.io.IOError _) {
                return null;
            }
        }

        /// {@return whether the calls are linked, starting to link them on a platform thread of their own if no thread
        /// has, so no commit waits the tens of milliseconds the first linking of the process takes}
        static boolean linked() {
            var links = LINKS.get();
            if (links == LINKED) {
                return true;
            }
            if (links == UNLINKED && LINKS.compareAndSet(UNLINKED, LINKING)) {
                try {
                    Thread.ofPlatform().daemon().name("ledger-link").start(Posix::link);
                } catch (RuntimeException | OutOfMemoryError _) {
                    LINKS.set(FAILED);
                }
            }
            return false;
        }

        /// Links the calls on the calling thread, and {@return whether they are linked}
        static boolean link() {
            boolean linked;
            try {
                linked = Libc.AVAILABLE;
            } catch (LinkageError _) {
                linked = false;
            }
            LINKS.set(linked ? LINKED : FAILED);
            return linked;
        }

        /// {@return the virtual threads now writing through descriptors, the caller included, or 0 if [#LIMIT] of them
        /// already do and the caller does not}
        static int enter() {
            var writing = WRITING.incrementAndGet();
            if (writing > LIMIT) {
                WRITING.decrementAndGet();
                return 0;
            }
            return writing;
        }

        /// Counts the end of a write through a descriptor that [#enter()] let start.
        static void exit() {
            WRITING.decrementAndGet();
        }
    }

    /// The calls of the C library a [PosixHandle] makes, linked when [Posix#link()] first uses the class, which it
    /// does only where the process is [eligible][Posix#ELIGIBLE].
    private static final class Libc {
        static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
        private static final VarHandle ERRNO = CALL_STATE.varHandle(MemoryLayout.PathElement.groupElement("errno"));
        /// Whether the calls are linked.
        static final boolean AVAILABLE;
        static final MethodHandle OPEN;
        static final MethodHandle PWRITE;
        static final MethodHandle CLOSE;
        /// Closes the descriptors of handles dropped without a close.
        static final Cleaner CLEANER;

        static {
            MethodHandle open = null;
            MethodHandle pwrite = null;
            MethodHandle close = null;
            Cleaner cleaner = null;
            try {
                var linker = Linker.nativeLinker();
                var library = linker.defaultLookup();
                var errno = Linker.Option.captureCallState("errno");
                var integer = ValueLayout.JAVA_INT;
                var wide = ValueLayout.JAVA_LONG;
                // open(const char *, int, ...): the mode, which only a creation reads, is the first variadic argument
                open = linker.downcallHandle(library.findOrThrow("open"),
                        FunctionDescriptor.of(integer, ValueLayout.ADDRESS, integer, integer), errno, Linker.Option.firstVariadicArg(2));
                pwrite = linker.downcallHandle(library.findOrThrow("pwrite"),
                        FunctionDescriptor.of(wide, integer, ValueLayout.ADDRESS, wide, wide), errno);
                close = linker.downcallHandle(library.findOrThrow("close"), FunctionDescriptor.of(integer, integer), errno);
                cleaner = Cleaner.create(Thread.ofVirtual().name("ledger-descriptor-cleaner").factory());
            } catch (RuntimeException | LinkageError | OutOfMemoryError _) {
                open = null;
            }
            AVAILABLE = open != null && pwrite != null && close != null && cleaner != null;
            OPEN = open;
            PWRITE = pwrite;
            CLOSE = close;
            CLEANER = cleaner;
        }

        private Libc() {
        }

        /// {@return the errno the last call that captured it left in `state`}
        static int errno(MemorySegment state) {
            return (int) ERRNO.get(state, 0L);
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
        public void write(byte[] bytes, int length, long position) throws IOException {
            var source = owner.copy(bytes, length);
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
            owner.closed();
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
        // GetFinalPathNameByHandleW: VOLUME_NAME_GUID, then the longest path Windows has and the room the name is
        // asked into first, in characters; the name of a volume takes 49 of them, its separator included
        private static final int VOLUME_NAME_GUID = 1;
        private static final int PATH_CHARS = 32_768;
        private static final int NAME_CHARS = 1_024;
        // The answers of the devices under this process's stores
        private static final Answers ANSWERS = new Answers(WriteCache::volumeOf, WriteCache::ofVolume);
        private static final int FILE_FLAG_BACKUP_SEMANTICS = 0x0200_0000;

        /// {@return whether a write through holds what a force holds on this device}
        ///
        /// It holds where the device honours forced unit access, which is what the request becomes, and where nothing
        /// volatile lies between a write and the medium. An unknown answer proves nothing.
        boolean provesWriteThrough() {
            return writeThrough == SUPPORTED || enabled == DISABLED || nonVolatile;
        }

        /// {@return whether this answer holds for the rest of the process}
        ///
        /// Forced unit access and a cache that keeps its contents without power are what the device is, and an answer
        /// that proves nothing only costs forces while it stands. A cache that is turned off is a setting an
        /// administrator can turn back on while the process runs, which would leave the writes through in a volatile
        /// cache.
        boolean lastsForProcess() {
            return writeThrough == SUPPORTED || nonVolatile || !provesWriteThrough();
        }

        /// {@return whether the device under `directory` reports that a write through holds; `false` if it cannot be
        /// asked, as on a network share}
        ///
        /// The device of a volume is asked once for the life of the process where its answer
        /// [lasts for it][#lastsForProcess()], and by each call otherwise.
        static boolean proves(Path directory) {
            return ANSWERS.proves(directory);
        }

        /// {@return what [#proves(Path)] answers for `directory`, where the process knows it without asking a device;
        /// `null` where a device would have to be asked}
        static Boolean provesIfKnown(Path directory) {
            return ANSWERS.provesIfKnown(directory);
        }

        /// {@return whether the process keeps the answer of any device, without which [#provesIfKnown(Path)] answers
        /// `null`; it names no volume}
        static boolean anyKnown() {
            return ANSWERS.anyKnown();
        }

        /// Forgets the answers of every volume, for tests that count the questions sent to devices.
        static void clearForTests() {
            ANSWERS.clear();
        }

        /// {@return the questions sent to devices so far, for tests}
        static int asksForTests() {
            return ANSWERS.asks();
        }

        /// {@return what the device of the volume that really holds `directory`, through its links and mount points,
        /// reports, or `null` if it cannot be asked or does not answer}
        ///
        /// It takes no privileges: both handles are opened for no access, and the query needs none. The answer may take
        /// a round trip to the device.
        static WriteCache of(Path directory) {
            var volume = volumeOf(directory);
            return volume == null ? null : ofVolume(volume);
        }

        /// {@return what the device of `volume`, a name such as `\\?\Volume{guid}`, reports, or `null` if it cannot be
        /// asked or does not answer}
        ///
        /// The answer may take a round trip to the device.
        static WriteCache ofVolume(String volume) {
            try (var arena = Arena.ofConfined()) {
                var state = arena.allocate(Kernel32.CALL_STATE);
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

        /// {@return the name of the volume that really holds `directory`, without the separator after it, or `null` if
        /// it has none or cannot be asked}
        ///
        /// It takes no privileges: the directory is opened for no access.
        static String volumeOf(Path directory) {
            try (var arena = Arena.ofConfined()) {
                var state = arena.allocate(Kernel32.CALL_STATE);
                var opened = (MemorySegment) Kernel32.CREATE_FILE.invokeExact(state,
                        arena.allocateFrom(UnbufferedHandle.win32Path(directory), StandardCharsets.UTF_16LE), 0, UnbufferedHandle.SHARE_ALL,
                        MemorySegment.NULL, UnbufferedHandle.OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, MemorySegment.NULL);
                if (opened.address() == -1L) {
                    return null;
                }
                try {
                    var capacity = NAME_CHARS;
                    var name = arena.allocate((long) capacity * Character.BYTES, Character.BYTES);
                    var length = (int) Kernel32.GET_FINAL_PATH.invokeExact(state, opened, name, capacity, VOLUME_NAME_GUID);
                    if (length >= capacity && length <= PATH_CHARS) {
                        // Too short for the name, whose characters with the terminator the length then counts
                        capacity = length;
                        name = arena.allocate((long) capacity * Character.BYTES, Character.BYTES);
                        length = (int) Kernel32.GET_FINAL_PATH.invokeExact(state, opened, name, capacity, VOLUME_NAME_GUID);
                    }
                    if (length <= 0 || length >= capacity) {
                        return null;
                    }
                    // Starts with the name: \\?\Volume{guid}\
                    var path = name.getString(0, StandardCharsets.UTF_16LE);
                    var end = path.indexOf('\\', "\\\\?\\".length());
                    return end < 0 ? path : path.substring(0, end);
                } finally {
                    var _ = (int) Kernel32.CLOSE_HANDLE.invokeExact(state, opened);
                }
            } catch (Throwable _) {
                // No volume, so the log forces, which needs none
                return null;
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

    /// The proof of the unbuffered way of a store in `directory`: what its device answers ([WriteCache#proves(Path)]),
    /// which the first group that writes through asks, and the answer the process already keeps
    /// ([WriteCache#provesIfKnown(Path)]), which the opening of the first segment beside the open takes.
    ///
    /// One named object serves as both, so a store links no lambda for either.
    ///
    /// @param directory the directory of the store's log
    private record DeviceProof(Path directory) implements BooleanSupplier, Supplier<Boolean> {
        @Override
        public boolean getAsBoolean() {
            return WriteCache.proves(directory);
        }

        @Override
        public Boolean get() {
            return WriteCache.provesIfKnown(directory);
        }

        /// {@return whether [#get()] may answer anything but `null`, as it does once the process keeps the answer of
        /// some device}
        boolean mayBeKnown() {
            return WriteCache.anyKnown();
        }
    }

    /// The answers of the devices under the stores of the process, by the name of their volume, each kept for the rest
    /// of the process if it [lasts for it][WriteCache#lastsForProcess()].
    ///
    /// No answer is kept for a volume that cannot be named or a device that does not answer, so a failure that passes
    /// is asked again by the next store rather than leaving it to forces. Two stores that ask beside each other may
    /// both send the question.
    static final class Answers {
        private final Function<Path, String> volumes;
        private final Function<String, WriteCache> devices;
        private final ConcurrentHashMap<String, WriteCache> answers;
        // The questions sent to devices, for tests
        private final AtomicInteger asks;

        /// Creates the answers that `volumes` names the volume of a directory for, or `null` for none, and that
        /// `devices` asks the device of a volume for, or `null` if it does not answer.
        Answers(Function<Path, String> volumes, Function<String, WriteCache> devices) {
            this.volumes = volumes;
            this.devices = devices;
            this.answers = new ConcurrentHashMap<>();
            this.asks = new AtomicInteger();
        }

        /// {@return whether the device under `directory` reports that a write through holds; `false` if it cannot be
        /// asked}
        boolean proves(Path directory) {
            var volume = volumes.apply(directory);
            if (volume == null) {
                return false;
            }
            var known = answers.get(volume);
            if (known != null) {
                return known.provesWriteThrough();
            }
            asks.incrementAndGet();
            var answer = devices.apply(volume);
            if (answer == null) {
                return false;
            }
            if (answer.lastsForProcess()) {
                answers.put(volume, answer);
            }
            return answer.provesWriteThrough();
        }

        /// {@return what [#proves(Path)] answers for `directory` where that sends no question to a device, `null` where
        /// it would}
        ///
        /// The volume is named only while some answer is kept: naming it takes no round trip to its device, but the
        /// first time in a process links the native calls that name it.
        Boolean provesIfKnown(Path directory) {
            if (answers.isEmpty()) {
                return null;
            }
            var volume = volumes.apply(directory);
            if (volume == null) {
                return false;
            }
            var known = answers.get(volume);
            return known == null ? null : known.provesWriteThrough();
        }

        /// {@return whether the answer of any device is kept}
        boolean anyKnown() {
            return !answers.isEmpty();
        }

        /// Forgets every answer.
        void clear() {
            answers.clear();
        }

        /// {@return the questions sent to devices so far}
        int asks() {
            return asks.get();
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

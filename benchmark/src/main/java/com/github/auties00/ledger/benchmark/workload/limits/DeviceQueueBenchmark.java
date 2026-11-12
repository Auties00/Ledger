package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.support.Directories;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
import com.sun.nio.file.ExtendedOpenOption;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/// What the device under the benchmark directory does with the blocks of one log group written as several requests
/// in flight at once, the question Ledger's chunked writes through rest on: a round writes a group of `blocks` blocks
/// at the next offset of a file whose blocks all exist, in `chunks` chunks issued together, and returns once every
/// chunk is durable.
///
/// - `through`: each chunk durable as it returns, as Ledger writes a group through: on Linux a channel opened with
///   `O_DIRECT` and `O_DSYNC`, each chunk on a platform thread of its own past the first; on Windows a handle opened with
///   `FILE_FLAG_NO_BUFFERING`, `FILE_FLAG_WRITE_THROUGH` and `FILE_FLAG_OVERLAPPED`, each chunk an overlapped write
///   with an event of its own.
/// - `forced`: the chunks past the caches, then one force: on Linux `O_DIRECT` alone and `fdatasync`, which is what
///   Ledger runs on a device without forced unit access; on Windows the same handle without `FILE_FLAG_WRITE_THROUGH`,
///   then `FlushFileBuffers`.
///
/// Chunks pay where `4x4` takes about as long as `1x1` and `16x4` less than `16x1`. `busy` runs a thread beside the
/// rounds that writes 1 MiB blocks into the cache of another file and forces it every 8 MiB, as a checkpoint and the
/// zeros of a new segment do beside the log, which shows whether the device keeps serving the chunks under load or a
/// limit of its requests a second throttles them. Linux runs print what the device's queue reports of forced unit
/// access and its write cache. A file system that refuses `O_DIRECT` runs the channel without it, and says so.
///
/// Not a Cobalt workload, and not an engine's: the device alone, as the floor is.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class DeviceQueueBenchmark {
    /// The bytes of a block, Ledger's alignment of a group.
    public static final int BLOCK = 4_096;
    /// The bytes of the file the rounds write into, cycling over it.
    public static final long FILE_BYTES = 64L << 20;
    private static final String OS = System.getProperty("os.name", "");
    private static final long SPIN_NANOS = 50_000;

    /// The blocks of a group and the chunks it is written in, as `<blocks>x<chunks>`.
    @Param({"1x1", "4x1", "4x4", "16x1", "16x4"})
    public String shape;

    /// How a chunk is made durable: `through` or `forced`.
    @Param({"through", "forced"})
    public String way;

    /// What runs beside the rounds: `idle`, nothing, or `busy`, a writer of the cache that forces as a checkpoint does.
    @Param({"idle", "busy"})
    public String background;

    private Path directory;
    private int blocks;
    private int chunks;
    private long next;
    private Writer writer;
    private Thread busy;
    private volatile boolean stopping;

    /// Creates the file with every block written and forced, opens it as `way` says and starts what `background` names.
    ///
    /// @throws IOException if the file cannot be written or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        var parts = shape.split("x");
        blocks = Integer.parseInt(parts[0]);
        chunks = Integer.parseInt(parts[1]);
        if (chunks < 1 || chunks > blocks) {
            throw new IllegalArgumentException("A shape needs 1 to <blocks> chunks: " + shape);
        }
        var through = switch (way) {
            case "through" -> true;
            case "forced" -> false;
            default -> throw new IllegalArgumentException("way must be through or forced: " + way);
        };
        directory = Stores.root().resolve("run").resolve("device-queue-" + UUID.randomUUID());
        Files.createDirectories(directory);
        var file = directory.resolve("log");
        fill(file, FILE_BYTES);
        writer = OS.startsWith("Windows") ? new OverlappedWriter(file, through, blocks) : new ChannelWriter(file, through, blocks, chunks);
        if (OS.equals("Linux")) {
            System.out.println("Device queue: " + LinuxQueue.describe(file) + ", " + writer.describe());
        } else {
            System.out.println("Device queue: " + writer.describe());
        }
        if (background.equals("busy")) {
            var other = directory.resolve("checkpoint");
            busy = Thread.ofPlatform().daemon().name("device-queue-busy").start(() -> checkpoint(other));
        } else if (!background.equals("idle")) {
            throw new IllegalArgumentException("background must be idle or busy: " + background);
        }
    }

    /// Stops the background writer, closes the file and deletes the directory.
    ///
    /// @throws IOException if the directory cannot be deleted
    @TearDown(Level.Trial)
    public void close() throws IOException {
        stopping = true;
        if (busy != null) {
            try {
                busy.join();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            writer.close();
        } finally {
            Directories.delete(directory);
        }
    }

    /// Writes one group at the next offset, in its chunks at once, and returns once it is durable.
    ///
    /// @throws IOException if a chunk fails
    @Benchmark
    public void round() throws IOException {
        var length = blocks * BLOCK;
        if (next + length > FILE_BYTES) {
            next = 0;
        }
        writer.write(next, length, chunks);
        next += length;
    }

    /// Writes zeros over `bytes` of `file` and forces them, so every round overwrites blocks that exist.
    private static void fill(Path file, long bytes) throws IOException {
        try (var channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            var zeros = ByteBuffer.allocateDirect(1 << 20);
            for (var at = 0L; at < bytes; at += zeros.capacity()) {
                zeros.clear();
                while (zeros.hasRemaining()) {
                    channel.write(zeros, at + zeros.position());
                }
            }
            channel.force(true);
        }
    }

    /// Writes 1 MiB blocks into the cache of `file`, 256 MiB over and over, forcing every 8 MiB, until the trial ends.
    private void checkpoint(Path file) {
        try (var channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            var block = ByteBuffer.allocateDirect(1 << 20);
            var at = 0L;
            while (!stopping) {
                block.clear();
                while (block.hasRemaining()) {
                    channel.write(block, (at % (256L << 20)) + block.position());
                }
                at += block.capacity();
                if (at % (8L << 20) == 0) {
                    channel.force(false);
                }
            }
        } catch (IOException exception) {
            throw new IllegalStateException("The background writer failed", exception);
        }
    }

    /// How a round's chunks reach the file.
    private sealed interface Writer permits ChannelWriter, OverlappedWriter {
        /// Writes `length` bytes at `position` in `chunks` chunks at once, durable once it returns.
        void write(long position, int length, int chunks) throws IOException;

        /// {@return how the file was opened}
        String describe();

        void close() throws IOException;
    }

    /// {@return where chunk `index` of `count` of a group of `length` bytes starts, a multiple of [#BLOCK]}
    private static int chunkStart(int length, int count, int index) {
        return (int) ((long) (length / BLOCK) * index / count) * BLOCK;
    }

    /// A channel of the JDK, with platform threads that write the chunks past the first.
    private static final class ChannelWriter implements Writer {
        private final FileChannel channel;
        private final boolean through;
        private final boolean direct;
        private final ByteBuffer[] views;
        private final Helper[] helpers;
        private final AtomicInteger remaining;
        // The thread of the round under way, which JMH may change between iterations
        private volatile Thread caller;

        ChannelWriter(Path file, boolean through, int blocks, int chunks) throws IOException {
            FileChannel opened;
            boolean direct;
            try {
                opened = through ? FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, ExtendedOpenOption.DIRECT)
                        : FileChannel.open(file, StandardOpenOption.WRITE, ExtendedOpenOption.DIRECT);
                direct = true;
            } catch (IOException | UnsupportedOperationException _) {
                opened = through ? FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)
                        : FileChannel.open(file, StandardOpenOption.WRITE);
                direct = false;
            }
            this.channel = opened;
            this.through = through;
            this.direct = direct;
            var buffer = Arena.ofAuto().allocate((long) blocks * BLOCK, BLOCK);
            buffer.fill((byte) 0x5A);
            this.views = new ByteBuffer[chunks];
            for (var index = 0; index < chunks; index++) {
                views[index] = buffer.asByteBuffer();
            }
            this.remaining = new AtomicInteger();
            this.helpers = new Helper[chunks - 1];
            for (var index = 0; index < helpers.length; index++) {
                helpers[index] = new Helper();
                Thread.ofPlatform().daemon().name("device-queue-chunk-" + (index + 1)).start(helpers[index]);
            }
        }

        @Override
        public void write(long position, int length, int chunks) throws IOException {
            caller = Thread.currentThread();
            remaining.set(chunks - 1);
            for (var index = 1; index < chunks; index++) {
                var start = chunkStart(length, chunks, index);
                helpers[index - 1].post(views[index].limit(chunkStart(length, chunks, index + 1)).position(start), position + start);
            }
            writeAll(views[0].limit(chunkStart(length, chunks, 1)).position(0), position);
            while (remaining.get() > 0) {
                LockSupport.park(this);
            }
            for (var helper : helpers) {
                var failure = helper.failure;
                if (failure != null) {
                    helper.failure = null;
                    throw failure;
                }
            }
            if (!through) {
                channel.force(false);
            }
        }

        private void writeAll(ByteBuffer view, long position) throws IOException {
            var at = position;
            while (view.hasRemaining()) {
                at += channel.write(view, at);
            }
        }

        @Override
        public String describe() {
            return (through ? "DSYNC" : "no DSYNC, then fdatasync") + (direct ? ", DIRECT" : ", DIRECT refused");
        }

        @Override
        public void close() throws IOException {
            for (var helper : helpers) {
                helper.stop();
            }
            channel.close();
        }

        /// A thread that writes one chunk a round, spinning for [#SPIN_NANOS] after a round before it parks.
        private final class Helper implements Runnable {
            // The rounds posted and the rounds written
            private final AtomicInteger posted = new AtomicInteger();
            private int written;
            private ByteBuffer view;
            private long position;
            volatile IOException failure;
            private volatile boolean stopped;
            private volatile Thread thread;

            void post(ByteBuffer view, long position) {
                this.view = view;
                this.position = position;
                posted.incrementAndGet();
                var current = thread;
                if (current != null) {
                    LockSupport.unpark(current);
                }
            }

            void stop() {
                stopped = true;
                var current = thread;
                if (current != null) {
                    LockSupport.unpark(current);
                }
            }

            @Override
            public void run() {
                thread = Thread.currentThread();
                var spinUntil = 0L;
                while (!stopped) {
                    if (posted.get() == written) {
                        if (System.nanoTime() < spinUntil) {
                            Thread.onSpinWait();
                        } else {
                            LockSupport.parkNanos(this, 1_000_000);
                        }
                        continue;
                    }
                    written++;
                    try {
                        writeAll(view, position);
                    } catch (IOException exception) {
                        failure = exception;
                    }
                    spinUntil = System.nanoTime() + SPIN_NANOS;
                    if (remaining.decrementAndGet() == 0) {
                        LockSupport.unpark(caller);
                    }
                }
            }
        }
    }

    /// A Windows handle opened unbuffered and overlapped, through the Windows API, as Ledger's log opens a segment.
    private static final class OverlappedWriter implements Writer {
        private static final int GENERIC_WRITE = 0x4000_0000;
        private static final int SHARE_ALL = 0x7;
        private static final int OPEN_EXISTING = 3;
        private static final int FILE_FLAG_WRITE_THROUGH = 0x8000_0000;
        private static final int FILE_FLAG_NO_BUFFERING = 0x2000_0000;
        private static final int FILE_FLAG_OVERLAPPED = 0x4000_0000;
        private static final int ERROR_IO_PENDING = 997;
        private static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
        private static final VarHandle LAST_ERROR = CALL_STATE.varHandle(MemoryLayout.PathElement.groupElement("GetLastError"));
        private static final MethodHandle CREATE_FILE;
        private static final MethodHandle WRITE_FILE;
        private static final MethodHandle GET_OVERLAPPED_RESULT;
        private static final MethodHandle CREATE_EVENT;
        private static final MethodHandle FLUSH_FILE_BUFFERS;
        private static final MethodHandle CLOSE_HANDLE;

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
            GET_OVERLAPPED_RESULT = linker.downcallHandle(library.findOrThrow("GetOverlappedResult"),
                    FunctionDescriptor.of(integer, address, address, address, integer), lastError);
            CREATE_EVENT = linker.downcallHandle(library.findOrThrow("CreateEventW"),
                    FunctionDescriptor.of(address, address, integer, integer, address), lastError);
            FLUSH_FILE_BUFFERS = linker.downcallHandle(library.findOrThrow("FlushFileBuffers"),
                    FunctionDescriptor.of(integer, address), lastError);
            CLOSE_HANDLE = linker.downcallHandle(library.findOrThrow("CloseHandle"), FunctionDescriptor.of(integer, address), lastError);
        }

        private final boolean through;
        private final MemorySegment state;
        private final MemorySegment handle;
        private final MemorySegment buffer;
        private final MemorySegment[] overlapped;
        private final MemorySegment[] events;
        private final MemorySegment written;

        OverlappedWriter(Path file, boolean through, int blocks) throws IOException {
            this.through = through;
            var arena = Arena.ofAuto();
            this.state = arena.allocate(CALL_STATE);
            this.buffer = arena.allocate((long) blocks * BLOCK, BLOCK);
            buffer.fill((byte) 0x5A);
            this.written = arena.allocate(ValueLayout.JAVA_INT);
            var name = arena.allocateFrom("\\\\?\\" + file.toAbsolutePath().normalize(), StandardCharsets.UTF_16LE);
            var flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED | (through ? FILE_FLAG_WRITE_THROUGH : 0);
            try {
                handle = (MemorySegment) CREATE_FILE.invokeExact(state, name, GENERIC_WRITE, SHARE_ALL, MemorySegment.NULL,
                        OPEN_EXISTING, flags, MemorySegment.NULL);
                if (handle.address() == -1L) {
                    throw new IOException("CreateFileW failed: error " + lastError());
                }
                overlapped = new MemorySegment[blocks];
                events = new MemorySegment[blocks];
                for (var index = 0; index < blocks; index++) {
                    overlapped[index] = arena.allocate(32, Long.BYTES);
                    events[index] = (MemorySegment) CREATE_EVENT.invokeExact(state, MemorySegment.NULL, 1, 0, MemorySegment.NULL);
                    if (events[index].address() == 0) {
                        throw new IOException("CreateEventW failed: error " + lastError());
                    }
                }
            } catch (IOException exception) {
                throw exception;
            } catch (Throwable throwable) {
                throw new IOException("The file could not be opened", throwable);
            }
        }

        private int lastError() {
            return (int) LAST_ERROR.get(state, 0L);
        }

        @Override
        public void write(long position, int length, int chunks) throws IOException {
            try {
                for (var index = 0; index < chunks; index++) {
                    var start = chunkStart(length, chunks, index);
                    var end = chunkStart(length, chunks, index + 1);
                    var at = position + start;
                    var block = overlapped[index];
                    block.fill((byte) 0);
                    block.set(ValueLayout.JAVA_INT, 16, (int) at);
                    block.set(ValueLayout.JAVA_INT, 20, (int) (at >>> 32));
                    block.set(ValueLayout.ADDRESS, 24, events[index]);
                    var issued = (int) WRITE_FILE.invokeExact(state, handle, buffer.asSlice(start, end - start), end - start,
                            MemorySegment.NULL, block);
                    if (issued == 0 && lastError() != ERROR_IO_PENDING) {
                        throw new IOException("WriteFile failed: error " + lastError());
                    }
                }
                for (var index = 0; index < chunks; index++) {
                    var done = (int) GET_OVERLAPPED_RESULT.invokeExact(state, handle, overlapped[index], written, 1);
                    var expected = chunkStart(length, chunks, index + 1) - chunkStart(length, chunks, index);
                    if (done == 0 || written.get(ValueLayout.JAVA_INT, 0) != expected) {
                        throw new IOException("A chunk failed or was cut short: error " + lastError());
                    }
                }
                if (!through) {
                    var flushed = (int) FLUSH_FILE_BUFFERS.invokeExact(state, handle);
                    if (flushed == 0) {
                        throw new IOException("FlushFileBuffers failed: error " + lastError());
                    }
                }
            } catch (IOException exception) {
                throw exception;
            } catch (Throwable throwable) {
                throw new IOException("A chunk could not be written", throwable);
            }
        }

        @Override
        public String describe() {
            return "unbuffered, overlapped" + (through ? ", write-through" : ", then FlushFileBuffers");
        }

        @Override
        public void close() throws IOException {
            try {
                for (var event : events) {
                    var _ = (int) CLOSE_HANDLE.invokeExact(state, event);
                }
                var _ = (int) CLOSE_HANDLE.invokeExact(state, handle);
            } catch (Throwable throwable) {
                throw new IOException("The file could not be closed", throwable);
            }
        }
    }

    /// What the queue of a Linux block device reports, from `/sys/dev/block`.
    private static final class LinuxQueue {
        private LinuxQueue() {
        }

        /// {@return what the queue of the device holding `file` reports of forced unit access, its write cache, its
        /// requests and whether it rotates}
        static String describe(Path file) {
            try {
                var device = (long) Files.getAttribute(file, "unix:dev");
                var major = ((device >>> 8) & 0xFFFL) | ((device >>> 32) & 0xFFFF_F000L);
                var minor = (device & 0xFFL) | ((device >>> 12) & 0xFFFF_FF00L);
                var block = Path.of("/sys/dev/block/" + major + ":" + minor);
                if (!Files.exists(block)) {
                    return "device " + major + ":" + minor + " has no queue";
                }
                var real = block.toRealPath();
                var queue = Files.isDirectory(real.resolve("queue")) ? real.resolve("queue") : real.getParent().resolve("queue");
                return real.getFileName() + " fua " + read(queue, "fua") + ", write_cache " + read(queue, "write_cache")
                        + ", nr_requests " + read(queue, "nr_requests") + ", rotational " + read(queue, "rotational")
                        + ", max_sectors_kb " + read(queue, "max_sectors_kb");
            } catch (IOException | RuntimeException exception) {
                return "the queue could not be read: " + exception;
            }
        }

        private static String read(Path queue, String name) {
            try {
                return Files.readString(queue.resolve(name)).trim();
            } catch (IOException _) {
                return "?";
            }
        }
    }
}

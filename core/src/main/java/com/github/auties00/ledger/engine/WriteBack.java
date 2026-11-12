package com.github.auties00.ledger.engine;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.UnaryOperator;

/// The early writeback of the file of large values: the pages of a unit start their way to the device while the rest
/// of the unit is still sealed, so the force of its commit finds most of them written.
///
/// A unit of megabytes is sealed and written a part of [#PART] bytes at a time; without this, the kernel holds every
/// part dirty in its cache and the device starts on the unit only when the log's group forces the file, after the
/// last part is sealed. After each part but a unit's last, the whole pages written so far and not yet started are
/// handed to `sync_file_range` with `SYNC_FILE_RANGE_WRITE`, which queues them for writeback and returns: it neither
/// waits for the device nor flushes its cache, so it is a hint, and the force of the group stays what makes the unit
/// durable. A writeback error the hint meets is reported by that force, through the channel the force runs on. A unit
/// written in a single part is left whole to the force, which writes it in one with the units beside it.
///
/// The calls run on a platform thread of the instance's own, started with its first range, through a descriptor of
/// the file it opens read-only: a call the kernel makes wait for a full request queue or the journal holds neither
/// the sealing thread nor a carrier of virtual threads. That thread is the only user of the descriptor and closes it
/// as it stops, so no call can reach a number the process has given to another file.
///
/// It is active only on Linux, for a file of the default file system, while the module has native access
/// (`--enable-native-access`) and unless the system property `ledger.blobs.writeBack` is `off`; elsewhere every
/// method does nothing, and the units are written in parts of the whole buffer, as the force alone needs. A call that
/// fails turns the instance off for the rest of the open.
///
/// It is safe for concurrent use.
final class WriteBack implements AutoCloseable {
    /// The bytes of a unit sealed between two writebacks: four of the cipher's slices.
    static final int PART = 256 << 10;

    // The instance of every store where the writeback is off
    private static final WriteBack OFF = new WriteBack(null, null, Long.MAX_VALUE);
    // Stops the thread of the calls
    private static final Range STOP = new Range(-1, -1);
    // The architectures whose open flags are the generic ones of Linux, which Libc passes
    private static final Set<String> ARCHITECTURES = Set.of("amd64", "x86_64", "aarch64", "riscv64");

    /// Replaces the calls an instance makes, or the `null` of a platform without them, before it is created; for tests.
    static volatile UnaryOperator<Calls> callsForTests;

    private final Path path;
    private final Calls calls;
    // The size of the kernel's pages, to which every range is aligned
    private final long pageSize;
    // The ranges handed over and not yet passed to the kernel
    private final LinkedBlockingQueue<Range> ranges;
    // Guards the start and the stop of the thread
    private final Object lock;
    // The thread of the calls, or null before the first range; guarded by the lock
    private Thread thread;
    // Whether the instance was closed; guarded by the lock
    private boolean closed;
    // Whether a call failed, or the instance was closed, so no range is taken any more
    private volatile boolean off;
    // The ranges passed to the kernel, for tests
    private volatile long passed;

    private WriteBack(Path path, Calls calls, long pageSize) {
        this.path = path;
        this.calls = calls;
        this.pageSize = pageSize;
        this.ranges = new LinkedBlockingQueue<>();
        this.lock = new Object();
        this.off = calls == null;
    }

    /// {@return the writeback of the file at `path`, active only where the kernel takes it}
    static WriteBack of(Path path) {
        var calls = enabled(path) ? Libc.calls() : null;
        var replaced = callsForTests;
        if (replaced != null) {
            calls = replaced.apply(calls);
        }
        if (calls == null) {
            return OFF;
        }
        var pageSize = calls.pageSize();
        if (pageSize < PageFormat.SIZE || Long.bitCount(pageSize) != 1 || pageSize > PART) {
            return OFF;
        }
        return new WriteBack(path, calls, pageSize);
    }

    /// {@return whether the platform, the file system and the module's native access let `path` be written back early}
    private static boolean enabled(Path path) {
        var setting = System.getProperty("ledger.blobs.writeBack");
        if (setting != null && setting.trim().toLowerCase(Locale.ROOT).equals("off")) {
            return false;
        }
        return System.getProperty("os.name", "").equals("Linux")
                && ARCHITECTURES.contains(System.getProperty("os.arch", ""))
                && path.getFileSystem() == FileSystems.getDefault()
                && WriteBack.class.getModule().isNativeAccessEnabled();
    }

    /// {@return whether units are written back as they are sealed, and so are to be written in parts of [#PART] bytes}
    boolean active() {
        return !off;
    }

    /// {@return the writeback of the unit between the file offsets `start` and `end`, or `null` while the instance is
    /// not active}
    ///
    /// @param start the offset of the unit's first page
    /// @param end the offset past its last page
    Unit unit(long start, long end) {
        return off ? null : new Unit(start, end);
    }

    /// The writeback of one unit, used by the thread that writes it.
    final class Unit {
        // The offset of the unit's first page of the kernel's: a page that the unit shares with the one before it may
        // still be written by another thread, which a writeback started under it would make wait
        private final long first;
        private final long end;
        // The offset below which the unit's pages were handed over
        private long started;

        private Unit(long start, long end) {
            this.first = alignUp(start);
            this.end = end;
            this.started = first;
        }

        /// Hands over the pages written since the last call, now that the unit's bytes below `at` are in the file.
        ///
        /// The page `at` falls in is left for the next call, since the next part writes it again; once `at` is the
        /// unit's end, the last pages are handed over only if earlier ones were, so a unit of one part is left whole to
        /// the force.
        void written(long at) {
            long to;
            if (at < end) {
                to = alignDown(at);
            } else if (started > first) {
                to = alignDown(end);
            } else {
                return;
            }
            if (to > started) {
                submit(started, to);
                started = to;
            }
        }
    }

    /// Queues the range from `from` to `to` for the thread of the calls, starting it if it is the first.
    private void submit(long from, long to) {
        synchronized (lock) {
            if (closed || off) {
                return;
            }
            if (thread == null) {
                thread = Thread.ofPlatform().daemon().name("ledger-blob-writeback").unstarted(this::run);
                thread.start();
            }
        }
        // A range a close stops before it is taken is left in the queue, which no thread reads any more
        ranges.offer(new Range(from, to));
    }

    /// Opens the file and passes the ranges to the kernel, merging those that follow one another, until the instance is
    /// closed or a call fails; then closes the file.
    private void run() {
        var fd = -1;
        try {
            fd = calls.open(path);
            if (fd < 0) {
                return;
            }
            // The range taken from the queue that did not follow the last one merged, passed in the next call
            Range pending = null;
            while (true) {
                var range = pending != null ? pending : take();
                pending = null;
                if (range == STOP) {
                    return;
                }
                var from = range.from();
                var to = range.to();
                // The ranges are merged as they are taken, never peeked at first: a close may clear the queue and queue
                // STOP between a look and a take, and a STOP taken here stops the thread before another call
                Range next;
                while ((next = ranges.poll()) != null) {
                    if (next == STOP || next.from() != to) {
                        pending = next;
                        break;
                    }
                    to = next.to();
                }
                // A close drops the ranges merged and not yet passed, as it drops those still queued
                if (off) {
                    return;
                }
                if (calls.syncRange(fd, from, to - from) != 0) {
                    return;
                }
                passed++;
            }
        } catch (Throwable _) {
            // The writeback is only a hint: the force of the group writes what it did not start, and reports what fails
        } finally {
            off = true;
            ranges.clear();
            if (fd >= 0) {
                try {
                    calls.close(fd);
                } catch (Throwable _) {
                    // The descriptor was only read
                }
            }
        }
    }

    /// {@return the next range, waiting for one}
    private Range take() {
        while (true) {
            try {
                return ranges.take();
            } catch (InterruptedException _) {
                // Only the instance stops its thread, through STOP
            }
        }
    }

    /// {@return `offset` rounded down to a page of the kernel's}
    private long alignDown(long offset) {
        return offset & -pageSize;
    }

    /// {@return `offset` rounded up to a page of the kernel's}
    private long alignUp(long offset) {
        return (offset + pageSize - 1) & -pageSize;
    }

    /// {@return the ranges passed to the kernel since the instance was created, merged ones counting once; for tests}
    long passedForTests() {
        return passed;
    }

    /// Stops the thread of the calls once the call it is in returns, if any, dropping the ranges still queued, and
    /// waits for it to close the file.
    @Override
    public void close() {
        Thread stopping;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            off = true;
            stopping = thread;
        }
        if (stopping == null) {
            return;
        }
        ranges.clear();
        ranges.offer(STOP);
        var interrupted = false;
        while (true) {
            try {
                stopping.join();
                break;
            } catch (InterruptedException _) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// A range of the file, from the offset `from` to the offset `to`.
    private record Range(long from, long to) {
    }

    /// The calls to the kernel an instance makes.
    interface Calls {
        /// {@return a descriptor of the file at `path` opened read-only, or a negative number if it cannot be opened}
        int open(Path path) throws Throwable;

        /// {@return the size of the kernel's pages, in bytes}
        long pageSize();

        /// Starts the writeback of the dirty pages of `fd` from `offset` for `length` bytes, without waiting for it.
        ///
        /// @return 0, or a negative number if the call failed
        int syncRange(int fd, long offset, long length) throws Throwable;

        /// Closes `fd`.
        void close(int fd) throws Throwable;
    }

    /// The functions of the C library the calls of Linux use, looked up the first time an instance needs them.
    private static final class Libc implements Calls {
        private static final int O_RDONLY = 0;
        private static final int O_CLOEXEC = 0x8_0000;
        private static final int SC_PAGESIZE = 30;
        private static final int SYNC_FILE_RANGE_WRITE = 2;
        // The calls, or null if the C library lacks one of them
        private static final Libc CALLS = bind();

        private final MethodHandle open;
        private final MethodHandle syncFileRange;
        private final MethodHandle close;
        private final long pageSize;

        private Libc(MethodHandle open, MethodHandle syncFileRange, MethodHandle close, long pageSize) {
            this.open = open;
            this.syncFileRange = syncFileRange;
            this.close = close;
            this.pageSize = pageSize;
        }

        /// {@return the calls of the C library, or `null` if it lacks one of them}
        static Calls calls() {
            return CALLS;
        }

        private static Libc bind() {
            try {
                var linker = Linker.nativeLinker();
                var library = linker.defaultLookup();
                var integer = ValueLayout.JAVA_INT;
                var size = ValueLayout.JAVA_LONG;
                // open(const char *, int, ...): the mode is the first variadic argument
                var open = linker.downcallHandle(library.findOrThrow("open"),
                        FunctionDescriptor.of(integer, ValueLayout.ADDRESS, integer, integer), Linker.Option.firstVariadicArg(2));
                var syncFileRange = linker.downcallHandle(library.findOrThrow("sync_file_range"),
                        FunctionDescriptor.of(integer, integer, size, size, integer));
                var close = linker.downcallHandle(library.findOrThrow("close"), FunctionDescriptor.of(integer, integer));
                var sysconf = linker.downcallHandle(library.findOrThrow("sysconf"), FunctionDescriptor.of(size, integer));
                var pageSize = (long) sysconf.invokeExact(SC_PAGESIZE);
                return new Libc(open, syncFileRange, close, pageSize > 0 ? pageSize : PageFormat.SIZE);
            } catch (Throwable _) {
                return null;
            }
        }

        @Override
        public int open(Path path) throws Throwable {
            try (var arena = Arena.ofConfined()) {
                var name = arena.allocateFrom(path.toAbsolutePath().toString());
                return (int) open.invokeExact(name, O_RDONLY | O_CLOEXEC, 0);
            }
        }

        @Override
        public long pageSize() {
            return pageSize;
        }

        @Override
        public int syncRange(int fd, long offset, long length) throws Throwable {
            return (int) syncFileRange.invokeExact(fd, offset, length, SYNC_FILE_RANGE_WRITE);
        }

        @Override
        public void close(int fd) throws Throwable {
            var _ = (int) close.invokeExact(fd);
        }
    }
}

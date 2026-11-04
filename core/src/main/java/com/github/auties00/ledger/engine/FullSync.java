package com.github.auties00.ledger.engine;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/// Forces that reach stable storage on macOS.
///
/// The JDK implements [FileChannel#force(boolean)] with `fsync` there, which leaves the data in the drive's own cache
/// for a loss of power to take; `fcntl(F_FULLFSYNC)` empties that cache too, as LMDB and RocksDB call it. A force goes
/// through it when the store's module has native access, which the application grants with
/// `--enable-native-access=com.github.auties00.ledger`; elsewhere, and without native access, a force is the
/// channel's own, and on macOS a durable commit then survives a crash of the system but not every loss of power.
final class FullSync {
    private static final boolean MAC_OS = System.getProperty("os.name", "").startsWith("Mac");
    private static final int O_RDONLY = 0;
    private static final int F_FULLFSYNC = 51;

    private FullSync() {
    }

    /// {@return whether a force can empty the drive's cache for a file of `path`}
    static boolean available(Path path) {
        return MAC_OS && path.getFileSystem() == FileSystems.getDefault()
                && FullSync.class.getModule().isNativeAccessEnabled() && Calls.LINKED;
    }

    /// {@return a handle that forces `path` to stable storage, or `null` if forcing its channel is all there is}
    static Handle open(Path path) throws IOException {
        if (!available(path)) {
            return null;
        }
        try (var arena = Arena.ofConfined()) {
            var state = arena.allocate(Calls.STATE);
            var descriptor = (int) Calls.OPEN.invokeExact(state, arena.allocateFrom(path.toString()), O_RDONLY);
            if (descriptor < 0) {
                throw new IOException("Cannot open " + path + " to force it: errno " + Calls.errno(state));
            }
            return new Handle(descriptor);
        } catch (IOException | RuntimeException | Error exception) {
            throw exception;
        } catch (Throwable throwable) {
            throw new IOException("Cannot open " + path + " to force it", throwable);
        }
    }

    /// Forces `channel`, the channel of `path`, to stable storage, through a full sync where there is one.
    static void force(FileChannel channel, Path path, boolean metadata) throws IOException {
        try (var handle = open(path)) {
            if (handle == null) {
                channel.force(metadata);
            } else {
                handle.force(channel, metadata);
            }
        }
    }

    /// A file kept open to empty the drive's cache for it.
    static final class Handle implements AutoCloseable {
        private final int descriptor;
        private boolean closed;

        private Handle(int descriptor) {
            this.descriptor = descriptor;
        }

        /// Forces the file to stable storage; a file system that does not support it, such as a network one, gets
        /// the force of `channel` instead.
        void force(FileChannel channel, boolean metadata) throws IOException {
            try (var arena = Arena.ofConfined()) {
                var state = arena.allocate(Calls.STATE);
                if ((int) Calls.FCNTL.invokeExact(state, descriptor, F_FULLFSYNC) == -1) {
                    channel.force(metadata);
                }
            } catch (IOException | RuntimeException | Error exception) {
                throw exception;
            } catch (Throwable throwable) {
                throw new IOException("A full sync failed", throwable);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                var _ = (int) Calls.CLOSE.invokeExact(descriptor);
            } catch (Throwable _) {
                // The descriptor was only read: closing it cannot lose data
            }
        }
    }

    /// The native calls, linked only when a full sync is possible, since linking is restricted.
    private static final class Calls {
        static final MemoryLayout STATE = Linker.Option.captureStateLayout();
        static final MethodHandle OPEN;
        static final MethodHandle FCNTL;
        static final MethodHandle CLOSE;
        static final boolean LINKED;

        static {
            MethodHandle open = null;
            MethodHandle fcntl = null;
            MethodHandle close = null;
            var linked = false;
            if (MAC_OS && FullSync.class.getModule().isNativeAccessEnabled()) {
                try {
                    var linker = Linker.nativeLinker();
                    var lookup = linker.defaultLookup();
                    var errno = Linker.Option.captureCallState("errno");
                    // Variadic in C; called with their fixed arguments only, which every ABI passes the same way
                    open = linker.downcallHandle(lookup.find("open").orElseThrow(),
                            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT), errno);
                    fcntl = linker.downcallHandle(lookup.find("fcntl").orElseThrow(),
                            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT), errno);
                    close = linker.downcallHandle(lookup.find("close").orElseThrow(),
                            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
                    linked = true;
                } catch (RuntimeException _) {
                    // Without the calls, a force is the channel's own
                }
            }
            OPEN = open;
            FCNTL = fcntl;
            CLOSE = close;
            LINKED = linked;
        }

        static int errno(java.lang.foreign.MemorySegment state) {
            return state.get(ValueLayout.JAVA_INT, STATE.byteOffset(MemoryLayout.PathElement.groupElement("errno")));
        }

        private Calls() {
        }
    }
}

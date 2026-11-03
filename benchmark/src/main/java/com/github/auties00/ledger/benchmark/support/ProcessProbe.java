package com.github.auties00.ledger.benchmark.support;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/// Reads the I/O and memory counters the OS keeps for this process, which see the native engines too.
///
/// What "bytes written" counts differs by OS, so compare engines on one OS, not one OS with another:
///
/// | OS | Bytes written | Resident memory |
/// |---|---|---|
/// | Windows | `WriteTransferCount` of `GetProcessIoCounters`: every byte passed to a write call | `WorkingSetSize` |
/// | Linux | `wchar` of `/proc/self/io`: every byte passed to a write call | `VmRSS` |
/// | macOS | `ri_diskio_byteswritten` of `proc_pid_rusage`: bytes the process caused to be written to disk | `ri_resident_size` |
///
/// Writes through a memory map are counted only on macOS.
public final class ProcessProbe {
    private static final Backend BACKEND = Backend.detect();

    private ProcessProbe() {
    }

    /// {@return the bytes this process has written since it started, or -1 when the OS does not say}
    public static long bytesWritten() {
        return BACKEND.bytesWritten();
    }

    /// {@return the memory of this process resident in RAM, or -1 when the OS does not say}
    public static long residentBytes() {
        return BACKEND.residentBytes();
    }

    private sealed interface Backend {
        long bytesWritten();

        long residentBytes();

        static Backend detect() {
            var os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
            try {
                if (os.startsWith("windows")) {
                    return new Windows();
                }
                if (os.startsWith("mac")) {
                    return new MacOs();
                }
                if (os.startsWith("linux")) {
                    return new Linux();
                }
            } catch (Throwable throwable) {
                System.err.println("process probe unavailable: " + throwable);
            }
            return new Unsupported();
        }
    }

    private record Unsupported() implements Backend {
        @Override
        public long bytesWritten() {
            return -1;
        }

        @Override
        public long residentBytes() {
            return -1;
        }
    }

    private record Windows(MemorySegment process, MethodHandle ioCounters, MethodHandle memoryInfo) implements Backend {
        // IO_COUNTERS: six ULONGLONG; WriteTransferCount is the fifth
        private static final long IO_COUNTERS_SIZE = 48;
        private static final long WRITE_TRANSFER_COUNT = 32;
        // PROCESS_MEMORY_COUNTERS on 64-bit: cb, PageFaultCount, then SIZE_T fields; WorkingSetSize is the second
        private static final int MEMORY_COUNTERS_SIZE = 72;
        private static final long WORKING_SET_SIZE = 16;

        Windows() throws Throwable {
            this(lookup());
        }

        private Windows(MethodHandle[] handles) throws Throwable {
            this((MemorySegment) handles[0].invokeExact(), handles[1], handles[2]);
        }

        private static MethodHandle[] lookup() {
            var linker = Linker.nativeLinker();
            var kernel32 = SymbolLookup.libraryLookup("kernel32", Arena.global());
            return new MethodHandle[]{
                    linker.downcallHandle(kernel32.findOrThrow("GetCurrentProcess"), FunctionDescriptor.of(ValueLayout.ADDRESS)),
                    linker.downcallHandle(kernel32.findOrThrow("GetProcessIoCounters"),
                            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS)),
                    linker.downcallHandle(kernel32.findOrThrow("K32GetProcessMemoryInfo"),
                            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT))
            };
        }

        @Override
        public long bytesWritten() {
            try (var arena = Arena.ofConfined()) {
                var counters = arena.allocate(IO_COUNTERS_SIZE, 8);
                var succeeded = (int) ioCounters.invokeExact(process, counters);
                return succeeded != 0 ? counters.get(ValueLayout.JAVA_LONG, WRITE_TRANSFER_COUNT) : -1;
            } catch (Throwable throwable) {
                return -1;
            }
        }

        @Override
        public long residentBytes() {
            try (var arena = Arena.ofConfined()) {
                var counters = arena.allocate(MEMORY_COUNTERS_SIZE, 8);
                counters.set(ValueLayout.JAVA_INT, 0, MEMORY_COUNTERS_SIZE);
                var succeeded = (int) memoryInfo.invokeExact(process, counters, MEMORY_COUNTERS_SIZE);
                return succeeded != 0 ? counters.get(ValueLayout.JAVA_LONG, WORKING_SET_SIZE) : -1;
            } catch (Throwable throwable) {
                return -1;
            }
        }
    }

    private record MacOs(MethodHandle rusage, int pid) implements Backend {
        private static final int RUSAGE_INFO_V2 = 2;
        // rusage_info_v2: a 16-byte uuid, then uint64 fields
        private static final long RESIDENT_SIZE = 16 + 6 * 8;
        private static final long DISK_BYTES_WRITTEN = 16 + 17 * 8;

        MacOs() {
            this(Linker.nativeLinker().downcallHandle(Linker.nativeLinker().defaultLookup().findOrThrow("proc_pid_rusage"),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS)),
                    Math.toIntExact(ProcessHandle.current().pid()));
        }

        private long read(long offset) {
            try (var arena = Arena.ofConfined()) {
                var info = arena.allocate(512, 8);
                var result = (int) rusage.invokeExact(pid, RUSAGE_INFO_V2, info);
                return result == 0 ? info.get(ValueLayout.JAVA_LONG, offset) : -1;
            } catch (Throwable throwable) {
                return -1;
            }
        }

        @Override
        public long bytesWritten() {
            return read(DISK_BYTES_WRITTEN);
        }

        @Override
        public long residentBytes() {
            return read(RESIDENT_SIZE);
        }
    }

    private record Linux() implements Backend {
        private static long field(Path file, String name, long scale) {
            try {
                for (var line : Files.readAllLines(file)) {
                    if (line.startsWith(name)) {
                        var digits = line.substring(name.length()).replaceAll("[^0-9]", "");
                        return Long.parseLong(digits) * scale;
                    }
                }
                return -1;
            } catch (IOException | NumberFormatException exception) {
                return -1;
            }
        }

        @Override
        public long bytesWritten() {
            return field(Path.of("/proc/self/io"), "wchar:", 1);
        }

        @Override
        public long residentBytes() {
            return field(Path.of("/proc/self/status"), "VmRSS:", 1024);
        }
    }
}

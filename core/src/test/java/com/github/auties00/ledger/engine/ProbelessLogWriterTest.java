package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Writers without a [LogWriter.Probe], as a store runs with assertions off: the tests run with assertions on, so
/// every other test runs the paths of a writer that keeps one, and this one runs a store's commit paths in a JVM of
/// its own with assertions off (spinning and parking platform waiters, virtual and timed waiters, groups led by their
/// commits, large values and their forces, rollovers, a close and a reopen).
@Timeout(180)
class ProbelessLogWriterTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> VALUES = Keyspace.paged("values", KeyCodec.utf8(), ValueCodec.bytes());
    private static final int THREADS = 8;
    private static final int COMMITS = 300;

    @Test
    @DisplayName("the tests' writers keep a probe")
    void testsKeepProbes() {
        assertTrue(LogWriter.PROBED, "the tests run with assertions on for LogWriter");
    }

    @Test
    @DisplayName("a store whose writer keeps no probe commits, rolls over, closes and reopens")
    void probelessStore(@TempDir Path directory) throws Exception {
        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        var command = List.of(java, "-da", "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
                Workload.class.getName(), directory.toString());
        var output = directory.resolve("output.txt");
        var process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        if (!process.waitFor(150, TimeUnit.SECONDS)) {
            process.destroyForcibly();
        }
        var text = Files.readString(output);
        assertEquals(0, process.exitValue(), text);
        assertTrue(text.contains("probeless " + (THREADS * COMMITS + 16)), text);
    }

    /// The store the forked JVM runs: it fails with a stack trace and a non-zero exit on any error.
    static final class Workload {
        public static void main(String[] args) throws Exception {
            if (LogWriter.PROBED) {
                throw new AssertionError("assertions are on for LogWriter");
            }
            var directory = Path.of(args[0]).resolve("store");
            // Places for spinners on every system, so the platform waiters spin here too
            ProcessFlusher.maxSpinnersForTests(2);
            try (var ledger = open(directory)) {
                assertNull(probeOf(ledger), "a writer keeps no probe with assertions off");
                var failure = new AtomicReference<Throwable>();
                var threads = new ArrayList<Thread>();
                for (var t = 0; t < THREADS; t++) {
                    var thread = t;
                    var builder = thread % 4 == 3 ? Thread.ofVirtual() : Thread.ofPlatform();
                    threads.add(builder.start(() -> {
                        try {
                            for (var i = 0; i < COMMITS; i++) {
                                var value = new byte[thread == 0 && i % 50 == 0 ? 64 << 10 : 600];
                                value[0] = (byte) i;
                                Commit commit = ledger.commit(ledger.newBatch().put(VALUES, thread + "-" + i, value));
                                if (thread % 4 == 2) {
                                    if (!commit.awaitDurable(Duration.ofMinutes(1))) {
                                        throw new AssertionError("a timed wait ran out");
                                    }
                                } else {
                                    commit.awaitDurable();
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.compareAndSet(null, throwable);
                        }
                    }));
                }
                for (var thread : threads) {
                    thread.join();
                }
                if (failure.get() != null) {
                    throw new AssertionError("a committer failed", failure.get());
                }
                for (var i = 0; i < 16; i++) {
                    ledger.commit(ledger.newBatch().put(VALUES, "lone-" + i, new byte[100])).awaitDurable();
                }
            }
            try (var ledger = open(directory)) {
                var size = ledger.size(VALUES);
                if (size != THREADS * COMMITS + 16) {
                    throw new AssertionError("the reopened store holds " + size + " values");
                }
                System.out.println("probeless " + size);
            }
        }

        private static Ledger open(Path directory) throws IOException {
            Files.createDirectories(directory);
            var options = (LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(VALUES);
            options.largeValues(VALUES, 16 << 10);
            options.segmentSize(256 << 10);
            return options.build();
        }

        private static Object probeOf(Ledger ledger) throws ReflectiveOperationException {
            var field = LogWriter.class.getDeclaredField("probe");
            field.setAccessible(true);
            return field.get(((LiveLedger) ledger).log());
        }
    }
}

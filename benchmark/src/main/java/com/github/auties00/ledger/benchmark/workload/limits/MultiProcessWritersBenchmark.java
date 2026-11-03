package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/// Several processes writing to one store at once: a webhook receiver and a worker that sends, sharing a Cloud
/// session, for example.
///
/// The benchmark process writes 1 KB entries durably while `processes - 1` [MultiProcessWriter] processes do the
/// same to the same store; the score is the benchmark process's share, and each writer's total is printed when the
/// trial ends. Only engines that coordinate writers across processes take part: SQLite through file locks, LMDB
/// through its lock file. Ledger, like MVStore, RocksDB and Berkeley DB, admits one writing process, so it cannot
/// run this at all.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class MultiProcessWritersBenchmark {
    /// The engine, by [EngineKind#id()]; only engines that allow writers in several processes.
    @Param({"sqlite", "lmdb"})
    public String engine;

    /// The processes writing, this one included.
    @Param({"1", "2", "4"})
    public int processes;

    private Corpus corpus;
    private Stores.Store store;
    private final List<Process> writers = new ArrayList<>();
    private final List<BufferedReader> outputs = new ArrayList<>();
    private long written;

    /// Opens an empty store and starts the other writers.
    ///
    /// @throws IOException if the store cannot be opened or a writer cannot start
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, false);
        var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        for (var writer = 1; writer < processes; writer++) {
            var process = new ProcessBuilder(java, Jvm.NATIVE_ACCESS, Jvm.UNSAFE, "-cp", System.getProperty("java.class.path"),
                    MultiProcessWriter.class.getName(), engine, store.directory().toString(), String.valueOf(writer))
                    .redirectErrorStream(true)
                    .start();
            var output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            String line;
            while ((line = output.readLine()) != null && !line.equals("ready")) {
                System.err.println("writer " + writer + ": " + line);
            }
            if (line == null) {
                throw new IOException("Writer " + writer + " exited before it was ready");
            }
            writers.add(process);
            outputs.add(output);
        }
    }

    /// Stops the other writers, prints what each wrote, closes the store and deletes it.
    ///
    /// @throws IOException if the store cannot be closed
    /// @throws InterruptedException if interrupted while waiting for a writer
    @TearDown(Level.Trial)
    public void close() throws IOException, InterruptedException {
        System.err.println("writer 0 (benchmark): written " + written);
        for (var writer = 0; writer < writers.size(); writer++) {
            var process = writers.get(writer);
            process.getOutputStream().close();
            String line;
            while ((line = outputs.get(writer).readLine()) != null) {
                System.err.println("writer " + (writer + 1) + ": " + line);
            }
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        }
        store.close();
    }

    /// Writes one entry durably.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void write() throws IOException {
        var key = Corpus.longKey(written % 4_096);
        store.engine().write(new WriteBatch().put(Keyspace.GENERIC, key, corpus.slice(written, 1_024)), Durability.DURABLE);
        written++;
    }
}

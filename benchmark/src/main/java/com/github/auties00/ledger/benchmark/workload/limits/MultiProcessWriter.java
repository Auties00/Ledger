package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;

import java.io.IOException;
import java.nio.file.Path;

/// The process [MultiProcessWritersBenchmark] starts for each extra writer.
///
/// It opens the store, prints `ready`, then writes 1 KB entries durably under its own keys until its standard input
/// closes, and prints `written` and the number of writes before it exits.
public final class MultiProcessWriter {
    private static volatile boolean stopped;

    private MultiProcessWriter() {
    }

    /// Runs the writer.
    ///
    /// @param args the engine id, the store directory and this writer's number
    /// @throws IOException if the engine fails to open or write
    public static void main(String[] args) throws IOException {
        var kind = EngineKind.of(args[0]);
        var directory = Path.of(args[1]);
        var writer = Long.parseLong(args[2]);
        Thread.ofVirtual().start(() -> {
            try {
                while (System.in.read() != -1) {
                    // Only the end of the stream matters
                }
            } catch (IOException _) {
                // A broken pipe means the parent is gone too
            }
            stopped = true;
        });
        var corpus = new Corpus(1);
        var written = 0L;
        try (var engine = kind.open(directory, EngineSettings.DEFAULT)) {
            System.out.println("ready");
            System.out.flush();
            while (!stopped) {
                var key = Corpus.longKey(writer << 32 | written % 4_096);
                engine.write(new WriteBatch().put(Keyspace.GENERIC, key, corpus.slice(written, 1_024)), Durability.DURABLE);
                written++;
            }
        }
        System.out.println("written " + written);
        System.out.flush();
    }
}

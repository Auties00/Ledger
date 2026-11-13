package com.github.auties00.ledger.benchmark.workload.memory;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Arrivals;
import com.github.auties00.ledger.benchmark.support.Stores;

import java.io.IOException;
import java.nio.file.Path;

/// The process the recovery workload of [OperationMemoryBenchmark] starts to leave a store as a crash leaves it.
///
/// It opens the store, stores received messages durably, 100 per batch, prints `written` and the number of messages,
/// and halts the JVM without closing the store: no shutdown hook, no final checkpoint, only what each durable write
/// made durable.
public final class CrashingWriter {
    private CrashingWriter() {
    }

    /// Runs the writer.
    ///
    /// @param args the engine id, the store directory, whether values are sealed, the messages of the corpus the store
    ///             holds and the messages to store
    /// @throws IOException if the engine fails to open or write
    public static void main(String[] args) throws IOException {
        var kind = EngineKind.of(args[0]);
        var directory = Path.of(args[1]);
        var sealed = Boolean.parseBoolean(args[2]);
        var corpus = new Corpus(Integer.parseInt(args[3]));
        var messages = Integer.parseInt(args[4]);
        var engine = Stores.openAt(kind, directory, Stores.settingsFor(corpus), sealed);
        var arrivals = new Arrivals(corpus, 0, 17);
        var batch = new WriteBatch();
        var written = 0;
        while (written < messages) {
            batch.clear();
            for (var i = 0; i < 100; i++) {
                arrivals.receive(batch);
            }
            engine.write(batch, Durability.DURABLE);
            written += 100;
        }
        System.out.println("written " + written);
        System.out.flush();
        Runtime.getRuntime().halt(0);
    }
}

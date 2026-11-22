package com.github.auties00.ledger.benchmark.support;

import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;

import java.io.IOException;

/// Writes a store template in an order no thread timing changes, so that two builds of one template lay it out alike:
/// one thread, every batch durable before the next is written, a checkpoint on this thread after every [#INTERVAL]
/// bytes of keys and values, and one more before the close.
///
/// The store must be opened with an automatic checkpoint interval far above [#INTERVAL]
/// ([com.github.auties00.ledger.benchmark.engine.EngineSettings#withCheckpointEvery(long)]): a checkpoint that Ledger
/// starts on its own thread cuts the commits wherever the writer happens to be, and every later checkpoint reuses the
/// pages it freed, which is what made each build a different draw.
final class TemplateBuilder implements AutoCloseable {
    /// The bytes of keys and values between two checkpoints: Ledger's default interval under the 4 GiB heap of the
    /// workloads, fixed so that a build in a JVM of another heap lays the template out the same way.
    static final long INTERVAL = 256L << 20;

    private final Engine engine;
    private long sinceCheckpoint;

    /// Takes over `engine`, which [#close()] checkpoints and closes.
    TemplateBuilder(Engine engine) {
        this.engine = engine;
    }

    /// Writes `batch` and waits until it is durable, then checkpoints if [#INTERVAL] bytes were written since the last.
    ///
    /// @throws IOException if the engine fails to write or checkpoint
    void write(WriteBatch batch) throws IOException {
        if (batch.size() == 0) {
            return;
        }
        engine.write(batch, Durability.DURABLE);
        for (var i = 0; i < batch.size(); i++) {
            var value = batch.value(i);
            sinceCheckpoint += batch.key(i).length + (value == null ? 0 : value.length);
        }
        if (sinceCheckpoint >= INTERVAL) {
            engine.checkpoint();
            sinceCheckpoint = 0;
        }
    }

    /// Checkpoints the engine, then closes it.
    ///
    /// @throws IOException if the engine fails to checkpoint or close
    @Override
    public void close() throws IOException {
        try {
            engine.checkpoint();
        } finally {
            engine.close();
        }
    }
}

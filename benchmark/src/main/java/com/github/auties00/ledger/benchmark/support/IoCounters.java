package com.github.auties00.ledger.benchmark.support;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/// The secondary results of the workloads that write: what they cost on disk, next to how long they took.
///
/// JMH reports each field as a secondary result of the iteration. A workload sets them itself, at the end of the
/// work it measures.
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class IoCounters {
    /// The bytes the process passed to the OS for writing during the iteration; see [ProcessProbe].
    public long bytesWritten;
    /// The bytes of keys and values the workload asked the engine to store during the iteration.
    public long payloadBytes;
    /// The bytes the engine's files occupied at the end of the iteration.
    public long sizeOnDisk;

    /// Clears the counters at the start of an iteration.
    @Setup(Level.Iteration)
    public void clear() {
        bytesWritten = 0;
        payloadBytes = 0;
        sizeOnDisk = 0;
    }
}

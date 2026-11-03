package com.github.auties00.ledger.benchmark.support;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.lang.management.ManagementFactory;

/// The secondary results of the workloads that measure memory: what an open, warm store keeps in memory.
///
/// The heap is what a Java engine keeps; resident memory adds what a native engine keeps and the pages of a memory
/// map, but also the rest of the JVM, so compare it between engines rather than read it alone.
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class MemoryCounters {
    /// The heap in use after a full collection.
    public long heapBytes;
    /// The memory of the process resident in RAM; see [ProcessProbe].
    public long residentBytes;

    /// Clears the counters at the start of an iteration.
    @Setup(Level.Iteration)
    public void clear() {
        heapBytes = 0;
        residentBytes = 0;
    }

    /// Collects the heap, then records the heap in use and the resident memory.
    public void record() {
        var memory = ManagementFactory.getMemoryMXBean();
        System.gc();
        System.gc();
        heapBytes = memory.getHeapMemoryUsage().getUsed();
        residentBytes = ProcessProbe.residentBytes();
    }
}

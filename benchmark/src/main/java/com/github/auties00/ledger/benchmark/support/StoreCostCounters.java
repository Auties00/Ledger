package com.github.auties00.ledger.benchmark.support;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/// The secondary results of the workload that measures what open stores cost the process: the memory with none open,
/// with all open, and the difference per store.
///
/// A workload calls [#begin()] before it opens the stores and [#end(int)] once they are open and used. Both collect
/// the heap. Every counter is in bytes. What the first store of a process creates for every later one (a pool, a
/// cache shared by the stores) shows in the counters of a JVM's first iteration, its warm-up, and stays in the floor of
/// the others.
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class StoreCostCounters {
    /// The heap in use after a full collection with the stores open.
    public long heapBytes;
    /// The heap in use after a full collection before the stores were opened.
    public long heapBytesFloor;
    /// `heapBytes` minus `heapBytesFloor`, over the number of stores.
    public long heapBytesPerStore;
    /// The memory of the process resident in RAM with the stores open; see [ProcessProbe].
    public long residentBytes;
    /// The memory of the process resident in RAM before the stores were opened.
    public long residentBytesFloor;
    /// `residentBytes` minus `residentBytesFloor`, over the number of stores.
    public long residentBytesPerStore;

    /// Clears the counters at the start of an iteration.
    @Setup(Level.Iteration)
    public void clear() {
        heapBytes = 0;
        heapBytesFloor = 0;
        heapBytesPerStore = 0;
        residentBytes = 0;
        residentBytesFloor = 0;
        residentBytesPerStore = 0;
    }

    /// Collects the heap and records the floor.
    public void begin() {
        heapBytesFloor = MemoryWatch.liveHeapBytes();
        residentBytesFloor = ProcessProbe.residentBytes();
    }

    /// Collects the heap and records what `stores` open stores added to the floor.
    ///
    /// @param stores the number of stores open
    public void end(int stores) {
        heapBytes = MemoryWatch.liveHeapBytes();
        residentBytes = ProcessProbe.residentBytes();
        heapBytesPerStore = (heapBytes - heapBytesFloor) / stores;
        residentBytesPerStore = (residentBytes - residentBytesFloor) / stores;
    }
}

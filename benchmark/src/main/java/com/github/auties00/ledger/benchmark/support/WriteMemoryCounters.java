package com.github.auties00.ledger.benchmark.support;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/// The secondary results of a workload that writes for a while and measures what the writes not yet checkpointed
/// hold: [PeakCounters], plus the live heap sampled by full collections while the writes go on.
///
/// A workload calls [#begin()] before it opens the store, [#sample()] between writes at fixed points of the
/// workload, and [#end()] after the last write, with the store still open. Every counter is in bytes.
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class WriteMemoryCounters {
    /// The heap in use after a full collection after the last write.
    public long heapBytes;
    /// The heap in use after a full collection before the store was opened.
    public long heapBytesFloor;
    /// The largest heap in use after a full collection at a sample point or at the end: the most the writes held at
    /// once at the points the workload sampled.
    public long heapBytesLive;
    /// The largest heap a collection the JVM ran on its own left in use; see [MemoryWatch]. Never below
    /// `heapBytesLive`.
    public long heapBytesPeak;
    /// The heap the JVM had committed after the last write, before the final collection.
    public long heapBytesCommitted;
    /// The memory of the process resident in RAM after the final collection; see [ProcessProbe].
    public long residentBytes;
    /// The memory of the process resident in RAM before the store was opened, after a full collection.
    public long residentBytesFloor;
    /// The most memory of the process resident in RAM while the workload wrote; see [MemoryWatch].
    public long residentBytesPeak;

    private MemoryWatch watch;

    /// Clears the counters at the start of an iteration.
    @Setup(Level.Iteration)
    public void clear() {
        heapBytes = 0;
        heapBytesFloor = 0;
        heapBytesLive = 0;
        heapBytesPeak = 0;
        heapBytesCommitted = 0;
        residentBytes = 0;
        residentBytesFloor = 0;
        residentBytesPeak = 0;
    }

    /// Collects the heap, records the floor and starts watching the peaks.
    public void begin() {
        heapBytesFloor = MemoryWatch.liveHeapBytes();
        residentBytesFloor = ProcessProbe.residentBytes();
        watch = MemoryWatch.start();
    }

    /// Collects the heap and keeps its live size if it is the largest so far.
    public void sample() {
        heapBytesLive = Math.max(heapBytesLive, MemoryWatch.liveHeapBytes());
    }

    /// Records the committed heap, collects the heap, then records what is left and the peaks, and stops watching.
    public void end() {
        heapBytesCommitted = MemoryWatch.committedHeapBytes();
        heapBytes = MemoryWatch.liveHeapBytes();
        residentBytes = ProcessProbe.residentBytes();
        heapBytesLive = Math.max(heapBytesLive, heapBytes);
        heapBytesPeak = Math.max(watch.heapPeak(), heapBytesLive);
        residentBytesPeak = Math.max(watch.residentPeak(), residentBytes);
        stop();
    }

    /// Stops a watch that a workload which failed left running.
    @TearDown(Level.Iteration)
    public void stop() {
        if (watch != null) {
            watch.close();
            watch = null;
        }
    }
}

package com.github.auties00.ledger.benchmark.support;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/// The secondary results of the workloads that measure the memory of one operation: what the process held before it,
/// at its peak and after it.
///
/// A workload calls [#begin()] before the operation, which runs with no store open unless the workload says
/// otherwise, and [#end()] after it, with the store still open. Both collect the heap, so the time of such a
/// workload is not the point. Every counter is in bytes; `heapBytes` and `residentBytes` minus their `Floor` are what
/// the engine and the operation added to the JVM, the benchmark's own data included in the floor.
///
/// The heap counters are those of a JVM whose heap starts small and grows ([Jvm#HEAP_GROWING]): the committed heap
/// is what the operation made the JVM take, and the resident memory follows it. A JVM's resident memory seldom falls
/// back, so the workloads run one measured operation per JVM: an iteration before it would leave its peak in the
/// floor.
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class PeakCounters {
    /// The heap in use after a full collection at the end of the operation.
    public long heapBytes;
    /// The heap in use after a full collection before the operation started.
    public long heapBytesFloor;
    /// The largest heap a collection left in use during the operation; see [MemoryWatch]. Never below `heapBytes`.
    public long heapBytesPeak;
    /// The heap the JVM had committed when the operation ended, before the final collection.
    public long heapBytesCommitted;
    /// The memory of the process resident in RAM after the final collection; see [ProcessProbe].
    public long residentBytes;
    /// The memory of the process resident in RAM before the operation started, after a full collection.
    public long residentBytesFloor;
    /// The most memory of the process resident in RAM during the operation; see [MemoryWatch].
    public long residentBytesPeak;

    private MemoryWatch watch;

    /// Clears the counters at the start of an iteration.
    @Setup(Level.Iteration)
    public void clear() {
        heapBytes = 0;
        heapBytesFloor = 0;
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

    /// Records the committed heap, collects the heap, then records what is left and the peaks, and stops watching.
    ///
    /// The final collections are watched too: they leave the live heap, which is never above the peak.
    public void end() {
        heapBytesCommitted = MemoryWatch.committedHeapBytes();
        heapBytes = MemoryWatch.liveHeapBytes();
        residentBytes = ProcessProbe.residentBytes();
        heapBytesPeak = Math.max(watch.heapPeak(), heapBytes);
        residentBytesPeak = Math.max(watch.residentPeak(), residentBytes);
        stop();
    }

    /// Stops a watch that an operation which failed left running.
    @TearDown(Level.Iteration)
    public void stop() {
        if (watch != null) {
            watch.close();
            watch = null;
        }
    }
}

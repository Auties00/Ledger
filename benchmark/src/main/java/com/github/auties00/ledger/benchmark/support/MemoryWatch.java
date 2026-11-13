package com.github.auties00.ledger.benchmark.support;

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/// Watches the memory of the process while a workload runs one operation, without collecting the heap itself: the
/// largest heap a garbage collection left behind, and the most memory resident in RAM.
///
/// The heap peak is the largest sum, over the heap's memory pools, of what each collection the JVM ran on its own
/// left in use (`GcInfo.getMemoryUsageAfterGc()`). It bounds the live heap from above, since a young collection
/// leaves the garbage of the old generation that no marking has reclaimed yet, and it sees only the moments a
/// collection ran. The resident peak is the largest of [ProcessProbe#residentBytes()] sampled every few
/// milliseconds by a thread of its own and, on Linux, of the peak the kernel keeps ([ProcessProbe#residentPeakBytes()]),
/// which also sees a spike between two samples.
///
/// The static methods measure the heap at one moment instead.
public final class MemoryWatch implements AutoCloseable {
    private static final long SAMPLE_MILLIS = 5;

    private final Set<String> heapPools;
    private final List<NotificationEmitter> emitters;
    private final NotificationListener listener;
    private final AtomicLong heapPeak;
    private final AtomicLong residentPeak;
    private final boolean kernelPeak;
    private final Thread sampler;
    private volatile boolean closed;

    private MemoryWatch() {
        this.heapPools = new HashSet<>();
        for (var pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                heapPools.add(pool.getName());
            }
        }
        this.heapPeak = new AtomicLong();
        this.residentPeak = new AtomicLong(ProcessProbe.residentBytes());
        this.listener = this::collected;
        this.emitters = new ArrayList<>();
        for (var collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (collector instanceof NotificationEmitter emitter) {
                emitter.addNotificationListener(listener, null, null);
                emitters.add(emitter);
            }
        }
        this.kernelPeak = ProcessProbe.resetResidentPeak();
        this.sampler = Thread.ofPlatform()
                .name("memory-watch")
                .daemon()
                .start(this::sample);
    }

    /// Starts watching.
    ///
    /// @return the watch, which the caller closes
    public static MemoryWatch start() {
        return new MemoryWatch();
    }

    /// {@return the heap in use after a full collection, in bytes}
    ///
    /// Collects twice, so that what the first collection made unreachable through references is gone too, as
    /// [MemoryCounters] does.
    public static long liveHeapBytes() {
        System.gc();
        System.gc();
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    /// {@return the heap the JVM has committed, in bytes, without collecting}
    public static long committedHeapBytes() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted();
    }

    /// {@return the largest heap in use after a collection since the watch started, in bytes, or 0 if none ran}
    public long heapPeak() {
        return heapPeak.get();
    }

    /// {@return the most memory of the process resident in RAM since the watch started, in bytes, or -1 when the OS
    /// does not say}
    public long residentPeak() {
        var peak = residentPeak.get();
        if (kernelPeak) {
            peak = Math.max(peak, ProcessProbe.residentPeakBytes());
        }
        return Math.max(peak, ProcessProbe.residentBytes());
    }

    /// Stops the sampler and stops listening to collections.
    @Override
    public void close() {
        closed = true;
        sampler.interrupt();
        try {
            sampler.join();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
        for (var emitter : emitters) {
            try {
                emitter.removeNotificationListener(listener);
            } catch (ListenerNotFoundException _) {
                // Never added, so nothing to remove
            }
        }
    }

    private void sample() {
        while (!closed) {
            var resident = ProcessProbe.residentBytes();
            residentPeak.accumulateAndGet(resident, Math::max);
            try {
                Thread.sleep(SAMPLE_MILLIS);
            } catch (InterruptedException _) {
                return;
            }
        }
    }

    private void collected(Notification notification, Object handback) {
        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        var info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
        var used = 0L;
        for (var pool : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
            if (heapPools.contains(pool.getKey())) {
                used += pool.getValue().getUsed();
            }
        }
        heapPeak.accumulateAndGet(used, Math::max);
    }
}

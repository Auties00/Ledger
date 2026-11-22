package com.github.auties00.ledger.benchmark.support;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/// The timed part of one benchmark operation, which [CpuAttributionProfiler] records as a JFR event so that it can
/// tell the GC pauses, deoptimizations and worker CPU of the timed code from those of the untimed setup and teardown
/// that share JMH's iteration window.
///
/// Until the profiler calls [#enable()] in the forked JVM (it does with `jfr=true`), [#start()] returns one shared
/// instance and [#finish()] does nothing: a fork without the profiler loads no `jdk.jfr` class and no thread bean
/// through this class. Loading and initializing JFR costs about 377 classes, 745 KB of heap that a full collection
/// keeps and 0.1 s at the first call, which the memory counters and the single-shot scores of the marked workloads
/// would otherwise measure.
public final class MeasuredOp {
    /// Whether the profiler records operations in this JVM; set once, before the first recorded operation.
    private static volatile boolean enabled;
    /// The operation every [#start()] returns while recording is off.
    private static final MeasuredOp OFF = new MeasuredOp(null, 0);

    /// The started `Recorder.Event` when recording, `null` otherwise; typed `Object` so that this class names no
    /// `jdk.jfr` type.
    private final Object event;
    /// CPU time of the measuring thread at the start, ns.
    private final long cpuAtStart;

    private MeasuredOp(Object event, long cpuAtStart) {
        this.event = event;
        this.cpuAtStart = cpuAtStart;
    }

    /// {@return a started operation}
    public static MeasuredOp start() {
        return enabled ? Recorder.start() : OFF;
    }

    /// Ends the operation and, when recording, commits its event.
    public void finish() {
        if (event != null) {
            Recorder.finish(event, cpuAtStart);
        }
    }

    /// Registers the event with JFR and starts recording operations in this JVM; a recording still has to enable
    /// [#EVENT_NAME].
    static void enable() {
        Recorder.register();
        enabled = true;
    }

    /// The name of the JFR event of an operation.
    static final String EVENT_NAME = "ledger.benchmark.MeasuredOp";

    /// The JFR side of [MeasuredOp], loaded only once the profiler enables recording.
    private static final class Recorder {
        private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

        static void register() {
            FlightRecorder.register(Event.class);
        }

        static MeasuredOp start() {
            var event = new Event();
            var cpu = event.isEnabled() ? THREADS.getCurrentThreadCpuTime() : 0;
            event.begin();
            return new MeasuredOp(event, cpu);
        }

        static void finish(Object started, long cpuAtStart) {
            var event = (Event) started;
            event.end();
            if (event.shouldCommit()) {
                event.workerCpu = THREADS.getCurrentThreadCpuTime() - cpuAtStart;
                event.commit();
            }
        }

        @Name(EVENT_NAME)
        @Label("Measured Operation")
        @Category("Ledger Benchmark")
        @Description("The timed part of one benchmark operation")
        @StackTrace(false)
        @Enabled(false)
        static final class Event extends jdk.jfr.Event {
            /// CPU time of the measuring thread inside the operation.
            @Label("Worker CPU")
            @Timespan(Timespan.NANOSECONDS)
            long workerCpu;
        }
    }
}

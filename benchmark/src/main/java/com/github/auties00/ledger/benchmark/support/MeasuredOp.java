package com.github.auties00.ledger.benchmark.support;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/// The timed part of one benchmark operation, as a JFR event, so that [CpuAttributionProfiler] can tell the GC
/// pauses, deoptimizations and worker CPU of the timed code from those of the untimed setup and teardown that share
/// JMH's iteration window.
///
/// Disabled unless a recording enables it (the profiler does with `jfr=true`); disabled, [#start()] and [#finish()]
/// cost an allocation the JIT usually removes and two field reads.
@Name("ledger.benchmark.MeasuredOp")
@Label("Measured Operation")
@Category("Ledger Benchmark")
@Description("The timed part of one benchmark operation")
@StackTrace(false)
@Enabled(false)
public final class MeasuredOp extends Event {
    private static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

    /// CPU time of the measuring thread inside the operation.
    @Label("Worker CPU")
    @Timespan(Timespan.NANOSECONDS)
    long workerCpu;

    private transient long cpuAtStart;

    /// {@return a started event}
    public static MeasuredOp start() {
        var event = new MeasuredOp();
        if (event.isEnabled()) {
            event.cpuAtStart = THREADS.getCurrentThreadCpuTime();
        }
        event.begin();
        return event;
    }

    /// Ends and commits the event.
    public void finish() {
        end();
        if (shouldCommit()) {
            workerCpu = THREADS.getCurrentThreadCpuTime() - cpuAtStart;
            commit();
        }
    }
}

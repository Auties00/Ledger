package com.github.auties00.ledger.benchmark.support;

import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordingStream;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;
import org.openjdk.jmh.runner.IterationType;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/// A JMH profiler that splits the CPU time and the allocation of each iteration by thread group, and counts the GC,
/// JIT and deoptimization events that land inside it, as secondary results normalized per operation.
///
/// Run with `-prof com.github.auties00.ledger.benchmark.support.CpuAttributionProfiler`. Options, joined by `;`
/// (no spaces, so they survive compare.yml's word splitting): `jfr=true` also streams JFR in process to count
/// deoptimizations, compilations and GC pauses, inside the iteration and inside the timed part of each operation the
/// benchmark marks with [MeasuredOp] (each iteration then waits, untimed, about a second for the stream's flush);
/// `period=<ms>` sets the thread sampling period (default 10); `pause=<ms>` sleeps that long after each iteration,
/// untimed, without JFR, to tell the effect of `jfr=true`'s wait for the stream apart from that of JFR itself.
///
/// Results (per op unless noted; `<group>` is `worker` for the JMH benchmark threads, one group per Ledger platform
/// thread name with its index dropped, such as `io`, `checkpoint` and `prepare`, `carrier` for the virtual-thread
/// carriers, which run Ledger's virtual threads, `commonpool`, `jfr`, `profiler` for this profiler's sampler, and
/// `other`):
/// - `cpu.process.norm`: process CPU time, ns/op, every thread included.
/// - `cpu.<group>.norm`: CPU time of the group, ns/op.
/// - `cpu.gc.norm`: GC CPU time, ns/op (`MemoryMXBean.getTotalGcCpuTime`).
/// - `cpu.vm.norm`: what no Java thread or the GC accounts for, ns/op: JIT compiler threads, the VM thread, and the
///   CPU a thread spent after its last sample when it ended inside the iteration.
/// - `cpu.util`: process CPU time over iteration wall time, CPUs, per iteration.
/// - `alloc.<group>.norm`, `alloc.total.norm`: bytes allocated by the group and by every thread, ended ones
///   included, B/op.
/// - `jit.ms`: compilation time during the iteration, ms; `gc.count.window`, `gc.pause.window`: stop-the-world
///   collections during the iteration and their time, count and ms; `threads.ended`: threads that ended inside the
///   iteration (each can lose up to one sampling period of CPU to `cpu.vm.norm`).
/// - With `jfr=true`: `jfr.deopt.ledger`, `jfr.deopt.all`, `jfr.c2.ledger` (deoptimizations of Ledger methods, all
///   deoptimizations, and C2 compilations of Ledger methods during the iteration), `jfr.deopt.ledger.total` (since the
///   fork's first iteration, warm-up included; MAX over iterations), and when the benchmark marks its timed code:
///   `op.ms`, `op.cpu.worker.norm` (worker CPU inside the timed code, ns/op), `op.gc.count`, `op.gc.pause.ms`,
///   `op.deopt.ledger` (summed over the iteration's operations). Every Ledger deoptimization is also printed to the
///   log as a `cpu-attribution: deopt` line.
///
/// The window is JMH's iteration window, which also holds `Level.Iteration` setup and teardown, the same window
/// `-prof gc` normalizes over; the `op.*` results are the only ones limited to the timed code.
public final class CpuAttributionProfiler implements InternalProfiler {
    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    private static final String SAMPLER = "cpu-attribution-sampler";
    private static final int PRINTED_DEOPTS = 50;

    private final boolean jfr;
    private final long periodMillis;
    private final long pauseMillis;

    private final Object lock = new Object();
    /// Every thread seen since the window opened, by id; guarded by [#lock].
    private final Map<Long, Tracked> tracked = new HashMap<>();
    /// Whether a window is open; guarded by [#lock].
    private boolean inWindow;
    private Thread sampler;

    private long processCpuBefore;
    private long gcCpuBefore;
    private long jitBefore;
    private long wallBefore;
    private long gcCountBefore;
    private long gcTimeBefore;
    private long allocTotalBefore;

    private RecordingStream stream;
    private final ConcurrentLinkedQueue<Deopt> deopts = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Gc> collections = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Op> ops = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Instant> compilationsLedgerC2 = new ConcurrentLinkedQueue<>();
    private final Map<Long, Instant> marks = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicLong nextMark = new AtomicLong();
    private long windowMark;
    private long ledgerDeoptsTotal;

    private static final class Tracked {
        final String group;
        long cpuBase;
        long allocBase;
        long cpu;
        long alloc;
        boolean live;

        Tracked(String group) {
            this.group = group;
        }
    }

    private record Deopt(Instant start, boolean ledger, String description) {
    }

    private record Gc(Instant start, long pauseNanos) {
    }

    private record Op(Instant start, Instant end, long workerCpu) {
    }

    /// A point in the stream's time base: the profiler commits one at each window edge and waits for it.
    @Name("ledger.benchmark.ProfilerMark")
    @StackTrace(false)
    @Enabled(false)
    static final class Mark extends Event {
        long id;
    }

    public CpuAttributionProfiler() {
        this("");
    }

    public CpuAttributionProfiler(String initLine) {
        var jfr = false;
        var period = 10L;
        var pause = 0L;
        for (var option : initLine.split("[;,]")) {
            var pair = option.trim().split("=", 2);
            if (pair.length == 2 && pair[0].equals("jfr")) {
                jfr = Boolean.parseBoolean(pair[1]);
            } else if (pair.length == 2 && pair[0].equals("period")) {
                period = Math.max(1, Long.parseLong(pair[1]));
            } else if (pair.length == 2 && pair[0].equals("pause")) {
                pause = Math.max(0, Long.parseLong(pair[1]));
            }
        }
        this.jfr = jfr;
        this.periodMillis = period;
        this.pauseMillis = pause;
    }

    @Override
    public String getDescription() {
        return "CPU time and allocation per thread group, JIT, GC and deoptimization events per iteration";
    }

    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        if (sampler == null) {
            System.out.println("cpu-attribution: cpus=" + Runtime.getRuntime().availableProcessors() + " model="
                    + cpuModel() + " period=" + periodMillis + "ms jfr=" + jfr);
            sampler = Thread.ofPlatform().daemon().name(SAMPLER).start(this::sampleLoop);
        }
        if (jfr && stream == null) {
            startStream();
        }
        if (stream != null) {
            windowMark = mark();
        }
        gcCountBefore = gcCount();
        gcTimeBefore = gcTime();
        synchronized (lock) {
            tracked.clear();
            sample(true);
            inWindow = true;
        }
        allocTotalBefore = THREADS.getTotalThreadAllocatedBytes();
        processCpuBefore = processCpu();
        gcCpuBefore = ManagementFactory.getMemoryMXBean().getTotalGcCpuTime();
        jitBefore = ManagementFactory.getCompilationMXBean().getTotalCompilationTime();
        wallBefore = System.nanoTime();
    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams,
                                                       IterationResult result) {
        var wall = System.nanoTime() - wallBefore;
        var process = processCpu() - processCpuBefore;
        var gc = ManagementFactory.getMemoryMXBean().getTotalGcCpuTime() - gcCpuBefore;
        var jit = ManagementFactory.getCompilationMXBean().getTotalCompilationTime() - jitBefore;
        var allocTotal = THREADS.getTotalThreadAllocatedBytes() - allocTotalBefore;
        var cpuByGroup = new TreeMap<String, Long>();
        var allocByGroup = new TreeMap<String, Long>();
        long javaThreads = 0;
        long ended = 0;
        synchronized (lock) {
            sample(false);
            inWindow = false;
            for (var thread : tracked.values()) {
                var cpu = thread.cpu - thread.cpuBase;
                javaThreads += cpu;
                cpuByGroup.merge(thread.group, cpu, Long::sum);
                allocByGroup.merge(thread.group, thread.alloc - thread.allocBase, Long::sum);
                if (!thread.live) {
                    ended++;
                }
            }
        }
        var collectionsInWindow = gcCount() - gcCountBefore;
        var gcTimeInWindow = gcTime() - gcTimeBefore;

        var measuredOps = Math.max(1, result.getMetadata().getMeasuredOps());
        List<Result> out = new ArrayList<>();
        out.add(perOp("cpu.process.norm", process, measuredOps));
        for (var entry : cpuByGroup.entrySet()) {
            out.add(perOp("cpu." + entry.getKey() + ".norm", entry.getValue(), measuredOps));
        }
        out.add(perOp("cpu.gc.norm", gc, measuredOps));
        out.add(perOp("cpu.vm.norm", Math.max(0, process - javaThreads - gc), measuredOps));
        out.add(new ScalarResult("cpu.util", wall > 0 ? (double) process / wall : 0, "CPUs", AggregationPolicy.AVG));
        for (var entry : allocByGroup.entrySet()) {
            if (entry.getValue() != 0) {
                out.add(new ScalarResult("alloc." + entry.getKey() + ".norm", (double) entry.getValue() / measuredOps,
                        "B/op", AggregationPolicy.AVG));
            }
        }
        out.add(new ScalarResult("alloc.total.norm", (double) allocTotal / measuredOps, "B/op", AggregationPolicy.AVG));
        out.add(new ScalarResult("jit.ms", jit, "ms", AggregationPolicy.AVG));
        out.add(new ScalarResult("gc.count.window", collectionsInWindow, "#", AggregationPolicy.AVG));
        out.add(new ScalarResult("gc.pause.window", gcTimeInWindow, "ms", AggregationPolicy.AVG));
        out.add(new ScalarResult("threads.ended", ended, "#", AggregationPolicy.AVG));

        var line = new StringBuilder("cpu-attribution: ").append(iterationParams.getType() == IterationType.WARMUP ? "warmup" : "measure")
                .append(" ops=").append(measuredOps)
                .append(" process=").append(TimeUnit.NANOSECONDS.toMicros(process)).append("us")
                .append(" gc=").append(TimeUnit.NANOSECONDS.toMicros(gc)).append("us")
                .append(" jit=").append(jit).append("ms")
                .append(" gcCount=").append(collectionsInWindow);
        for (var entry : cpuByGroup.entrySet()) {
            line.append(' ').append(entry.getKey()).append('=').append(TimeUnit.NANOSECONDS.toMicros(entry.getValue())).append("us");
        }
        if (stream != null) {
            jfrResults(out, line, measuredOps);
        }
        System.out.println(line);
        if (pauseMillis > 0) {
            try {
                Thread.sleep(pauseMillis);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
        return out;
    }

    /// Adds the results that come from the JFR stream, once the stream has delivered the window's closing mark.
    private void jfrResults(List<Result> out, StringBuilder line, long measuredOps) {
        var endMark = mark();
        var start = awaitMark(windowMark);
        var end = awaitMark(endMark);
        if (start == null || end == null) {
            System.out.println("cpu-attribution: the JFR stream did not deliver the window marks");
            return;
        }
        long deoptLedger = 0;
        long deoptAll = 0;
        var printed = 0;
        var windowOps = new ArrayList<Op>();
        for (var op : ops) {
            if (!op.start().isBefore(start) && !op.end().isAfter(end)) {
                windowOps.add(op);
            }
        }
        long opDeoptLedger = 0;
        for (var deopt : deopts) {
            if (inside(deopt.start(), start, end)) {
                deoptAll++;
                if (deopt.ledger()) {
                    deoptLedger++;
                    var inOp = inAnyOp(deopt.start(), windowOps);
                    if (inOp) {
                        opDeoptLedger++;
                    }
                    if (printed++ < PRINTED_DEOPTS) {
                        System.out.println("cpu-attribution: deopt " + deopt.description() + " inOp=" + inOp);
                    }
                }
            }
        }
        long c2Ledger = 0;
        for (var compilation : compilationsLedgerC2) {
            if (inside(compilation, start, end)) {
                c2Ledger++;
            }
        }
        ledgerDeoptsTotal += deoptLedger;
        out.add(new ScalarResult("jfr.deopt.ledger", deoptLedger, "#", AggregationPolicy.AVG));
        out.add(new ScalarResult("jfr.deopt.all", deoptAll, "#", AggregationPolicy.AVG));
        out.add(new ScalarResult("jfr.c2.ledger", c2Ledger, "#", AggregationPolicy.AVG));
        out.add(new ScalarResult("jfr.deopt.ledger.total", ledgerDeoptsTotal, "#", AggregationPolicy.MAX));
        line.append(" deoptLedger=").append(deoptLedger).append(" c2Ledger=").append(c2Ledger);
        if (!windowOps.isEmpty()) {
            long opNanos = 0;
            long opCpu = 0;
            for (var op : windowOps) {
                opNanos += Duration.between(op.start(), op.end()).toNanos();
                opCpu += op.workerCpu();
            }
            long opGcCount = 0;
            long opGcPause = 0;
            for (var collection : collections) {
                if (inAnyOp(collection.start(), windowOps)) {
                    opGcCount++;
                    opGcPause += collection.pauseNanos();
                }
            }
            out.add(new ScalarResult("op.ms", opNanos / 1e6, "ms", AggregationPolicy.AVG));
            out.add(new ScalarResult("op.cpu.worker.norm", (double) opCpu / windowOps.size(), "ns/op", AggregationPolicy.AVG));
            out.add(new ScalarResult("op.gc.count", opGcCount, "#", AggregationPolicy.AVG));
            out.add(new ScalarResult("op.gc.pause.ms", opGcPause / 1e6, "ms", AggregationPolicy.AVG));
            out.add(new ScalarResult("op.deopt.ledger", opDeoptLedger, "#", AggregationPolicy.AVG));
            line.append(" op=").append(TimeUnit.NANOSECONDS.toMicros(opNanos)).append("us")
                    .append(" opWorker=").append(TimeUnit.NANOSECONDS.toMicros(opCpu)).append("us")
                    .append(" opGc=").append(opGcCount).append('/').append(TimeUnit.NANOSECONDS.toMicros(opGcPause)).append("us");
        }
        deopts.removeIf(deopt -> !deopt.start().isAfter(end));
        collections.removeIf(collection -> !collection.start().isAfter(end));
        ops.removeIf(op -> !op.start().isAfter(end));
        compilationsLedgerC2.removeIf(compilation -> !compilation.isAfter(end));
        marks.keySet().removeIf(id -> id <= endMark);
    }

    private static boolean inside(Instant time, Instant start, Instant end) {
        return !time.isBefore(start) && !time.isAfter(end);
    }

    private static boolean inAnyOp(Instant time, List<Op> windowOps) {
        for (var op : windowOps) {
            if (inside(time, op.start(), op.end())) {
                return true;
            }
        }
        return false;
    }

    private void startStream() {
        jdk.jfr.FlightRecorder.register(Mark.class);
        jdk.jfr.FlightRecorder.register(MeasuredOp.class);
        stream = new RecordingStream();
        stream.enable("jdk.Deoptimization").withoutStackTrace();
        stream.enable("jdk.GarbageCollection");
        stream.enable("jdk.Compilation").withThreshold(Duration.ZERO);
        stream.enable("ledger.benchmark.MeasuredOp");
        stream.enable("ledger.benchmark.ProfilerMark");
        stream.onEvent("jdk.Deoptimization", event -> {
            var method = event.getValue("method");
            var ledger = method instanceof RecordedMethod recorded && isLedger(recorded);
            deopts.add(new Deopt(event.getStartTime(), ledger, ledger ? describeDeopt(event, (RecordedMethod) method) : ""));
        });
        stream.onEvent("jdk.GarbageCollection", event -> collections.add(new Gc(event.getStartTime(),
                event.getDuration("sumOfPauses").toNanos())));
        stream.onEvent("jdk.Compilation", event -> {
            if (event.getValue("compileLevel") instanceof Number level && level.intValue() == 4
                    && event.getValue("method") instanceof RecordedMethod method
                    && isLedger(method)) {
                compilationsLedgerC2.add(event.getStartTime());
            }
        });
        stream.onEvent("ledger.benchmark.MeasuredOp", event -> ops.add(new Op(event.getStartTime(), event.getEndTime(),
                event.getLong("workerCpu"))));
        stream.onEvent("ledger.benchmark.ProfilerMark", event -> marks.put(event.getLong("id"), event.getStartTime()));
        stream.startAsync();
    }

    private static boolean isLedger(RecordedMethod method) {
        var type = method.getType().getName();
        return type.startsWith("com.github.auties00.ledger.") && !type.startsWith("com.github.auties00.ledger.benchmark.");
    }

    private static String describeDeopt(RecordedEvent event, RecordedMethod method) {
        var type = method.getType().getName();
        return type.substring(type.lastIndexOf('.') + 1) + "." + method.getName()
                + " line=" + event.getValue("lineNumber") + " bci=" + event.getValue("bci")
                + " reason=" + event.getValue("reason") + " action=" + event.getValue("action")
                + " instruction=" + event.getValue("instruction");
    }

    private long mark() {
        var event = new Mark();
        event.id = nextMark.incrementAndGet();
        event.commit();
        return event.id;
    }

    /// Waits, untimed, for the stream to deliver a mark: events reach the stream at each flush, about once a second.
    private Instant awaitMark(long id) {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            var time = marks.get(id);
            if (time != null) {
                return time;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private void sampleLoop() {
        while (true) {
            try {
                Thread.sleep(periodMillis);
            } catch (InterruptedException exception) {
                return;
            }
            synchronized (lock) {
                if (inWindow) {
                    sample(false);
                }
            }
        }
    }

    /// Reads the CPU time and allocation of every live platform thread; with `baseline`, also takes them as the
    /// window's starting point. Called with [#lock] held.
    private void sample(boolean baseline) {
        var ids = THREADS.getAllThreadIds();
        var cpu = THREADS.getThreadCpuTime(ids);
        var alloc = THREADS.getThreadAllocatedBytes(ids);
        for (var thread : tracked.values()) {
            thread.live = false;
        }
        List<Integer> unnamed = null;
        for (var i = 0; i < ids.length; i++) {
            if (cpu[i] < 0) {
                continue;
            }
            var thread = tracked.get(ids[i]);
            if (thread == null) {
                if (unnamed == null) {
                    unnamed = new ArrayList<>();
                }
                unnamed.add(i);
                continue;
            }
            thread.cpu = cpu[i];
            thread.alloc = alloc[i];
            thread.live = true;
        }
        if (unnamed != null) {
            var newIds = new long[unnamed.size()];
            for (var i = 0; i < newIds.length; i++) {
                newIds[i] = ids[unnamed.get(i)];
            }
            var infos = THREADS.getThreadInfo(newIds);
            for (var i = 0; i < newIds.length; i++) {
                var index = unnamed.get(i);
                var thread = new Tracked(infos[i] == null ? "other" : group(infos[i].getThreadName()));
                thread.cpu = cpu[index];
                thread.alloc = alloc[index];
                thread.live = true;
                if (baseline) {
                    thread.cpuBase = thread.cpu;
                    thread.allocBase = thread.alloc;
                }
                tracked.put(newIds[i], thread);
            }
        }
    }

    private static Result perOp(String name, long nanos, long measuredOps) {
        return new ScalarResult(name, (double) nanos / measuredOps, "ns/op", AggregationPolicy.AVG);
    }

    private static long gcCount() {
        long count = 0;
        for (var collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!collector.getName().contains("Concurrent")) {
                count += Math.max(0, collector.getCollectionCount());
            }
        }
        return count;
    }

    private static long gcTime() {
        long time = 0;
        for (var collector : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!collector.getName().contains("Concurrent")) {
                time += Math.max(0, collector.getCollectionTime());
            }
        }
        return time;
    }

    static String group(String name) {
        if (name.startsWith("ledger-")) {
            // ledger-io-3 -> io, ledger-page-force -> page-force
            return name.substring("ledger-".length()).replaceAll("-?[0-9]+$", "");
        }
        if (name.contains("jmh-worker")) {
            return "worker";
        }
        if (name.startsWith("ForkJoinPool.commonPool")) {
            return "commonpool";
        }
        if (name.startsWith("ForkJoinPool")) {
            return "carrier";
        }
        if (name.equals(SAMPLER)) {
            return "profiler";
        }
        if (name.startsWith("JFR")) {
            return "jfr";
        }
        return "other";
    }

    private static long processCpu() {
        return ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class).getProcessCpuTime();
    }

    private static String cpuModel() {
        try {
            var cpuinfo = Path.of("/proc/cpuinfo");
            if (Files.isReadable(cpuinfo)) {
                for (var line : Files.readAllLines(cpuinfo, StandardCharsets.UTF_8)) {
                    if (line.startsWith("model name")) {
                        return line.substring(line.indexOf(':') + 1).trim();
                    }
                }
            }
            if (System.getProperty("os.name", "").startsWith("Windows")) {
                var process = new ProcessBuilder("reg", "query", "HKLM\\HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0",
                        "/v", "ProcessorNameString").redirectErrorStream(true).start();
                var text = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                process.waitFor(5, TimeUnit.SECONDS);
                var at = text.indexOf("REG_SZ");
                if (at >= 0) {
                    return text.substring(at + "REG_SZ".length()).trim();
                }
            }
        } catch (IOException exception) {
            return "unknown (" + exception.getMessage() + ")";
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        return System.getenv().getOrDefault("PROCESSOR_IDENTIFIER", "unknown");
    }
}

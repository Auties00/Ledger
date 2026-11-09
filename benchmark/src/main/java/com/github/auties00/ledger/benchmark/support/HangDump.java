package com.github.auties00.ledger.benchmark.support;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;

/// A temporary diagnostic: a fork still alive after a few minutes has hung, so it writes the stacks of its threads to
/// the results of the compare, virtual ones included, twice, and ends itself.
public final class HangDump {
    private static final long FIRST_MILLIS = 7 * 60_000;
    private static final long NEXT_MILLIS = 60_000;

    static {
        Thread.ofPlatform().daemon().name("hang-dump").start(HangDump::run);
    }

    private HangDump() {
    }

    /// Starts the timer of this JVM, if it did not start yet.
    public static void arm() {
    }

    private static void run() {
        try {
            Thread.sleep(FIRST_MILLIS);
            dump(1);
            Thread.sleep(NEXT_MILLIS);
            dump(2);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        Runtime.getRuntime().halt(3);
    }

    private static void dump(int number) throws Exception {
        var directory = Path.of("results");
        if (!Files.isDirectory(directory)) {
            directory = Path.of(System.getProperty("java.io.tmpdir"));
        }
        var stem = "hang-" + ProcessHandle.current().pid() + "-" + number;
        var threads = ManagementFactory.getThreadMXBean();
        var lines = new ArrayList<String>();
        lines.add("At " + Instant.now());
        var deadlocked = threads.findDeadlockedThreads();
        lines.add("Deadlocked: " + java.util.Arrays.toString(deadlocked));
        for (var info : threads.dumpAllThreads(true, true, 200)) {
            lines.add(info.toString());
            lines.add("  full stack:");
            for (var frame : info.getStackTrace()) {
                lines.add("    " + frame);
            }
        }
        Files.write(directory.resolve(stem + "-platform.txt"), lines);
        var file = directory.resolve(stem + "-all.txt").toAbsolutePath();
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).dumpThreads(file.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
    }
}

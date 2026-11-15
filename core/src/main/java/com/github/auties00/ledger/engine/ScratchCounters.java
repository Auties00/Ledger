package com.github.auties00.ledger.engine;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// Diagnostic counters, printed to the standard error by each checkpoint. Not for merging.
final class ScratchCounters {
    private static final ConcurrentHashMap<String, AtomicLong> C = new ConcurrentHashMap<>();

    private ScratchCounters() {
    }

    static String who() {
        var thread = Thread.currentThread();
        var name = thread.getName();
        if (name.isEmpty()) {
            return thread.isVirtual() ? "vthread" : "unnamed";
        }
        var dash = name.lastIndexOf('-');
        if (dash > 0 && dash < name.length() - 1 && name.substring(dash + 1).chars().allMatch(Character::isDigit)) {
            name = name.substring(0, dash);
        }
        return name;
    }

    static void add(String key, long n) {
        C.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(n);
    }

    static void addWho(String key, long n) {
        add(key + "@" + who(), n);
        add(key, n);
    }

    static Map<String, Long> drainAll() {
        var out = new TreeMap<String, Long>();
        for (var entry : C.entrySet()) {
            out.put(entry.getKey(), entry.getValue().getAndSet(0));
        }
        out.values().removeIf(v -> v == 0);
        return out;
    }

    static void print(String what) {
        System.err.println("DIAG " + what + " " + drainAll());
    }
}

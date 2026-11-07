package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// The ticker of the process on its own: what one watched object that throws leaves to the others.
class IdleTickerTest {
    @Test
    @DisplayName("a watched object that throws an error loses its tick, and the others still get theirs")
    void throwingWatcher() {
        var ticks = new AtomicInteger();
        IdleTicker.Watched failing = () -> {
            throw new OutOfMemoryError("unable to create native thread");
        };
        IdleTicker.Watched counting = ticks::incrementAndGet;
        IdleTicker.watch(failing);
        IdleTicker.watch(counting);
        try {
            IdleTicker.tickAll();
            IdleTicker.tickAll();
        } finally {
            IdleTicker.unwatch(failing);
            IdleTicker.unwatch(counting);
        }
        // The process's own ticker may have ticked them too
        assertTrue(ticks.get() >= 2, "ticked " + ticks.get() + " times");
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A store whose log fails to force: it reports the stop once, refuses every later commit, compute and wait, keeps
/// reading, starts no checkpoint, and releases its directory when closed, so it opens again at once with every durable
/// commit.
class FailureTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());

    private static Ledger.Options options(Path directory) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES).keyspace(CHATS))
                .writeThrough(WriteThrough.Mode.OFF)
                .checkpointEvery(1 << 20);
    }

    @Test
    @DisplayName("a failed force of the log stops the store, which reports it once and recovers its durable commits")
    void stop() throws Exception {
        var disk = CrashFileSystem.create();
        var failing = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                throw new IOException("The device failed the flush");
            }
        });
        var directory = disk.getPath("/store");
        var stops = new CopyOnWriteArrayList<LedgerIoException>();
        var background = new CopyOnWriteArrayList<LedgerException>();
        var ledger = options(directory).onStop(stops::add).onBackgroundFailure(background::add).build();
        for (var i = 0; i < 20; i++) {
            ledger.commit(ledger.newBatch().put(NAMES, "durable" + i, "v").put(CHATS, "chat", "v" + i)).awaitDurable();
        }
        failing.set(true);
        var lost = ledger.commit(ledger.newBatch().put(NAMES, "lost", "v"));
        assertThrows(LedgerIoException.class, lost::awaitDurable);
        assertEquals(1, stops.size());
        var stopped = stops.getFirst();

        // Every later write and wait fails with it; reads go on
        assertTrue(assertThrows(LedgerIoException.class, () -> ledger.commit(ledger.newBatch().put(NAMES, "after", "v"))) == stopped);
        var refused = assertThrows(LedgerIoException.class, () -> ledger.compute(CHATS, "chat", value -> value + "!"));
        assertTrue(refused == stopped, "a later failure replaced the first");
        // A compute that changes nothing, a batch of only such computes and an empty batch are refused too
        assertThrows(LedgerIoException.class, () -> ledger.compute(CHATS, "chat", value -> value));
        var unchanged = ledger.newBatch();
        unchanged.compute(NAMES, "durable0", value -> value);
        assertThrows(LedgerIoException.class, () -> ledger.commit(unchanged));
        assertThrows(LedgerIoException.class, () -> ledger.commit(ledger.newBatch()));
        assertThrows(LedgerIoException.class, ledger::awaitDurable);
        assertThrows(LedgerIoException.class, ledger::checkpoint);
        assertEquals(Optional.of("v19"), ledger.get(CHATS, "chat"));
        assertEquals(Optional.of("v"), ledger.get(NAMES, "durable0"));
        // Many more commits start no checkpoint, so nothing else is reported
        var big = "x".repeat(100_000);
        for (var i = 0; i < 40; i++) {
            var index = i;
            assertThrows(LedgerIoException.class, () -> ledger.commit(ledger.newBatch().put(NAMES, "big" + index, big)));
        }
        assertFalse(((LiveLedger) ledger).checkpointer().startedForTests());
        assertEquals(1, stops.size());
        assertTrue(background.isEmpty(), "background failures: " + background);

        // The close throws the failure and lets go of the directory
        var closing = assertThrows(LedgerIoException.class, ledger::close);
        assertInstanceOf(LedgerIoException.class, closing);
        assertEquals(stopped.getClass(), closing.getClass());
        failing.set(false);
        try (var reopened = options(directory).build()) {
            for (var i = 0; i < 20; i++) {
                assertEquals(Optional.of("v"), reopened.get(NAMES, "durable" + i));
            }
            assertEquals(Optional.of("v19"), reopened.get(CHATS, "chat"));
            reopened.commit(reopened.newBatch().put(NAMES, "after", "v")).awaitDurable();
        }
        // After a loss of power, the durable commits still
        var survivor = disk.afterPowerLoss(new Random(1));
        try (var recovered = options(survivor.getPath("/store")).build()) {
            for (var i = 0; i < 20; i++) {
                assertEquals(Optional.of("v"), recovered.get(NAMES, "durable" + i));
            }
            assertEquals(Optional.of("v"), recovered.get(NAMES, "after"));
        }
    }

    @Test
    @DisplayName("a store in memory never stops, and a store whose log works never reports a stop")
    void noStop() throws Exception {
        var stops = new CopyOnWriteArrayList<LedgerIoException>();
        try (var ledger = Ledger.builder().memory().keyspace(NAMES).onStop(stops::add).build()) {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "b")).awaitDurable();
        }
        var disk = CrashFileSystem.create();
        try (var ledger = options(disk.getPath("/store")).onStop(stops::add).build()) {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "b")).awaitDurable();
            ledger.checkpoint();
        }
        assertTrue(stops.isEmpty());
    }
}

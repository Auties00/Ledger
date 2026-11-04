package com.github.auties00.ledger.benchmark.engine;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/// Checks every engine, sealed and plain, against the contract of [Engine]: a benchmark that compares engines is
/// only as fair as their adapters are equivalent.
class EngineConformanceTest {
    // Keys whose order differs between signed and unsigned comparison, and prefixes of each other
    private static final List<byte[]> ORDERED_KEYS = List.of(
            bytes(0x00), bytes(0x01), bytes(0x7F), bytes(0x80), bytes(0x80, 0x00), bytes(0xC8), bytes(0xFF), bytes(0xFF, 0xFF));

    static Stream<Arguments> engines() {
        return Arrays.stream(EngineKind.values())
                .flatMap(kind -> Stream.of(Arguments.of(kind, false), Arguments.of(kind, true)));
    }

    private static byte[] bytes(int... values) {
        var result = new byte[values.length];
        for (var i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }

    private static byte[] value(String text) {
        return text.getBytes();
    }

    private static Engine open(EngineKind kind, boolean sealed, Path directory) throws IOException {
        var engine = kind.open(directory, EngineSettings.DEFAULT);
        return sealed && !kind.sealsItself() ? new SealedEngine(engine) : engine;
    }

    private static List<String> scan(Engine engine, byte[] from, byte[] to, boolean descending, int limit) throws IOException {
        var keys = new ArrayList<String>();
        engine.scan(Keyspace.GENERIC, from, to, descending, limit, (key, value) -> {
            keys.add(HexFormat.of().formatHex(key));
            return true;
        });
        return keys;
    }

    private static List<String> hex(List<byte[]> keys) {
        return keys.stream().map(HexFormat.of()::formatHex).toList();
    }

    private static void putAll(Engine engine) throws IOException {
        var batch = new WriteBatch();
        // Written out of order, so the engine has to sort them
        for (var i = ORDERED_KEYS.size() - 1; i >= 0; i--) {
            batch.put(Keyspace.GENERIC, ORDERED_KEYS.get(i), value("v" + i));
        }
        engine.write(batch, Durability.DURABLE);
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("scans order keys as unsigned bytes, in both directions")
    void unsignedOrder(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            putAll(engine);
            var everything = bytes(0xFF, 0xFF, 0xFF);
            assertEquals(hex(ORDERED_KEYS), scan(engine, bytes(0x00), everything, false, 100));
            assertEquals(hex(ORDERED_KEYS.reversed()), scan(engine, bytes(0x00), everything, true, 100));
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("a scan includes its lower bound and excludes its upper bound, even above 0x7F")
    void halfOpenBounds(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            putAll(engine);
            var expected = hex(List.of(bytes(0x80), bytes(0x80, 0x00), bytes(0xC8)));
            assertEquals(expected, scan(engine, bytes(0x80), bytes(0xFF), false, 100));
            assertEquals(expected.reversed(), scan(engine, bytes(0x80), bytes(0xFF), true, 100));
            assertEquals(List.of(), scan(engine, bytes(0x02), bytes(0x7F), false, 100));
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("a scan stops at its limit and when the visitor says so")
    void scanStops(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            putAll(engine);
            assertEquals(hex(ORDERED_KEYS.subList(0, 2)), scan(engine, bytes(0x00), bytes(0xFF, 0xFF, 0xFF), false, 2));
            var visited = engine.scan(Keyspace.GENERIC, bytes(0x00), bytes(0xFF, 0xFF, 0xFF), true, 100, (key, value) -> false);
            assertEquals(1, visited);
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("a batch overwrites and deletes in order, and values read back intact")
    void overwriteAndDelete(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            engine.write(new WriteBatch()
                    .put(Keyspace.GENERIC, bytes(1), value("first"))
                    .put(Keyspace.GENERIC, bytes(1), value("second"))
                    .put(Keyspace.GENERIC, bytes(2), value("doomed"))
                    .delete(Keyspace.GENERIC, bytes(2))
                    .delete(Keyspace.GENERIC, bytes(3)), Durability.DEFERRED);
            assertArrayEquals(value("second"), engine.get(Keyspace.GENERIC, bytes(1)));
            assertNull(engine.get(Keyspace.GENERIC, bytes(2)));
            assertNull(engine.get(Keyspace.GENERIC, bytes(3)));
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("a range delete removes exactly the range")
    void deleteRange(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            putAll(engine);
            engine.deleteRange(Keyspace.GENERIC, bytes(0x7F), bytes(0xC8), Durability.DURABLE);
            assertEquals(hex(List.of(bytes(0x00), bytes(0x01), bytes(0xC8), bytes(0xFF), bytes(0xFF, 0xFF))),
                    scan(engine, bytes(0x00), bytes(0xFF, 0xFF, 0xFF), false, 100));
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("key spaces are separate")
    void separateKeyspaces(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            engine.write(new WriteBatch()
                    .put(Keyspace.MESSAGES, bytes(9), value("message"))
                    .put(Keyspace.SESSIONS, bytes(9), value("session")), Durability.DURABLE);
            assertArrayEquals(value("message"), engine.get(Keyspace.MESSAGES, bytes(9)));
            assertArrayEquals(value("session"), engine.get(Keyspace.SESSIONS, bytes(9)));
            assertNull(engine.get(Keyspace.CHATS, bytes(9)));
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("a read view keeps seeing the version it opened on")
    void readViewIsolation(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        try (var engine = open(kind, sealed, directory)) {
            engine.write(new WriteBatch().put(Keyspace.GENERIC, bytes(5), value("before")), Durability.DURABLE);
            Engine.ReadView view;
            try {
                view = engine.openReadView();
            } catch (UnsupportedOperationException exception) {
                Assumptions.abort(kind.id() + " has no read views");
                return;
            }
            try (view) {
                engine.write(new WriteBatch()
                        .put(Keyspace.GENERIC, bytes(5), value("after"))
                        .put(Keyspace.GENERIC, bytes(6), value("new")), Durability.DURABLE);
                assertArrayEquals(value("before"), view.get(Keyspace.GENERIC, bytes(5)));
                assertNull(view.get(Keyspace.GENERIC, bytes(6)));
            }
            assertArrayEquals(value("after"), engine.get(Keyspace.GENERIC, bytes(5)));
        }
    }

    @ParameterizedTest(name = "{0}, sealed={1}")
    @MethodSource("engines")
    @DisplayName("durable writes survive closing and reopening")
    void survivesReopen(EngineKind kind, boolean sealed, @TempDir Path directory) throws IOException {
        Assumptions.assumeFalse(kind == EngineKind.FLOOR, "The floor keeps nothing across a reopen");
        try (var engine = open(kind, sealed, directory)) {
            putAll(engine);
        }
        try (var engine = open(kind, sealed, directory)) {
            assertEquals(hex(ORDERED_KEYS), scan(engine, bytes(0x00), bytes(0xFF, 0xFF, 0xFF), false, 100));
            assertArrayEquals(value("v0"), engine.get(Keyspace.GENERIC, ORDERED_KEYS.getFirst()));
        }
    }
}

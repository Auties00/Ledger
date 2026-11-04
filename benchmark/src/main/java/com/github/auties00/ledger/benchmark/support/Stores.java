package com.github.auties00.ledger.benchmark.support;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.SealedEngine;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/// Opens the stores the workloads run against, under the directory the `ledger.bench.dir` system property or the
/// `LEDGER_BENCH_DIR` environment variable names (`bench-data` in the working directory by default). JMH forks do
/// not inherit system properties, so the environment variable is the simpler way to set it.
///
/// Put that directory on the device under test: durable writes cost what its forces cost.
public final class Stores {
    /// The number of messages a loaded store holds unless a workload says otherwise.
    public static final int DEFAULT_MESSAGES = 200_000;
    /// The number of Signal sessions a loaded store holds.
    public static final int SESSIONS = 2_000;
    /// The size of a Signal session record.
    public static final int SESSION_BYTES = 1_536;
    /// The size of a chat's metadata record.
    public static final int CHAT_BYTES = 320;

    private static final int INGEST_BATCH = 5_000;
    private static final String COMPLETE_MARKER = ".complete";
    private static final AtomicBoolean STALE_RUNS_DELETED = new AtomicBoolean();

    private Stores() {
    }

    /// {@return the directory every store lives under}
    public static Path root() {
        var configured = System.getProperty("ledger.bench.dir", System.getenv("LEDGER_BENCH_DIR"));
        return Path.of(configured != null ? configured : "bench-data").toAbsolutePath();
    }

    /// {@return the settings of a store holding `corpus`}
    ///
    /// @param corpus the messages
    public static EngineSettings settingsFor(Corpus corpus) {
        return EngineSettings.DEFAULT.withExpectedBytes(corpus.messages() * 2_048L);
    }

    /// Opens an empty store in a fresh directory.
    ///
    /// @param kind the engine
    /// @param settings the shared settings
    /// @param sealed whether to seal values with AES-GCM before they reach the engine
    /// @return the store, whose directory is deleted when it closes
    /// @throws IOException if the engine fails to open
    public static Store empty(EngineKind kind, EngineSettings settings, boolean sealed) throws IOException {
        var directory = runDirectory(kind);
        return new Store(openAt(kind, directory, settings, sealed), directory);
    }

    /// Opens a store holding `corpus`, copied from a template built the first time it is asked for.
    ///
    /// The template holds the messages, the id index, one metadata record per chat and [#SESSIONS] sessions. Its
    /// values are sealed when `sealed` is set; a template is built for each combination.
    ///
    /// @param kind the engine
    /// @param settings the shared settings
    /// @param sealed whether values are sealed with AES-GCM
    /// @param corpus the messages
    /// @return the store, whose directory is deleted when it closes
    /// @throws IOException if the template cannot be built or copied, or the engine fails to open
    public static Store loaded(EngineKind kind, EngineSettings settings, boolean sealed, Corpus corpus) throws IOException {
        var directory = copyOf(kind, settings, sealed, corpus);
        return new Store(openAt(kind, directory, settings, sealed), directory);
    }

    /// Copies the template holding `corpus` to a fresh directory, without opening it.
    ///
    /// @param kind the engine
    /// @param settings the shared settings
    /// @param sealed whether values are sealed with AES-GCM
    /// @param corpus the messages
    /// @return the directory of the copy, which the caller deletes
    /// @throws IOException if the template cannot be built or copied
    public static Path copyOf(EngineKind kind, EngineSettings settings, boolean sealed, Corpus corpus) throws IOException {
        if (kind == EngineKind.FLOOR) {
            throw new IllegalArgumentException("The floor keeps nothing across a reopen, so it has no loaded store");
        }
        var template = template(kind, settings, sealed, corpus);
        var directory = runDirectory(kind);
        Directories.copy(template, directory);
        return directory;
    }

    /// {@return a fresh directory for one store}
    ///
    /// The first call in a JVM deletes the directories earlier runs left behind: a trial whose setup or benchmark
    /// fails never reaches its teardown. JMH runs one fork at a time, so no other run is using them; two benchmark
    /// runs must not share one bench directory.
    ///
    /// @param kind the engine that will own the directory
    /// @throws IOException if the leftovers cannot be deleted
    private static Path runDirectory(EngineKind kind) throws IOException {
        var runs = root().resolve("run");
        if (STALE_RUNS_DELETED.compareAndSet(false, true)) {
            Directories.delete(runs);
        }
        return runs.resolve(kind.id() + "-" + UUID.randomUUID());
    }

    /// {@return the directory of the template holding `corpus`, building it if needed}
    ///
    /// @param kind the engine
    /// @param settings the shared settings
    /// @param sealed whether values are sealed with AES-GCM
    /// @param corpus the messages
    /// @throws IOException if the template cannot be built
    public static Path template(EngineKind kind, EngineSettings settings, boolean sealed, Corpus corpus) throws IOException {
        var template = root().resolve("templates").resolve(kind.id() + (sealed ? "-sealed-" : "-") + corpus.messages());
        if (Files.exists(template.resolve(COMPLETE_MARKER))) {
            return template;
        }
        Directories.delete(template);
        try (var engine = openAt(kind, template, settings, sealed)) {
            ingest(engine, corpus, INGEST_BATCH);
            var batch = new WriteBatch();
            for (var chat = 0; chat < corpus.chats(); chat++) {
                batch.put(Keyspace.CHATS, Corpus.chatStart(chat), corpus.slice(chat, CHAT_BYTES));
            }
            for (var session = 0; session < SESSIONS; session++) {
                batch.put(Keyspace.SESSIONS, Corpus.sessionKey(session), corpus.slice(~session, SESSION_BYTES));
            }
            engine.write(batch, Durability.DURABLE);
        }
        // The marker goes last, so an interrupted build is redone
        Files.createFile(template.resolve(COMPLETE_MARKER));
        return template;
    }

    /// Opens a store holding `keys` entries of `valueSize` bytes in [Keyspace#GENERIC], copied from a template built
    /// the first time it is asked for.
    ///
    /// Entry `i` is stored under [Corpus#hashedKey(long, int)] of `i` with length 16, so keys spread uniformly.
    ///
    /// @param kind the engine
    /// @param settings the shared settings
    /// @param keys the number of entries
    /// @param valueSize the size of each value
    /// @return the store, whose directory is deleted when it closes
    /// @throws IOException if the template cannot be built or copied, or the engine fails to open
    public static Store loadedGeneric(EngineKind kind, EngineSettings settings, int keys, int valueSize) throws IOException {
        if (kind == EngineKind.FLOOR) {
            throw new IllegalArgumentException("The floor keeps nothing across a reopen, so it has no loaded store");
        }
        var template = root().resolve("templates").resolve("generic-" + kind.id() + "-" + keys + "-" + valueSize);
        if (!Files.exists(template.resolve(COMPLETE_MARKER))) {
            Directories.delete(template);
            var corpus = new Corpus(1);
            try (var engine = openAt(kind, template, settings, false)) {
                var batch = new WriteBatch();
                for (var key = 0; key < keys; key++) {
                    batch.put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(key, valueSize));
                    if (batch.size() == INGEST_BATCH * 2) {
                        engine.write(batch, Durability.DEFERRED);
                        batch.clear();
                    }
                }
                engine.write(batch, Durability.DURABLE);
            }
            Files.createFile(template.resolve(COMPLETE_MARKER));
        }
        var directory = runDirectory(kind);
        Directories.copy(template, directory);
        return new Store(openAt(kind, directory, settings, false), directory);
    }

    /// Writes every message of `corpus` and its id index entry in ingest order, `batchSize` messages per deferred
    /// batch, then makes them durable.
    ///
    /// @param engine the engine to write
    /// @param corpus the messages
    /// @param batchSize the messages per batch
    /// @throws IOException if the engine fails to write
    public static void ingest(Engine engine, Corpus corpus, int batchSize) throws IOException {
        var batch = new WriteBatch();
        for (var index = 0; index < corpus.messages(); index++) {
            var chat = corpus.chat(index);
            var sequence = corpus.sequence(index);
            var key = corpus.messageKey(chat, sequence);
            batch.put(Keyspace.MESSAGES, key, corpus.body(chat, sequence));
            batch.put(Keyspace.MESSAGE_IDS, corpus.idKey(chat, sequence), key);
            if (batch.size() >= batchSize * 2) {
                engine.write(batch, Durability.DEFERRED);
                batch.clear();
            }
        }
        engine.write(batch, Durability.DURABLE);
    }

    /// Opens the engine over `directory`.
    ///
    /// @param kind the engine
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @param sealed whether to seal values with AES-GCM before they reach the engine
    /// @return the open engine
    /// @throws IOException if the engine fails to open
    public static Engine openAt(EngineKind kind, Path directory, EngineSettings settings, boolean sealed) throws IOException {
        var engine = kind.open(directory, settings);
        return sealed && !kind.sealsItself() ? new SealedEngine(engine) : engine;
    }

    /// An open engine and the directory it owns.
    ///
    /// @param engine the engine
    /// @param directory the directory the engine owns
    public record Store(Engine engine, Path directory) implements AutoCloseable {
        /// Closes the engine and deletes its directory.
        ///
        /// @throws IOException if the engine fails to close or the directory cannot be deleted
        @Override
        public void close() throws IOException {
            try {
                engine.close();
            } finally {
                Directories.delete(directory);
            }
        }
    }
}

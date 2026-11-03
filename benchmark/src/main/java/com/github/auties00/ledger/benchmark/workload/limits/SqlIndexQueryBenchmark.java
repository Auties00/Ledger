package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.support.Directories;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/// The question of [AdHocQueryBenchmark], asked of SQLite with the messages in a relational table.
///
/// The table is `messages(chat, ts, id, starred, body)` with key `(chat, ts, id)` and holds the same corpus.
/// [#createIndex(IndexState)] creates the index on `(starred, ts)` the question needs, as an application would the
/// first time it asks; [#query(QueryState, Blackhole)] answers the question through that index.
///
/// Not a Cobalt workload: it shows what an engine with a query planner answers that a key-value engine cannot
/// without scanning.
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class SqlIndexQueryBenchmark {
    private static final String QUERY = "SELECT chat, ts, id FROM messages WHERE starred = 1 ORDER BY ts DESC LIMIT 50";

    /// Creates the index the question needs.
    ///
    /// @param state the database without the index
    /// @throws SQLException if SQLite fails
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1)
    @Measurement(iterations = 5)
    public void createIndex(IndexState state) throws SQLException {
        try (var statement = state.connection.createStatement()) {
            statement.execute("CREATE INDEX starred_by_time ON messages (starred, ts)");
        }
    }

    /// Answers the question through the index.
    ///
    /// @param state the database with the index
    /// @param blackhole consumes the answer
    /// @throws SQLException if SQLite fails
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 2, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void query(QueryState state, Blackhole blackhole) throws SQLException {
        try (var results = state.query.executeQuery()) {
            while (results.next()) {
                blackhole.consume(results.getBytes(3));
            }
        }
    }

    /// Fills a SQLite database with `messages` messages of the corpus.
    private static Connection load(Path directory, int messages) throws IOException, SQLException {
        Files.createDirectories(directory);
        var connection = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("messages.db"));
        try (var statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode = WAL");
            statement.execute("PRAGMA cache_size = -262144");
            statement.execute("CREATE TABLE messages (chat INTEGER, ts INTEGER, id BLOB, starred INTEGER, body BLOB, PRIMARY KEY (chat, ts, id)) WITHOUT ROWID");
        }
        var corpus = new Corpus(messages);
        connection.setAutoCommit(false);
        try (var insert = connection.prepareStatement("INSERT INTO messages VALUES (?, ?, ?, ?, ?)")) {
            for (var index = 0; index < corpus.messages(); index++) {
                var chat = corpus.chat(index);
                var sequence = corpus.sequence(index);
                var body = corpus.body(chat, sequence);
                insert.setInt(1, chat);
                insert.setLong(2, corpus.timestamp(chat, sequence));
                insert.setBytes(3, corpus.id(chat, sequence));
                insert.setInt(4, Corpus.starred(body) ? 1 : 0);
                insert.setBytes(5, body);
                insert.executeUpdate();
            }
        }
        connection.commit();
        connection.setAutoCommit(true);
        return connection;
    }

    /// A database holding the corpus without the index; the index is dropped after every iteration.
    @State(Scope.Benchmark)
    public static class IndexState {
        /// The messages the database holds.
        @Param({"200000"})
        public int messages;

        Connection connection;
        private Path directory;

        /// Fills the database.
        ///
        /// @throws IOException if the directory cannot be created
        /// @throws SQLException if SQLite fails
        @Setup(Level.Trial)
        public void open() throws IOException, SQLException {
            directory = Stores.root().resolve("run").resolve("sql-" + UUID.randomUUID());
            connection = load(directory, messages);
        }

        /// Drops the index the iteration created.
        ///
        /// @throws SQLException if SQLite fails
        @TearDown(Level.Iteration)
        public void dropIndex() throws SQLException {
            try (var statement = connection.createStatement()) {
                statement.execute("DROP INDEX IF EXISTS starred_by_time");
            }
        }

        /// Closes the database and deletes it.
        ///
        /// @throws IOException if the directory cannot be deleted
        /// @throws SQLException if SQLite fails
        @TearDown(Level.Trial)
        public void close() throws IOException, SQLException {
            connection.close();
            Directories.delete(directory);
        }
    }

    /// A database holding the corpus and the index.
    @State(Scope.Benchmark)
    public static class QueryState {
        /// The messages the database holds.
        @Param({"200000"})
        public int messages;

        Connection connection;
        PreparedStatement query;
        private Path directory;

        /// Fills the database and creates the index.
        ///
        /// @throws IOException if the directory cannot be created
        /// @throws SQLException if SQLite fails
        @Setup(Level.Trial)
        public void open() throws IOException, SQLException {
            directory = Stores.root().resolve("run").resolve("sql-" + UUID.randomUUID());
            connection = load(directory, messages);
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE INDEX starred_by_time ON messages (starred, ts)");
            }
            query = connection.prepareStatement(QUERY);
        }

        /// Closes the database and deletes it.
        ///
        /// @throws IOException if the directory cannot be deleted
        /// @throws SQLException if SQLite fails
        @TearDown(Level.Trial)
        public void close() throws IOException, SQLException {
            query.close();
            connection.close();
            Directories.delete(directory);
        }
    }
}

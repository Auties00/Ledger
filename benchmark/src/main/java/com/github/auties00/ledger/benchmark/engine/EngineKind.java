package com.github.auties00.ledger.benchmark.engine;

import com.github.auties00.ledger.benchmark.engine.floor.AppendLogFloor;
import com.github.auties00.ledger.benchmark.engine.je.BerkeleyJeEngine;
import com.github.auties00.ledger.benchmark.engine.ledger.LedgerEngine;
import com.github.auties00.ledger.benchmark.engine.lmdb.LmdbEngine;
import com.github.auties00.ledger.benchmark.engine.mvstore.MvStoreEngine;
import com.github.auties00.ledger.benchmark.engine.rocksdb.RocksDbEngine;
import com.github.auties00.ledger.benchmark.engine.sqlite.SqliteEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/// The engines a workload can run against, by the name its `engine` parameter takes.
public enum EngineKind {
    /// Ledger, encrypting its files with AES-256-GCM: a log of commits checkpointed into copy-on-write B+trees. It
    /// authenticates every frame and page itself, so the workloads do not seal its values again.
    LEDGER("ledger", false, true) {
        @Override
        public Engine open(Path directory, EngineSettings settings) {
            return LedgerEngine.open(directory, settings, true);
        }
    },
    /// Ledger without encryption, which the workloads seal like the other engines when they seal values.
    LEDGER_PLAIN("ledger-plain", false) {
        @Override
        public Engine open(Path directory, EngineSettings settings) {
            return LedgerEngine.open(directory, settings, false);
        }
    },
    /// H2 MVStore, the engine under Cobalt's current store: a copy-on-write B-tree written as append-only chunks.
    MVSTORE("mvstore", false) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return MvStoreEngine.open(directory, settings, null);
        }
    },
    /// H2 MVStore with its own XTS-AES encryption, as Cobalt's current store configures it.
    MVSTORE_ENCRYPTED("mvstore-encrypted", false) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return MvStoreEngine.open(directory, settings, "ledger-benchmark".toCharArray());
        }
    },
    /// RocksDB through JNI: a log-structured merge tree with a write-ahead log and group commit.
    ROCKSDB("rocksdb", true) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return RocksDbEngine.open(directory, settings);
        }
    },
    /// LMDB through lmdbjava: a copy-on-write B+tree read through a memory map, one writer at a time.
    LMDB("lmdb", true) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return LmdbEngine.open(directory, settings);
        }
    },
    /// SQLite through the xerial driver, in WAL mode, one table per key space.
    SQLITE("sqlite", true) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return SqliteEngine.open(directory, settings);
        }
    },
    /// Berkeley DB Java Edition: a pure-Java B+tree inside an append-only log, with group commit.
    BERKELEY_JE("berkeley-je", false) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return BerkeleyJeEngine.open(directory, settings);
        }
    },
    /// Not an engine: an append-only file with the whole index in memory, no recovery and no compaction. It shows
    /// what the device charges for writing and forcing the same bytes, the bound every engine pays on top of.
    FLOOR("floor", false) {
        @Override
        public Engine open(Path directory, EngineSettings settings) throws IOException {
            return AppendLogFloor.open(directory);
        }
    };

    private final String id;
    private final boolean nativeCode;
    private final boolean sealsItself;

    EngineKind(String id, boolean nativeCode) {
        this(id, nativeCode, false);
    }

    EngineKind(String id, boolean nativeCode, boolean sealsItself) {
        this.id = id;
        this.nativeCode = nativeCode;
        this.sealsItself = sealsItself;
    }

    /// {@return whether the engine encrypts and authenticates what it stores, so sealing its values again would only
    /// add the cost of a second seal}
    public boolean sealsItself() {
        return sealsItself;
    }

    /// Opens the engine over `directory`, creating its files when the directory is empty.
    ///
    /// @param directory the directory the engine owns
    /// @param settings the shared settings
    /// @return the open engine
    /// @throws IOException if the engine fails to open
    public abstract Engine open(Path directory, EngineSettings settings) throws IOException;

    /// {@return the name the `engine` parameter of a workload takes}
    public String id() {
        return id;
    }

    /// {@return whether the engine runs native code, which Cobalt cannot ship}
    public boolean nativeCode() {
        return nativeCode;
    }

    /// {@return the engine named `id`}
    ///
    /// @param id the name, as [#id()] returns it
    /// @throws IllegalArgumentException if no engine has that name
    public static EngineKind of(String id) {
        var normalized = id.toLowerCase(Locale.ROOT);
        for (var kind : values()) {
            if (kind.id.equals(normalized)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("Unknown engine " + id + ", expected one of " + Arrays.stream(values())
                .map(EngineKind::id)
                .collect(Collectors.joining(", ")));
    }
}

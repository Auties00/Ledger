# Ledger

An embedded storage engine for Java applications with one writing process that need their data durable, encrypted and ordered: a group-committed log with a durability barrier, byte-ordered key spaces, atomic batches and authenticated encryption, in pure Java.

Ledger is in development, and has no release yet. The repository holds:

- `core`, the engine: a group-committed log of AES-256-GCM frames, checkpointed into copy-on-write B+trees in a sealed data file, with snapshots, transactions, range removals and exact counts;
- `testkit`, a file system in memory that simulates losses of power and crashes of the process, which Ledger's crash tests run on and which other stores can use too;
- the [benchmark](benchmark/README.md), which compares Ledger with MVStore, RocksDB, LMDB, SQLite and Berkeley DB on workloads modelled on [Cobalt](https://github.com/Auties00/Cobalt) and on workloads Ledger is not built for. The [Benchmark workflow](.github/workflows/benchmark.yml) runs it on Linux, Windows and macOS.

Ledger requires Java 26.

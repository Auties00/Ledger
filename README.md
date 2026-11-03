# Ledger

An embedded storage engine for Java applications with one writing process that need their data durable, encrypted and ordered: a group-committed log with a durability barrier, byte-ordered key spaces, atomic batches and authenticated encryption, in pure Java.

Ledger is in development. This repository currently holds its [benchmark](benchmark/README.md), which compares the engines Ledger is measured against, MVStore, RocksDB, LMDB, SQLite and Berkeley DB, on workloads modelled on [Cobalt](https://github.com/Auties00/Cobalt) and on workloads Ledger is not built for. The [Benchmark workflow](.github/workflows/benchmark.yml) runs it on Linux, Windows and macOS.

Ledger requires Java 26.

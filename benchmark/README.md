# Ledger benchmark

JMH workloads that compare Ledger with other embedded storage engines. Some workloads model what Cobalt asks of its store; the others probe what Ledger deliberately does not do, so the results show where it loses as well as where it wins.

## Engines

| `engine` | What it is | Pure Java |
|---|---|---|
| `ledger` | Ledger, encrypting its files with AES-256-GCM: a group-committed log checkpointed into copy-on-write B+trees; sessions and chats resident, the rest paged | yes |
| `ledger-plain` | The same without encryption | yes |
| `ledger-resident-at-open` | `ledger`, reading its resident key spaces while it opens; only when `-p engine` names it, to compare the two ways of loading them | yes |
| `mvstore` | H2 MVStore 2.5.252, configured as Cobalt's current store configures it | yes |
| `mvstore-encrypted` | The same, with MVStore's XTS-AES file encryption (not authenticated) | yes |
| `rocksdb` | RocksDB 11.8.1 through JNI: an LSM tree with a write-ahead log and group commit | no |
| `lmdb` | LMDB 0.9.33 through lmdbjava: a copy-on-write B+tree read through a memory map | no |
| `sqlite` | SQLite 3.54 through the xerial driver, WAL mode, one `WITHOUT ROWID` table per key space | no |
| `berkeley-je` | Berkeley DB Java Edition 18.3.12: a B+tree inside an append-only log, with group commit | yes |
| `floor` | Not an engine: an append-only file with the index in memory. It bounds what the device charges for the same bytes and forces | yes |

Every engine gets the same contract (`engine.Engine`):
- byte-ordered key spaces;
- atomic batches;
- ordered scans in both directions;
- range deletes;
- read views.

Each engine also gets the same cache budget, 256 MiB.

Durability means the same thing everywhere:

- **`DEFERRED`**: atomic and visible, and it survives a crash of the process, but not necessarily a power loss.
- **`DURABLE`**: on stable storage when the call returns.

How each engine implements both levels is documented on its adapter. Cobalt seals everything it stores, so the Cobalt workloads seal values with AES-GCM (`sealed=true`) before they reach engines that cannot; `ledger` authenticates every frame and page itself and is not sealed again, while `ledger-plain` is. `-p sealed=false` measures the engines bare.

## Workloads

Cobalt's workloads, in `workload.cobalt`:

| Benchmark | What it measures |
|---|---|
| `DurableCommitBenchmark` | Latency of one durable 1.5 KB write: the session stored before each send and each acknowledgement |
| `GroupCommitBenchmark` | Durable writes per second from 1 to 256 concurrent virtual threads: whether concurrent committers share forces |
| `OfflineDrainBenchmark` | Latency of storing one received message (body, id index, session, chat) durably, over an existing history |
| `HistoryIngestBenchmark` | Time to store a whole history sync, plus write and space amplification |
| `ChatPageBenchmark` | Newest page of a chat and a page deeper in its history, optionally while messages arrive |
| `MessageLookupBenchmark` | Finding a message by id anywhere in the history |
| `AppStateSyncBenchmark` | Applying a 20,000-entry app-state snapshot atomically, then small patches |
| `WebhookInboxBenchmark` | Cloud webhook deliveries: stored durably, removed once handled, four at a time |
| `ReopenBenchmark` | Startup over an existing store, and the memory the warm store holds |
| `ChatDeleteBenchmark` | Clearing the most active chat |

Workloads Ledger is not built for, in `workload.limits`:

| Benchmark | What it shows |
|---|---|
| `RandomOverwriteBenchmark` | Random updates over a data set larger than the cache, where LSM trees beat B-trees |
| `ParallelWritersBenchmark` | Eight threads writing at once, which single-writer engines serialize |
| `MultiProcessWritersBenchmark` | Several processes writing one store, which Ledger does not allow (SQLite and LMDB only) |
| `AdHocQueryBenchmark` | A question no key space indexes, answered by scanning everything |
| `SqlIndexQueryBenchmark` | The same question answered by SQLite with an index created on the spot |
| `LargeValueBenchmark` | Values of 1 and 16 MiB |
| `PinnedReaderBenchmark` | File growth while a long reader pins an old version |

Memory, in `workload.memory`:

| Benchmark | What it measures |
|---|---|
| `SessionMemoryBenchmark` | The heap and resident memory a store holds after a client's session over a history of 200,000 and 1,000,000 messages |
| `OpenStoreMemoryBenchmark` | What one open store costs, empty or nearly so, alone and with eight open at once, per store |
| `OperationMemoryBenchmark` | The peak memory of a history ingest, a checkpoint under load, an app-state snapshot, a recovery after a crash and large values, and what each leaves |
| `SteadyWritesMemoryBenchmark` | What a store holds of writes it has not checkpointed yet while messages keep arriving |
| `MemoryPipelineBenchmark` | A design measurement: whether Ledger keeps the bytes it encrypts and writes on the heap or in native memory, since AES-GCM in the JDK is fastest on arrays and file I/O on native memory |

`ReopenBenchmark.footprint` measures memory too, in a JVM with a fixed heap like every other workload. The other memory workloads run in a JVM whose heap starts at 64 MiB and grows up to the same 4 GiB, so the heap the JVM commits, and its resident memory, follow what the engine holds. Each records the heap after a full collection and the resident memory before the store opens (`heapBytesFloor`, `residentBytesFloor`: the JVM, the corpus and the workload's input) and with the store open at the end (`heapBytes`, `residentBytes`); the peak of the heap left by the collections the JVM ran (`heapBytesPeak`, an upper bound of the live heap) and of resident memory (`residentBytesPeak`, sampled every 5 ms, and on Linux also the kernel's peak); and the heap committed at the end (`heapBytesCommitted`). Their time is not the point.

## Running

```
mvn -q package
java -jar benchmark/target/benchmarks.jar -l
java -jar benchmark/target/benchmarks.jar DurableCommit -p engine=rocksdb,sqlite
```

Stores live under `LEDGER_BENCH_DIR` (or `-Dledger.bench.dir`, which JMH forks do not inherit), `bench-data` in the working directory by default. Put it on the device you want to measure: durable writes cost what its forces cost.

Workloads over an existing history copy a template store, built once per engine and size under `templates/`. The first run of each combination therefore takes longer.

Run one benchmark process per bench directory at a time. Each run deletes the stores that earlier runs left in `run/` when a trial failed before its teardown.

The JMH options that matter:
- `-p name=value,...` overrides a parameter;
- `-rf json -rff results.json` keeps the results;
- `-prof gc` adds allocation rates.

A full run takes several hours. A quick pass to check that everything works: `-wi 0 -i 1 -r 1s -p messages=20000`.

On GitHub, the Benchmark workflow runs the full suite on Linux, Windows and macOS. Start it from the Actions tab, or with `gh workflow run benchmark.yml`; its inputs pick the runners and add JMH arguments. Each job's summary has its results, the report job's summary compares the runners, and the JSON is kept as artifacts for 90 days. Every push also runs each workload once on small data (the CI workflow), to catch breakage.

## Reading the numbers

- **Results move with the device and the OS.** A force on a consumer NVMe drive costs about a millisecond on Windows and less on Linux. Compare engines within one machine.
- **On macOS, every engine's durable write empties the drive's cache.** The Java engines (Ledger, MVStore, Berkeley DB, the floor) force through `FileChannel.force`, which the JDK implements with `fcntl(F_FULLFSYNC)` there; LMDB and RocksDB call it themselves, and SQLite does with its `fullfsync` pragmas, which the adapter sets.
- **JMH adds the secondary counters up over the measurement iterations.** In JMH's own output and JSON, `bytesWritten`, `payloadBytes`, `sizeOnDisk` and every `heapBytes` and `residentBytes` counter are the sum of the iterations; the workflow summaries divide them back to their value per iteration.
- **`bytesWritten` is not measured the same way on each OS.** On Windows and Linux it counts the bytes passed to write calls; on macOS, the bytes written to disk. See `support.ProcessProbe`.
- **`ReopenBenchmark` measures a warm start on Linux and macOS.** The files it opens were just copied, so they are in the OS page cache. On Windows the reads after the copy reach the drive, as the profiles of the hosted runners show, so the start there is closer to a cold one.
- **LMDB's `sizeOnDisk` counts the pages in use.** On Windows, LMDB 0.9 creates its data file at the full map size, which the adapter sets to twice the data a workload expects.

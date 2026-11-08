package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerException;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/// Puts in memory, in key order, the entries the tree of a resident key space holds, once the store has opened.
///
/// No commit writes a key the loader has not reached, and memory holds no chain of one: a write waits for the loader
/// to pass its key ([#await(MemorySegment)]), and a read reads the key from the tree of its own checkpoint, which holds
/// the key's value as the read sees it, since no commit changed the key and no range removal covered it while the
/// loader had not reached it. The loader reads the leaves of the tree of a read it registers, so their pages stay
/// that tree's while it reads them, and installs the entries of each run of leaves as chains whose only version is
/// stamped as the checkpoint the store opened from, below the bound of every read and the stamp of every later write.
/// The volatile store of [#below] after a run publishes it: a read or a write of a key below it finds the key's chain,
/// or no chain if the key has no entry, as in a key space loaded while the store opened.
///
/// It runs on a virtual thread of its own from the first use of its key space ([#start()]), and reads the leaves on
/// [#THREADS] threads, which leave the other processors to the reads of the store and to the compiler, busy after an
/// open; or on the opening thread ([#loadNow()]), on as many threads as the data file has read channels. Once a thread
/// waits for it or a long scan reads beside it ([#awaitRun(MemorySegment)]), its next runs take every read channel too,
/// since a read or a write is waiting for them.
final class ResidentLoader {
    /// The threads a loader running beside the store reads the leaves on.
    private static final int THREADS = 2;
    /// The leaves of the first run a loader running beside the store installs: the first keys reach memory soon after
    /// the first use, and each run after takes twice as many, up to one [PageFile#readAll(List, byte, int)] call per
    /// thread.
    private static final int FIRST_RUN = 16;
    private static final int IDLE = 0;
    private static final int RUNNING = 1;
    private static final int FAILED = 2;
    private static final int DONE = 3;
    /// Sorts before every key: a key is at least one byte.
    private static final MemorySegment NOTHING = MemorySegment.ofArray(new byte[0]).asReadOnly();
    /// The value of [#reading] for a run that reaches the last leaf, by identity.
    private static final MemorySegment LAST_LEAF = MemorySegment.ofArray(new byte[0]).asReadOnly();

    private final LiveLedger ledger;
    private final PageFile pages;
    private final KeyspaceState state;
    // The stamp of the versions the loader installs: the sequence of the checkpoint the store opened from
    private final long stamp;
    private final Consumer<? super LedgerException> failures;
    // Guards the thread, the attempts and the failure, and wakes the writes waiting for a key
    private final ReentrantLock lock;
    private final Condition progress;
    // Memory holds every entry of a key below it; only grows, written by the loader after each run it installs
    private volatile MemorySegment below;
    // The first key after the run the loader is reading, or null while it reads none; written by the loader
    private volatile MemorySegment reading;
    // Set once a thread needs the loader to go faster: its next runs take every read channel
    private volatile boolean hurried;
    // Set once memory holds every entry, after the last run
    private volatile boolean complete;
    // IDLE before the first use, or after a stop; written holding the lock
    private volatile int status;
    // Set as the store closes: the loader stops before its next run, and no thread starts it again
    private volatile boolean stopping;
    // The thread of the last attempt, guarded by the lock
    private Thread thread;
    // The attempts started and the last that failed, with its failure; guarded by the lock
    private int attempts;
    private int failedAttempt;
    private RuntimeException failure;
    /// Called after the loader registered its read, with 0, and after each run it installs, with the runs installed so
    /// far; for tests that pause it there.
    volatile IntConsumer stepHookForTests;

    /// Creates the loader of `state`, whose tree is in `pages`.
    ///
    /// @param stamp the stamp of the versions it installs, that of the first write of the checkpoint the store opened
    ///              from
    /// @param failures receives the failures of the loads that run beside the store
    ResidentLoader(LiveLedger ledger, PageFile pages, KeyspaceState state, long stamp, Consumer<? super LedgerException> failures) {
        this.ledger = ledger;
        this.pages = pages;
        this.state = state;
        this.stamp = stamp;
        this.failures = failures;
        this.lock = new ReentrantLock();
        this.progress = lock.newCondition();
        this.below = NOTHING;
    }

    /// {@return whether memory holds `key`'s entry, if it has one}
    boolean holds(MemorySegment key) {
        return complete || Segments.compare(key, below) < 0;
    }

    /// {@return the key below which memory holds every entry, or `null` once it holds them all}
    MemorySegment limit() {
        return complete ? null : below;
    }

    /// Starts loading on a thread of its own, unless the loader has started before.
    void start() {
        if (status != IDLE) {
            return;
        }
        lock.lock();
        try {
            launch(false);
        } finally {
            lock.unlock();
        }
    }

    /// Waits until memory holds the entry of `key`, if it has one, or every entry if `key` is `null`; starts the loader
    /// first, again if it failed.
    ///
    /// @throws IllegalStateException if the store closes meanwhile
    /// @throws RuntimeException the failure of the load it waited for
    void await(MemorySegment key) {
        if (key == null ? complete : holds(key)) {
            return;
        }
        hurried = true;
        lock.lock();
        try {
            var attempt = launch(true);
            while (!(key == null ? complete : holds(key))) {
                if (stopping) {
                    throw new IllegalStateException("The store is closed");
                }
                if (status == FAILED && failedAttempt >= attempt) {
                    throw failure;
                }
                progress.awaitUninterruptibly();
            }
        } finally {
            lock.unlock();
        }
    }

    /// Has the next runs read on every read channel and, if the loader is reading the run that holds the keys next to
    /// `key`, in either direction, waits until that run is in memory; for a long scan, which would read the same leaves
    /// again one at a time.
    ///
    /// It waits for a run in flight alone, which needs no thread that waits here: it returns at once from a loader
    /// that reads no leaves, as one that failed or stopped, or that has yet to reach the key.
    ///
    /// @return whether it waited
    boolean awaitRun(MemorySegment key) {
        hurried = true;
        if (!readingFor(key)) {
            return false;
        }
        lock.lock();
        try {
            while (readingFor(key)) {
                progress.awaitUninterruptibly();
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    /// {@return whether the run the loader is reading holds the keys next to `key`, which memory does not hold yet}
    private boolean readingFor(MemorySegment key) {
        var end = reading;
        return end != null && (end == LAST_LEAF || Segments.compare(key, end) < 0) && !holds(key);
    }

    /// Starts a thread for the next attempt, holding the lock, unless the loader is stopping, done or running, or
    /// failed and `retry` is `false`.
    ///
    /// @return the attempt that runs from now on
    private int launch(boolean retry) {
        if (stopping || status == DONE || status == RUNNING || (status == FAILED && !retry)) {
            return attempts;
        }
        attempts++;
        status = RUNNING;
        try {
            thread = Thread.ofVirtual().name("ledger-resident-load").start(this::run);
        } catch (RuntimeException | Error refused) {
            // As under a limit of memory: left running, every write of an unloaded key would wait for good
            ended(refused);
            throw refused;
        }
        return attempts;
    }

    /// Stops the loader before its next run and waits for its thread, as the store closes; no thread starts it again.
    void stop() {
        Thread running;
        lock.lock();
        try {
            stopping = true;
            progress.signalAll();
            running = status == RUNNING ? thread : null;
        } finally {
            lock.unlock();
        }
        if (running == null) {
            return;
        }
        var interrupted = false;
        while (true) {
            try {
                running.join();
                break;
            } catch (InterruptedException _) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Loads every entry on the calling thread, while the store opens.
    ///
    /// @throws LedgerCorruptedException if a node fails authentication
    /// @throws com.github.auties00.ledger.LedgerIoException if the file cannot be read
    void loadNow() {
        load(PageFile.READ_CHANNELS, PageFile.READ_CHANNELS * PageFile.RUN_PAGES);
    }

    private void run() {
        Throwable failed = null;
        try {
            load(THREADS, FIRST_RUN);
        } catch (RuntimeException | Error throwable) {
            failed = throwable;
        }
        var reported = ended(failed);
        if (reported != null) {
            report(reported);
        }
        if (failed instanceof Error error) {
            throw error;
        }
    }

    /// Records how the attempt ended, `failed` or not, and wakes the writes waiting for it.
    ///
    /// @return the failure the waiting writes throw, or `null`
    private RuntimeException ended(Throwable failed) {
        lock.lock();
        try {
            if (failed == null) {
                status = complete ? DONE : IDLE;
                return null;
            }
            status = FAILED;
            failedAttempt = attempts;
            failure = failed instanceof RuntimeException runtime ? runtime
                    : new LedgerCorruptedException("The entries of " + state.keyspace.name() + " could not be loaded", failed);
            return failure;
        } finally {
            progress.signalAll();
            lock.unlock();
        }
    }

    private void report(RuntimeException failure) {
        var reported = failure instanceof LedgerException known ? known
                : new LedgerCorruptedException("The entries of " + state.keyspace.name() + " could not be loaded", failure);
        try {
            failures.accept(reported);
        } catch (RuntimeException _) {
            // A listener that throws loses its report, not the store
        }
    }

    /// Installs the entries from [#below] on, `firstRun` leaves first and twice as many each run after, up to
    /// `threads` reads of [PageFile#RUN_PAGES] pages a run; once [#hurried], every run takes all it can on every read
    /// channel.
    private void load(int threads, int firstRun) {
        var readers = ledger.readers();
        // The registration keeps the tree and its pages, which a checkpoint published meanwhile would free
        var sequence = readers.register();
        try {
            step(0);
            var from = below;
            var trees = ledger.point(sequence).tree();
            var root = trees == null ? null : trees.root(state.id);
            if (root != null) {
                var leaves = TreeSearch.leaves(pages, root, from, threads);
                var width = threads;
                var run = firstRun;
                var runs = 0;
                for (var start = 0; start < leaves.size(); ) {
                    if (stopping) {
                        return;
                    }
                    if (hurried && width < PageFile.READ_CHANNELS) {
                        width = PageFile.READ_CHANNELS;
                        run = width * PageFile.RUN_PAGES;
                    }
                    var end = Math.min(leaves.size(), start + run);
                    var refs = leaves.subList(start, end).stream().map(TreeSearch.Leaf::ref).toList();
                    reading = end == leaves.size() ? LAST_LEAF : leaves.get(end).first();
                    try {
                        install(pages.readAll(refs, PageFile.NODE, width), from);
                    } finally {
                        reading = null;
                        signal();
                    }
                    // In memory now: a read that cached one of them on its way reads memory from here on
                    for (var ref : refs) {
                        pages.cache().remove(ref);
                    }
                    start = end;
                    run = Math.min(width * PageFile.RUN_PAGES, 2 * run);
                    step(++runs);
                }
            }
            finish();
        } finally {
            readers.unregister(sequence);
        }
    }

    /// Installs the entries of `leaves` at or after `from`, then publishes them by moving [#below] past the last.
    ///
    /// A key that has a chain already got it from an earlier attempt that failed, and keeps it: no commit writes the
    /// keys at or after [#below].
    private void install(byte[][] leaves, MemorySegment from) {
        var values = state.keyspace.values();
        MemorySegment last = null;
        for (var leaf : leaves) {
            for (var i = 0; i < TreeNodes.count(leaf); i++) {
                var offset = TreeNodes.entry(leaf, i);
                var key = TreeNodes.leafKey(leaf, offset);
                if (Segments.compare(key, from) < 0) {
                    continue;
                }
                var value = TreeSearch.value(pages, leaf, offset, false);
                var decoded = values.decode(value);
                // A version on an array of its own, not on the leaf, which would stay reachable while one of its
                // entries does; the decoded array is the encoding, of which the version keeps one copy
                var version = decoded instanceof byte[] bytes && values == BuiltinCodecs.BYTES_VALUE
                        ? new Version(stamp, bytes, decoded, null)
                        : new Version(stamp, Segments.toBytes(value), decoded, null);
                state.install(key, version);
                last = key;
            }
        }
        if (last != null) {
            // Past the last key: a key between it and the next leaf's first has no entry
            below = after(last);
        }
    }

    /// {@return the key right after `key`, on an array of its own}
    static MemorySegment after(MemorySegment key) {
        var next = new byte[Math.toIntExact(key.byteSize()) + 1];
        MemorySegment.copy(key, 0, MemorySegment.ofArray(next), 0, next.length - 1);
        return MemorySegment.ofArray(next).asReadOnly();
    }

    /// Marks every entry loaded and lets the key space read memory alone.
    private void finish() {
        complete = true;
        state.loading = null;
        ledger.residentLoaded();
        lock.lock();
        try {
            status = DONE;
        } finally {
            lock.unlock();
        }
        signal();
    }

    private void signal() {
        lock.lock();
        try {
            progress.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void step(int runs) {
        var hook = stepHookForTests;
        if (hook != null) {
            hook.accept(runs);
        }
    }
}

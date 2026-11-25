package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerException;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
///
/// An application's virtual threads may hold every carrier for as long as they compute, which nothing preempts, so the
/// loader's thread may not start for as long: a write that waits for the loader then loads on its own thread instead,
/// once the thread has had [#START_GUARD_NANOS] to start, up to its key, and hands the rest to a new thread
/// ([Attempt]); a close that stops the loader makes sure it never runs.
final class ResidentLoader {
    /// The threads a loader running beside the store reads the leaves on.
    private static final int THREADS = 2;
    /// The leaves of the first run a loader running beside the store installs: the first keys reach memory soon after
    /// the first use, and each run after takes twice as many, up to one [PageFile#readAll(List, byte, int)] call per
    /// thread.
    private static final int FIRST_RUN = 16;
    /// How long a write waits for the thread of an attempt to start, from the attempt's launch, before it takes the
    /// attempt: a thread just launched has seldom been scheduled yet, so writes that took an attempt at once would load
    /// a part each on their own threads while the carriers are free, each part a walk of the tree from where the last
    /// stopped. As long as the guard of a parked commit ([PendingCommit#PARK_NANOS]), and far more than a free carrier
    /// takes to start a thread.
    private static final long START_GUARD_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
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
    // The last attempt started, guarded by the lock
    private Attempt current;
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

    /// {@return whether memory holds the entry of the key whose bytes are `key`, if it has one}
    boolean holds(byte[] key) {
        return complete || Segments.compare(key, 0, key.length, below) < 0;
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
    /// If the thread of the attempt running has not started within [#START_GUARD_NANOS] of its launch, it runs the
    /// attempt on the calling thread, up to `key`, then starts a thread for the rest. The caller's interrupt status is
    /// kept, and does not end the wait.
    ///
    /// @throws IllegalStateException if the store closes meanwhile
    /// @throws LedgerCorruptedException if the load it waited for failed, its failure as the cause: a new exception, so
    ///                                  its frames are the caller's, and a codec's exception of any type is told apart
    ///                                  from the one a closed store throws
    void await(MemorySegment key) {
        if (loaded(key)) {
            return;
        }
        hurried = true;
        var interrupted = false;
        lock.lock();
        try {
            var attempt = launch(true);
            while (!loaded(key)) {
                if (stopping) {
                    throw new IllegalStateException("The store is closed");
                }
                if (status == FAILED && failedAttempt >= attempt) {
                    throw loadFailure(failure);
                }
                if (status == IDLE) {
                    // An attempt a waiter ran up to its own key, before it started the next
                    attempt = launch(true);
                    continue;
                }
                var unstarted = current;
                if (status == RUNNING && !unstarted.claimed()) {
                    var left = unstarted.launched + START_GUARD_NANOS - System.nanoTime();
                    if (left > 0) {
                        try {
                            progress.awaitNanos(left);
                        } catch (InterruptedException _) {
                            interrupted = true;
                        }
                        continue;
                    }
                    if (unstarted.take()) {
                        lock.unlock();
                        try {
                            unstarted.perform(key);
                        } finally {
                            lock.lock();
                        }
                        continue;
                    }
                }
                progress.awaitUninterruptibly();
            }
            if (status == IDLE) {
                try {
                    launch(false);
                } catch (RuntimeException | OutOfMemoryError _) {
                    // Recorded as the attempt's failure: the next write that waits starts it again
                }
            }
        } finally {
            lock.unlock();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// {@return whether memory holds the entry of `key`, if it has one, or every entry if `key` is `null`}
    private boolean loaded(MemorySegment key) {
        return key == null ? complete : holds(key);
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
        var attempt = new Attempt();
        current = attempt;
        try {
            Thread.ofVirtual().name("ledger-resident-load").start(attempt);
        } catch (RuntimeException | Error refused) {
            // As under a limit of memory: the attempt would wait for a write to take it, and start() would never retry
            attempt.cancel();
            ended(refused);
            throw refused;
        }
        return attempts;
    }

    /// Stops the loader before its next run and waits for the attempt running, as the store closes; no thread starts it
    /// again. An attempt whose thread has not started never runs.
    void stop() {
        Attempt running;
        lock.lock();
        try {
            stopping = true;
            progress.signalAll();
            running = status == RUNNING ? current : null;
        } finally {
            lock.unlock();
        }
        if (running == null) {
            return;
        }
        if (running.cancel()) {
            ended(null);
            return;
        }
        running.awaitDone();
    }

    /// Loads every entry on the calling thread, while the store opens.
    ///
    /// @throws LedgerCorruptedException if a node fails authentication
    /// @throws com.github.auties00.ledger.LedgerIoException if the file cannot be read
    void loadNow() {
        load(PageFile.READ_CHANNELS, PageFile.READ_CHANNELS * PageFile.RUN_PAGES, null);
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
            failure = failed instanceof RuntimeException runtime ? runtime : loadFailure(failed);
            return failure;
        } finally {
            progress.signalAll();
            lock.unlock();
        }
    }

    private LedgerCorruptedException loadFailure(Throwable cause) {
        return new LedgerCorruptedException("The entries of " + state.keyspace.name() + " could not be loaded", cause);
    }

    private void report(RuntimeException failure) {
        var reported = failure instanceof LedgerException known ? known : loadFailure(failure);
        try {
            failures.accept(reported);
        } catch (RuntimeException _) {
            // A listener that throws loses its report, not the store
        }
    }

    /// Installs the entries from [#below] on, `firstRun` leaves first and twice as many each run after, up to
    /// `threads` reads of [PageFile#RUN_PAGES] pages a run; once [#hurried], every run takes all it can on every read
    /// channel.
    ///
    /// @param until the key whose entry it stops once memory holds, before its next run; or `null`, to install every
    ///              entry
    private void load(int threads, int firstRun, MemorySegment until) {
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
                    if (stopping || (until != null && holds(until))) {
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
                        // The entries are installed on arrays of their own, so the leaves go back to the pool
                        var collectable = new boolean[refs.size()];
                        var units = pages.readAll(refs, PageFile.NODE, width, collectable);
                        try {
                            install(units, from);
                        } finally {
                            for (var i = 0; i < units.length; i++) {
                                if (collectable[i]) {
                                    pages.releasePrefetched(units[i]);
                                }
                            }
                        }
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
                // entries does; the decoded array is the encoding, of which the version keeps one copy. The array must
                // be a fresh one that no other version, page or cache holds: the open's replay writes a later value of
                // the key into it in place (LiveLedger.Applier#replayedArray)
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

    /// One attempt to load: its own virtual thread runs it, unless a write that waits for the loader takes it first
    /// ([#await(MemorySegment)]) or a close cancels it ([#stop()]); the first to come decides, so it runs at most once.
    private final class Attempt implements Runnable {
        // Set by the first to take or cancel the attempt
        private final AtomicBoolean claimed;
        // Released once the attempt ended, its failure reported, or once it was cancelled
        private final CountDownLatch done;
        // When the attempt was made, just before its thread was started, in nanoseconds
        private final long launched;

        private Attempt() {
            this.claimed = new AtomicBoolean();
            this.done = new CountDownLatch(1);
            this.launched = System.nanoTime();
        }

        /// Loads every entry, on the attempt's own thread, unless the attempt was taken or cancelled.
        @Override
        public void run() {
            if (take() && perform(null) instanceof Error error) {
                throw error;
            }
        }

        /// {@return whether the attempt was taken or cancelled}
        boolean claimed() {
            return claimed.get();
        }

        /// {@return whether the caller took the attempt, which it then runs with [#perform(MemorySegment)]}
        boolean take() {
            return claimed.compareAndSet(false, true);
        }

        /// {@return whether the attempt was cancelled before anyone took it, so it never runs}
        boolean cancel() {
            if (!claimed.compareAndSet(false, true)) {
                return false;
            }
            done.countDown();
            return true;
        }

        /// Loads up to `until`, or every entry if `null`, then records how the attempt ended, waking the writes waiting
        /// for it, and reports its failure.
        ///
        /// @return the failure, or `null`
        Throwable perform(MemorySegment until) {
            Throwable failed = null;
            try {
                try {
                    load(THREADS, FIRST_RUN, until);
                } catch (RuntimeException | Error throwable) {
                    failed = throwable;
                }
                var reported = ended(failed);
                if (reported != null) {
                    report(reported);
                }
                return failed;
            } finally {
                done.countDown();
            }
        }

        /// Waits until the attempt ended, whatever the interrupt status of the caller, which it keeps.
        void awaitDone() {
            var interrupted = false;
            while (true) {
                try {
                    done.await();
                    break;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

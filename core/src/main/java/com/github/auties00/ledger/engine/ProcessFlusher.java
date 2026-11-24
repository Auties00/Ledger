package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/// The flush threads the stores opened without a pool share: daemon platform threads named `ledger-io-N`, made as
/// flushes need them and ended after [#KEEP_ALIVE_SECONDS] without one, so a process holding many idle stores holds
/// few threads, and none once they have been idle that long. No store closes it. Every store, with a pool or not,
/// gives them the forces of its file of large values too, beside the flush that waits for the force; a flush the
/// store's executor runs does a force itself if no thread has started it yet, so a thread mostly forces the file for a
/// commit that writes its own group.
///
/// Each store's log runs its flushes on a lane of its own ([#lane()]), and the forces of its file of large values on
/// another; a lane holds a thread while it has a task to run and gives it back once it has none. A task given to a
/// lane that holds a thread, running a task or about to, costs its caller one atomic add: the thread runs it next, as
/// the thread of a pool of the store's own would take it from its queue, and nothing waits for a thread to come back
/// or makes one. A task given to a lane that holds none takes the thread that waited the least, which for one busy
/// store is the thread of its last flush, and only when every thread runs another lane's tasks is a thread made, on
/// the caller's thread. A lane runs one task at a time, so the flusher holds a thread per lane busy at the same
/// moment.
///
/// A thread that runs out of tasks while its lanes have been giving it the next ones at short intervals keeps looking
/// for its next lane for a while before it parks ([#SPIN_NANOS]), waiting for a lane meanwhile as a parked one does.
/// The caller that gives it a lane then unparks a running thread: on Linux that issues no system call, and on Windows
/// it sets an event no thread waits on, so no thread is readied, where waking a parked thread took 6% of a lone
/// durable commit's time on Linux and about 8 microseconds of its processor on Windows. At most [#maxSpinners()]
/// threads of the process spin at once, none on fewer than four processors: the flusher's threads, the flushes of a
/// store's lane waiting for a commit expected to write its own group and the commits retaking a group such a flush gave
/// back take the same places, and so, on Windows, do the commits on platform threads that wait for a flush
/// ([#waitersSpin()]). A lane's flush waiting for a commit and that commit retaking the group are the two ends of one
/// hand-off of the store's writer, and take one place between them: the commit holds a place whenever it spins, and
/// the flush waits without a place of its own only while that commit retakes, from its first look at the writer to
/// its try at it, so the two spin together only from the flush letting the writer go to the commit taking it.
/// Outside these places spins only a flush that holds its store's writer while the commits the last group woke come
/// back, for a few microseconds before it yields its processor and for three quarters of a force at most: one thread
/// per store whose commits are on their way to it, which is what it waits for.
///
/// Each of these waits spins for a few microseconds, then yields its processor between looks
/// ([#pause(long, long)]), so a thread it waits for that needs the processor gets it.
///
/// It is safe for concurrent use.
final class ProcessFlusher {
    /// How long a thread waits for a lane before it ends.
    static final long KEEP_ALIVE_SECONDS = 60;
    /// The longest a thread that ran out of tasks looks for its next lane before it parks, in nanoseconds. It looks
    /// for twice the average time its lanes took to give it the next task, while that average is below this: a lone
    /// durable commit's lane is given a task every write and force of a group, 85 to 150 microseconds apart on the
    /// runners measured, so it finds the thread running.
    static final long SPIN_NANOS = 200_000;
    /// How long of [#SPIN_NANOS] a thread spins before it yields its processor between looks, in nanoseconds: a tenth
    /// of it, so for most of the window the threads that give the next task, which may need the processor, run.
    private static final long BUSY_SPIN_NANOS = 20_000;
    // The threads that spin now, of every flusher of the process and of the flushes that wait for a commit to lead
    private static final AtomicInteger SPINNERS = new AtomicInteger();
    // The most threads that spin at once: a quarter of the processors the JVM started with, at most 2, so asking costs
    // no system call on the paths that ask for every task
    private static final int MAX_SPINNERS = Math.min(2, Runtime.getRuntime().availableProcessors() / 4);
    // Whether the commits waiting for a flush take places among the spinners by default: only on Windows (see
    // waitersSpin)
    private static final boolean WAITERS_SPIN = System.getProperty("os.name", "").startsWith("Windows");
    // The most threads that spin at once for tests, or -1 for the default of maxSpinners()
    private static volatile int spinnersForTests = -1;
    // Guards process
    private static final Object PROCESS_LOCK = new Object();
    // The flusher of the stores opened without a pool, made at the first open; never ended
    private static ProcessFlusher process;

    private final ThreadFactory factory;
    // Guards idle and threads, and hands a waiting worker its lane
    private final Object lock;
    // The workers waiting for a lane, the one that waited the least last
    private final ArrayDeque<Worker> idle;
    // The workers alive, running a lane or waiting for one
    private int threads;
    // How long a worker waits for a lane before it ends, in nanoseconds
    private volatile long keepAliveNanos;

    /// Creates a flusher whose threads `factory` makes as lanes need them.
    ///
    /// @param factory makes the threads; one that returns `null` or throws refuses the flush that needed the thread
    ProcessFlusher(ThreadFactory factory) {
        this.factory = factory;
        this.lock = new Object();
        this.idle = new ArrayDeque<>();
        this.keepAliveNanos = TimeUnit.SECONDS.toNanos(KEEP_ALIVE_SECONDS);
    }

    /// {@return the flusher the stores of the process opened without a pool share}
    static ProcessFlusher process() {
        synchronized (PROCESS_LOCK) {
            var flusher = process;
            if (flusher == null) {
                flusher = process = new ProcessFlusher(
                        Thread.ofPlatform().daemon().inheritInheritableThreadLocals(false).name("ledger-io-", 0).factory());
            }
            return flusher;
        }
    }

    /// {@return a new lane, the executor of one store's flushes, or of the forces of its file of large values}
    ///
    /// A lane runs one task at a time, and runs again after every task given to it: the tasks given while one runs, or
    /// waits for its thread, are run once after it, as the last of them given. A flush writes whatever the queue holds
    /// when it runs, so one run serves every flush given before it starts; a force of the file of large values is given
    /// only once the one before it is done, so none is left behind. A task that throws is reported to its thread's
    /// uncaught exception handler, and the lane goes on. A task the lane cannot get a thread for is refused with
    /// [RejectedExecutionException], and the tasks given while it was refused are dropped with it: the log stops on a
    /// refused flush, and runs a refused force on the thread that gave it, or on a virtual thread for a commit that
    /// writes its own group.
    Executor lane() {
        return new Lane(this);
    }

    /// {@return the most threads that spin at once: those of the process's flushers waiting for a lane, the flushes of
    /// the log waiting for a commit to write its own group, the commits retaking the group such a flush gave back and,
    /// where [#waitersSpin()], the commits waiting for a group being written to be durable}
    ///
    /// A quarter of the processors, at most 2: a process holding many stores leaves the other processors to the
    /// application and to the checkpoints, and on fewer than four processors none spins, where a spinning thread would
    /// keep the one it waits for off its processor.
    static int maxSpinners() {
        var forTests = spinnersForTests;
        return forTests >= 0 ? forTests : MAX_SPINNERS;
    }

    /// {@return whether the commits on platform threads waiting for a group being written to be durable may spin, each
    /// taking a place among the [#maxSpinners()] while it does}
    ///
    /// Only on Windows, where a parked thread takes long enough to run again that skipping its wake pays, and only
    /// where any thread spins; with a number of places set for tests, wherever that number is not 0. Linux runs a woken
    /// thread again within microseconds, so a spinning commit saves next to nothing there, while the processor it holds
    /// for a whole force is one the flush and the commits on their way back need once the forces are short enough to
    /// keep every processor busy: 8 committers on 4 processors on Linux spent 35% more processor time per commit, all
    /// of it in the committing threads, and committed 2.6% fewer, where Windows committed 6% more.
    static boolean waitersSpin() {
        var forTests = spinnersForTests;
        return forTests >= 0 ? forTests > 0 : WAITERS_SPIN && MAX_SPINNERS > 0;
    }

    /// {@return whether the caller may spin, which takes one of the [#maxSpinners()] places until [#releaseSpinner()]}
    ///
    /// It reads the count before it adds to it, so callers that find every place taken write nothing.
    static boolean takeSpinner() {
        var max = maxSpinners();
        while (true) {
            var spinners = SPINNERS.get();
            if (spinners >= max) {
                return false;
            }
            if (SPINNERS.compareAndSet(spinners, spinners + 1)) {
                return true;
            }
        }
    }

    /// Gives back the place a [#takeSpinner()] that returned `true` took.
    static void releaseSpinner() {
        SPINNERS.decrementAndGet();
    }

    /// Waits for the next look of a spinning wait that has spun `spun` nanoseconds: spins once while that is below
    /// `busy`, and yields the processor from then on, so a thread the wait is for that needs the processor runs.
    static void pause(long spun, long busy) {
        if (spun < busy) {
            Thread.onSpinWait();
        } else {
            Thread.yield();
        }
    }

    /// Sets how many threads may spin at once, or `-1` for the default, for tests.
    static void maxSpinnersForTests(int spinners) {
        spinnersForTests = spinners;
    }

    /// {@return the threads that spin now, for tests}
    static int spinnersForTests() {
        return SPINNERS.get();
    }

    /// {@return the threads alive, running a lane or waiting for one, for tests}
    int threadsForTests() {
        synchronized (lock) {
            return threads;
        }
    }

    /// {@return the threads waiting for a lane, for tests}
    int idleForTests() {
        synchronized (lock) {
            return idle.size();
        }
    }

    /// Sets how long a thread waits for a lane before it ends, waking those that wait, for tests.
    void keepAliveForTests(long time, TimeUnit unit) {
        keepAliveNanos = unit.toNanos(time);
        synchronized (lock) {
            for (var worker : idle) {
                LockSupport.unpark(worker.thread);
            }
        }
    }

    /// Gives `lane`, which holds no thread and has a task, the thread that waited the least, or a new one.
    ///
    /// @throws RejectedExecutionException if no thread waits and none can be made
    private void dispatch(Lane lane) {
        Worker worker;
        synchronized (lock) {
            worker = idle.pollLast();
            if (worker != null) {
                worker.lane = lane;
            } else {
                threads++;
            }
        }
        if (worker != null) {
            LockSupport.unpark(worker.thread);
            return;
        }
        var started = new Worker(lane);
        try {
            var thread = factory.newThread(started);
            if (thread == null) {
                throw new RejectedExecutionException("No thread could be made for a task");
            }
            started.thread = thread;
            thread.start();
        } catch (RuntimeException | OutOfMemoryError failure) {
            synchronized (lock) {
                threads--;
            }
            throw failure instanceof RejectedExecutionException rejected ? rejected
                    : new RejectedExecutionException("No thread could be started for a task", failure);
        }
    }

    /// The executor of one store's flushes, or of the forces of its file of large values: a count of the tasks given
    /// since its thread last looked, 0 while it holds no thread.
    static final class Lane implements Executor {
        private static final VarHandle REQUESTS;

        static {
            try {
                REQUESTS = MethodHandles.lookup().findVarHandle(Lane.class, "requests", int.class);
            } catch (ReflectiveOperationException exception) {
                throw new ExceptionInInitializerError(exception);
            }
        }

        private final ProcessFlusher flusher;
        // The tasks given and not run since the lane's thread last looked; 0 while the lane holds no thread
        private volatile int requests;
        // The task the next run runs, the last one given, written before the add that publishes it
        private volatile Runnable task;
        // Whether the task running was given to a thread that was running or spinning; written and read on the lane's
        // thread only
        private boolean handedHot;

        private Lane(ProcessFlusher flusher) {
            this.flusher = flusher;
        }

        @Override
        public void execute(Runnable command) {
            task = command;
            if ((int) REQUESTS.getAndAdd(this, 1) != 0) {
                // The lane's thread runs it once the task it runs, if any, returns
                return;
            }
            try {
                flusher.dispatch(this);
            } catch (RejectedExecutionException exception) {
                // No thread runs the lane: the next task given takes one again
                requests = 0;
                throw exception;
            }
        }

        /// {@return whether the lane holds no thread, with no task running or left to run, for tests}
        boolean idleForTests() {
            return requests == 0;
        }

        /// {@return whether the task running, which alone may ask, was given to a thread that was running or spinning
        /// rather than one parked or made for it}
        ///
        /// Such a thread starts the task within microseconds of its caller giving it, where a parked or new one takes
        /// the time of a wake or a start, which leaves the caller time to go on first.
        boolean handedHot() {
            return handedHot;
        }

        /// Runs the lane's task until no task given since the last run is left, on the lane's thread, then gives the
        /// lane up holding the flusher's lock, so a task given from then on finds the thread waiting or takes another.
        ///
        /// @param worker the thread's worker, queued to wait for a lane once this one has no task left
        /// @param hot whether the thread was running or spinning when the lane was handed to it
        private void drain(Worker worker, boolean hot) {
            var seen = requests;
            while (true) {
                // Nothing should interrupt a flush thread; an interrupt left by a task does not reach the next one
                Thread.interrupted();
                handedHot = hot;
                try {
                    task.run();
                } catch (Throwable throwable) {
                    var thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, throwable);
                }
                synchronized (flusher.lock) {
                    seen = (int) REQUESTS.getAndAdd(this, -seen) - seen;
                    if (seen == 0) {
                        worker.lane = null;
                        worker.drained = System.nanoTime();
                        flusher.idle.addLast(worker);
                        return;
                    }
                }
                // Given while the task ran: the thread runs it next
                hot = true;
            }
        }
    }

    /// A thread of the flusher: it runs the lane it was made for, then the lanes it is handed while it waits, and
    /// ends once it has waited longer than the keep-alive for one.
    private final class Worker implements Runnable {
        // Set before the thread starts, and so before the worker can wait
        private Thread thread;
        // The lane to run next, handed under the lock while the worker waits; null while it waits for one
        private volatile Lane lane;
        // When the last lane the worker ran gave it up, written under the lock as it does so; read by the worker
        private long drained;
        // A moving average of the time from a lane giving the worker up to the next lane being handed to it, each
        // counted as at most twice SPIN_NANOS, in nanoseconds; owned by the worker
        private long gap;
        // Whether the last wait spun out without a lane, so the next one parks at once; owned by the worker
        private boolean missed;
        // Whether the lane the last wait returned was handed to the worker while it was running or spinning
        private boolean hot;

        private Worker(Lane first) {
            this.lane = first;
            this.drained = System.nanoTime();
            this.gap = SPIN_NANOS;
        }

        @Override
        public void run() {
            // The lane the thread was made for, whose caller went on while the thread started
            var next = handed(lane, false);
            while (true) {
                next.drain(this, hot);
                // A closed store's lane, and its log, are not kept for as long as the thread waits for the next one
                next = null;
                next = await();
                if (next == null) {
                    return;
                }
            }
        }

        /// {@return the lane handed to this worker while it waited, or `null` once it waited too long and left the
        /// flusher}
        ///
        /// While its lanes gave it their next tasks within [#SPIN_NANOS] on average, it first looks for a lane for
        /// twice that average, at most [#SPIN_NANOS], holding a place of [#maxSpinners()]: it spins, then yields
        /// between looks. A wait that spun out parks at once the next time, so tasks that come just too late for the
        /// spin pay for it every other wait at most. A lane handed while the worker spins unparks it all the same, so
        /// a later park may return at once on the permit left; the wait looks again, as after any park.
        private Lane await() {
            var since = System.nanoTime();
            var window = missed || gap >= SPIN_NANOS ? 0 : Math.min(SPIN_NANOS, 2 * gap);
            missed = false;
            if (window > 0 && lane == null && takeSpinner()) {
                try {
                    while (true) {
                        var handed = lane;
                        if (handed != null) {
                            return handed(handed, true);
                        }
                        var spun = System.nanoTime() - since;
                        if (spun >= window) {
                            break;
                        }
                        pause(spun, BUSY_SPIN_NANOS);
                    }
                } finally {
                    releaseSpinner();
                }
                missed = true;
            }
            var parked = false;
            while (true) {
                var handed = lane;
                if (handed != null) {
                    return handed(handed, !parked);
                }
                var left = since + keepAliveNanos - System.nanoTime();
                if (left <= 0) {
                    synchronized (lock) {
                        if (lane == null) {
                            idle.remove(this);
                            threads--;
                            return null;
                        }
                    }
                    continue;
                }
                Thread.interrupted();
                LockSupport.parkNanos(this, left);
                parked = true;
            }
        }

        /// {@return `lane`, just handed to the worker, once the time since the last lane gave the worker up is counted
        /// in the average}
        ///
        /// @param running whether the worker was running or spinning when the lane was handed to it
        private Lane handed(Lane lane, boolean running) {
            var took = Math.clamp(System.nanoTime() - drained, 0, 2 * SPIN_NANOS);
            gap += (took - gap) >> 3;
            hot = running;
            return lane;
        }
    }
}

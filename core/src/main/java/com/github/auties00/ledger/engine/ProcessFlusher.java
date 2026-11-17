package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/// The flush threads the stores opened without a pool share: daemon platform threads named `ledger-io-N`, made as
/// flushes need them and ended after [#KEEP_ALIVE_SECONDS] without one, so a process holding many idle stores holds
/// few threads, and none once they have been idle that long. No store closes it. Every store, with a pool or not,
/// forces its file of large values on them too, beside the flush that waits for the force.
///
/// Each store's log runs its flushes on a lane of its own ([#lane()]), and the forces of its file of large values on
/// another, which holds a thread while it has a task to run and gives it back once it has none. A flush given to a lane that holds a thread, running a flush or about to,
/// costs its caller one atomic add: the thread runs it next, as the thread of a pool of the store's own would take it
/// from its queue, and nothing waits for a thread to come back or makes one. A flush given to a lane that holds none
/// takes the thread that waited the least, which for one busy store is the thread of its last flush, and only when
/// every thread runs another store's flushes is a thread made, on the caller's thread. A store's log runs one flush at
/// a time, so the flusher holds a thread per store flushing at the same moment.
///
/// It is safe for concurrent use.
final class ProcessFlusher {
    /// How long a thread waits for a lane before it ends.
    static final long KEEP_ALIVE_SECONDS = 60;
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
    /// waits for its thread, are run once after it, as the last of them given. Every task of a log is its flush, which
    /// writes whatever the queue holds when it runs, so one run serves every flush given before it starts; a force of
    /// the file of large values is given only once the one before it is done. A task that
    /// throws is reported to its thread's uncaught exception handler, and the lane goes on. A task the lane cannot get a
    /// thread for is refused with [RejectedExecutionException], and the tasks given while it was refused are dropped
    /// with it: the log stops on the refusal.
    Executor lane() {
        return new Lane(this);
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
                throw new RejectedExecutionException("No thread could be made for a flush");
            }
            started.thread = thread;
            thread.start();
        } catch (RuntimeException | OutOfMemoryError failure) {
            synchronized (lock) {
                threads--;
            }
            throw failure instanceof RejectedExecutionException rejected ? rejected
                    : new RejectedExecutionException("No thread could be started for a flush", failure);
        }
    }

    /// The executor of one store's flushes: a count of the tasks given since its thread last looked, 0 while it holds
    /// no thread.
    private static final class Lane implements Executor {
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

        /// Runs the lane's task until no task given since the last run is left, on the lane's thread, then gives the
        /// lane up holding the flusher's lock, so a task given from then on finds the thread waiting or takes another.
        ///
        /// @param worker the thread's worker, queued to wait for a lane once this one has no task left
        private void drain(Worker worker) {
            var seen = requests;
            while (true) {
                // Nothing should interrupt a flush thread; an interrupt left by a task does not reach the next one
                Thread.interrupted();
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
                        flusher.idle.addLast(worker);
                        return;
                    }
                }
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

        private Worker(Lane first) {
            this.lane = first;
        }

        @Override
        public void run() {
            while (true) {
                var next = await();
                if (next == null) {
                    return;
                }
                next.drain(this);
                // A closed store's lane, and its log, are not kept for as long as the thread waits for the next one
                next = null;
            }
        }

        /// {@return the lane handed to this worker while it waited, or `null` once it waited too long and left the
        /// flusher}
        private Lane await() {
            var since = System.nanoTime();
            while (true) {
                var handed = lane;
                if (handed != null) {
                    return handed;
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
            }
        }
    }
}

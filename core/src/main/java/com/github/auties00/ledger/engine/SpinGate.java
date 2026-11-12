package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// Decides, for a store's commit pipeline, which waiting platform threads go on spinning once a wait outlasts its first
/// stretch, so that spinning waiters do not take the processors from the threads that apply and publish the commits
/// they wait for.
///
/// A wait spins freely while the platform threads committing to the store, not parked, fit on the processors. Once
/// they outnumber the processors, at most [#SLOTS] such waits spin at once; every other one yields, and parks when its
/// yields run out, as a wait that spun its whole budget does.
///
/// Each committing platform thread raises a flag of a small table while it commits, and lowers it while it parks
/// waiting in the pipeline, with plain stores to a line that no other thread writes unless two threads hash to the
/// same flag. The table is read only at the gate, once per wait that outlasts its first stretch. Two threads sharing
/// a flag make the count low, never high, so the gate closes only when at least that many threads truly commit. The
/// slots are a count on a line of its own, which only the waits past their first stretch, while the gate is closed,
/// write.
///
/// Beside its flag, each line keeps, for tests, the spins after which the last wait of its thread turned to yielding,
/// written once per wait that yields, and the answers its thread was given while the gate was closed, written once per
/// such answer.
///
/// Virtual threads are neither counted nor gated: a waiting virtual thread spins only briefly and yields its carrier.
final class SpinGate {
    /// The processors the committing threads share.
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    /// The waits that spin at once past their first stretch while the committing threads outnumber the processors:
    /// one per four processors, at least one, unless the system property `ledger.pipeline.spinners` names another
    /// number.
    static final int SLOTS = Math.max(0, Integer.getInteger("ledger.pipeline.spinners", Math.max(1, PROCESSORS / 4)));
    /// The flags of the table, a power of two of about four per processor, so few committing threads share one.
    private static final int FLAGS = Integer.highestOneBit(Math.clamp(4L * PROCESSORS - 1, 7, 63)) << 1;
    /// The bits of a thread's hash that pick its flag.
    private static final int FLAG_BITS = Integer.numberOfTrailingZeros(FLAGS);
    /// The ints from one flag to the next, 128 bytes: each flag has its line, and the pair of lines Intel's spatial
    /// prefetcher fetches together, to itself. The table starts and ends with as many ints of padding.
    private static final int STRIDE = 32;
    /// The offset from a flag of the spins after which the last wait of its thread turned to yielding.
    private static final int YIELDED = 1;
    /// The offset from a flag of the answers its thread was given while the gate was closed.
    private static final int CLOSED = 2;
    /// The answer of [#admit()] for a wait that spins on, holding no slot: the committing threads fit on the processors.
    static final int FREE = 0;
    /// The answer of [#admit()] for a wait that spins on holding a slot, which it gives back with [#release()].
    static final int HOLDING = 1;
    /// The answer of [#admit()] for a wait that yields instead of spinning.
    static final int GATED = 2;
    private static final VarHandle FLAG = MethodHandles.arrayElementVarHandle(int[].class);

    /// One flag every [#STRIDE] ints, from index [#STRIDE].
    private final int[] flags;
    /// The slots of the waits that spin while the gate is closed.
    private final Padded.Slots spinners;

    SpinGate() {
        this.flags = new int[(FLAGS + 2) * STRIDE];
        this.spinners = new Padded.Slots();
    }

    /// Raises the flag of the current thread, which starts a commit, unless it is virtual.
    ///
    /// @return the index of the flag, which [#exit(int)] lowers, or -1 for a virtual thread
    int enter() {
        var thread = Thread.currentThread();
        if (thread.isVirtual()) {
            return -1;
        }
        var index = indexOf(thread);
        FLAG.setOpaque(flags, index, 1);
        return index;
    }

    /// Lowers the flag [#enter()] raised, once the commit is done; does nothing for -1.
    void exit(int index) {
        if (index >= 0) {
            FLAG.setOpaque(flags, index, 0);
        }
    }

    /// Lowers the flag of the current thread, a committing one about to park, unless it is virtual.
    void parks() {
        var thread = Thread.currentThread();
        if (!thread.isVirtual()) {
            FLAG.setOpaque(flags, indexOf(thread), 0);
        }
    }

    /// Raises again the flag of the current thread, a committing one that [#parks()] lowered, unless it is virtual.
    void unparks() {
        var thread = Thread.currentThread();
        if (!thread.isVirtual()) {
            FLAG.setOpaque(flags, indexOf(thread), 1);
        }
    }

    /// Decides whether a platform thread whose wait outlasted its first stretch spins on.
    ///
    /// @return [#FREE], [#HOLDING] or [#GATED]
    int admit() {
        if (!oversubscribed()) {
            return FREE;
        }
        var closed = indexOf(Thread.currentThread()) + CLOSED;
        FLAG.setOpaque(flags, closed, (int) FLAG.getOpaque(flags, closed) + 1);
        return spinners.tryTake(SLOTS) ? HOLDING : GATED;
    }

    /// {@return whether it took a slot again}, for a wait that gave its slot back to help and spins on
    boolean reclaim() {
        return spinners.tryTake(SLOTS);
    }

    /// Gives back the slot of a wait that [#admit()] answered [#HOLDING] or [#reclaim()] answered `true`.
    void release() {
        spinners.giveBack();
    }

    /// Notes that a wait of the current thread, a platform one, turns to yielding after `spins` spins; for tests.
    void yields(int spins) {
        FLAG.setOpaque(flags, indexOf(Thread.currentThread()) + YIELDED, spins);
    }

    /// {@return the spins after which the last wait of `thread`, or of a thread sharing its flag, turned to yielding,
    /// or 0 if none did; for tests}
    int yieldedAfter(Thread thread) {
        return (int) FLAG.getOpaque(flags, indexOf(thread) + YIELDED);
    }

    /// {@return the answers [#admit()] gave while the gate was closed, [#HOLDING] or [#GATED], short of those two
    /// threads sharing a flag counted at once; for tests}
    int closedAnswers() {
        var answers = 0;
        for (var index = STRIDE; index <= FLAGS * STRIDE; index += STRIDE) {
            answers += (int) FLAG.getOpaque(flags, index + CLOSED);
        }
        return answers;
    }

    /// {@return the slots the waits hold; for tests}
    int spinning() {
        return spinners.taken();
    }

    /// {@return the flags raised; for tests}
    int raised() {
        var raised = 0;
        for (var index = STRIDE; index <= FLAGS * STRIDE; index += STRIDE) {
            raised += (int) FLAG.getOpaque(flags, index);
        }
        return raised;
    }

    /// {@return whether more flags are raised than there are processors}
    private boolean oversubscribed() {
        var raised = 0;
        for (var index = STRIDE; index <= FLAGS * STRIDE; index += STRIDE) {
            raised += (int) FLAG.getOpaque(flags, index);
            if (raised > PROCESSORS) {
                return true;
            }
        }
        return false;
    }

    /// {@return the index of the flag of `thread`}, from a Fibonacci hash of its id, which spreads ids given in a row
    private static int indexOf(Thread thread) {
        var slot = (int) ((thread.threadId() * 0x9E3779B97F4A7C15L) >>> (Long.SIZE - FLAG_BITS));
        return (slot + 1) * STRIDE;
    }
}

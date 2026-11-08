package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// Holders of the fields that one thread writes often and others read often, each alone on its cache lines.
///
/// The padding is inherited: the JVM lays a class's fields out after its superclass's, so a field declared between two
/// classes of 128 bytes of padding shares its 64-byte line with no other field, nor the pair of lines Intel's spatial
/// prefetcher fetches together. The `int` before the leading longs fills the gap the 12-byte object header leaves,
/// so no field of a subclass is placed there.
final class Padded {
    private Padded() {
    }

    @SuppressWarnings("unused")
    abstract static class Before {
        int pad0;
        long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15, p16;
    }

    abstract static class RoleField extends Before {
        volatile int held;
    }

    /// A role at most one thread holds, taken with a compare-and-set.
    @SuppressWarnings("unused")
    static final class Role extends RoleField {
        private static final VarHandle HELD;

        static {
            try {
                HELD = MethodHandles.lookup().findVarHandle(RoleField.class, "held", int.class);
            } catch (ReflectiveOperationException exception) {
                throw new ExceptionInInitializerError(exception);
            }
        }

        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;

        /// {@return whether a thread holds the role}
        boolean held() {
            return held != 0;
        }

        /// {@return whether it took the role, which no thread held}
        boolean tryAcquire() {
            return held == 0 && HELD.compareAndSet(this, 0, 1);
        }

        /// {@return whether it took the role, which no thread held, for one short step after which it lets the role
        /// go without looking for work that arrived meanwhile}
        boolean tryAcquireBriefly() {
            return held == 0 && HELD.compareAndSet(this, 0, 2);
        }

        /// {@return whether a thread holds the role through [#tryAcquireBriefly()]}
        boolean heldBriefly() {
            return held == 2;
        }

        /// Lets the role go with a volatile store, so the reads after it see what other threads did before they
        /// found the role held.
        void release() {
            HELD.setVolatile(this, 0);
        }

        /// Lets the role go with a release store, which does not wait for the reads after it.
        void releaseLazily() {
            HELD.setRelease(this, 0);
        }

        /// Marks the role held without a compare-and-set, for a flag that any thread may raise.
        void raise() {
            HELD.setVolatile(this, 1);
        }
    }

    abstract static class PublishedFields extends Before {
        /// The sequence of the latest visible commit.
        volatile long latest;
        /// The last commit published.
        volatile PendingCommit last;
    }

    /// What the publisher writes at every commit it publishes, which readers and waiting commits read.
    @SuppressWarnings("unused")
    static final class Published extends PublishedFields {
        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;
    }

    abstract static class CommitFields extends Before {
        volatile PendingCommit commit;
    }

    /// A reference to a commit that one side of the arrival list writes at every commit.
    @SuppressWarnings("unused")
    static final class CommitReference extends CommitFields {
        private static final VarHandle COMMIT;

        static {
            try {
                COMMIT = MethodHandles.lookup().findVarHandle(CommitFields.class, "commit", PendingCommit.class);
            } catch (ReflectiveOperationException exception) {
                throw new ExceptionInInitializerError(exception);
            }
        }

        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;

        CommitReference(PendingCommit commit) {
            this.commit = commit;
        }

        /// Makes `commit` the reference and {@return the commit it held}, with a full fence.
        PendingCommit getAndSet(PendingCommit commit) {
            return (PendingCommit) COMMIT.getAndSet(this, commit);
        }

        /// Makes `commit` the reference with a release store.
        void setLazily(PendingCommit commit) {
            COMMIT.setRelease(this, commit);
        }
    }

    abstract static class SequencedFields extends Before {
        /// The last commit given a sequence.
        PendingCommit last;
        /// The sequence the next commit takes.
        long next;
    }

    /// What the sequencer writes at every commit it gives a sequence, guarded by the sequencing role.
    @SuppressWarnings("unused")
    static final class Sequenced extends SequencedFields {
        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;
    }

    abstract static class ClockFields extends Before {
        /// The entries of the table.
        int live;
        /// The slots whose entry left and that no entry reused.
        int tombstones;
        /// The slot of the entry the clock reaches first, or -1.
        int oldest = -1;
        /// The slot of the entry the clock reaches last, or -1.
        int newest = -1;
    }

    /// What every put and eviction of a page cache writes, guarded by the cache's lock, away from the fields of its table
    /// that every read of the cache reads.
    @SuppressWarnings("unused")
    static final class Clock extends ClockFields {
        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16;
    }
}

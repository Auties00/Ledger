package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;

import java.time.Duration;
import java.util.Objects;

/// A [Commit] of a store in a directory, durable once the [LogWriter] has forced its frame.
///
/// A wait that sees the commit durable moves the values of a commit large enough to leave the heap an array at a time
/// before it returns, on the caller's thread ([SpillQueue#moveOwn(long)]); any other commit returns at once.
///
/// @param sequence the sequence of the commit
/// @param log the log holding its frame
record LogCommit(long sequence, LogWriter log) implements Commit {
    @Override
    public boolean isDurable() {
        return log.isDurable(sequence);
    }

    @Override
    public void awaitDurable() throws InterruptedException {
        log.awaitDurable(sequence);
        log.moveOwn(sequence);
    }

    @Override
    public boolean awaitDurable(Duration timeout) throws InterruptedException {
        if (!log.awaitDurable(sequence, Objects.requireNonNull(timeout, "timeout"))) {
            return false;
        }
        log.moveOwn(sequence);
        return true;
    }

    @Override
    public String toString() {
        return "Commit[" + sequence + "]";
    }
}

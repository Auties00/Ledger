package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;

import java.time.Duration;
import java.util.Objects;

/// A [Commit] of a store in a directory, durable once the [LogWriter] has forced its frame.
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
    }

    @Override
    public boolean awaitDurable(Duration timeout) throws InterruptedException {
        return log.awaitDurable(sequence, Objects.requireNonNull(timeout, "timeout"));
    }

    @Override
    public String toString() {
        return "Commit[" + sequence + "]";
    }
}

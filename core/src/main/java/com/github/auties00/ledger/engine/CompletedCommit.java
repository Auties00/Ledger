package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;

import java.time.Duration;
import java.util.Objects;

/// A [Commit] that is durable from the moment it exists: every commit of a store kept in memory.
///
/// @param sequence the sequence of the commit
record CompletedCommit(long sequence) implements Commit {
    @Override
    public boolean isDurable() {
        return true;
    }

    @Override
    public void awaitDurable() {
    }

    @Override
    public boolean awaitDurable(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return true;
    }
}

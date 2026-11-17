package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Computed;

import java.time.Duration;
import java.util.Optional;

/// The [Computed] of [LiveLedger#compute]: the outcome of its one write, and the commit that made it, or the ticket of
/// the state the function read when the write left its key unchanged.
///
/// @param <V> the value type
public final class LiveComputed<V> implements Computed<V> {
    private final Commit commit;
    private final BatchComputation<V> computation;

    LiveComputed(Commit commit, BatchComputation<V> computation) {
        this.commit = commit;
        this.computation = computation;
    }

    @Override
    public long sequence() {
        return commit.sequence();
    }

    @Override
    public boolean isDurable() {
        return commit.isDurable();
    }

    @Override
    public void awaitDurable() throws InterruptedException {
        commit.awaitDurable();
    }

    @Override
    public boolean awaitDurable(Duration timeout) throws InterruptedException {
        return commit.awaitDurable(timeout);
    }

    @Override
    public Optional<V> value() {
        return computation.value();
    }

    @Override
    public Optional<V> previous() {
        return computation.previous();
    }

    @Override
    public boolean changed() {
        return computation.changed();
    }

    @Override
    public String toString() {
        return "Computed[sequence=" + commit.sequence() + ", " + computation + "]";
    }
}

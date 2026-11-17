package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Computation;

import java.util.Optional;

/// The [Computation] of a computed write of an [ArrayWriteBatch], filled in by the commit that applies it.
///
/// The commit records what it applied as it applies the write, and marks the outcome complete once the commit has
/// returned successfully; a commit that fails leaves it incomplete, and a later commit of the same batch fills it in
/// again.
///
/// @param <V> the value type
public final class BatchComputation<V> implements Computation<V> {
    private volatile boolean complete;
    private Object previous;
    private Object value;

    BatchComputation() {
    }

    /// Records what the commit applied: the value the last run of the function received, and the one the write left.
    void applied(Object previous, Object value) {
        this.previous = previous;
        this.value = value;
    }

    /// Marks the outcome complete, once the commit that applied it returned; a volatile store, which publishes what
    /// [#applied] recorded.
    void complete() {
        complete = true;
    }

    /// Marks the outcome incomplete, as a commit of its batch starts.
    void reset() {
        complete = false;
    }

    private void checkComplete() {
        if (!complete) {
            throw new IllegalStateException("The batch holding the write has not been committed, or its commit failed");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<V> value() {
        checkComplete();
        return Optional.ofNullable((V) value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<V> previous() {
        checkComplete();
        return Optional.ofNullable((V) previous);
    }

    @Override
    public boolean changed() {
        checkComplete();
        return value != previous;
    }

    @Override
    public String toString() {
        return complete ? "Computation[previous=" + previous + ", value=" + value + "]" : "Computation[incomplete]";
    }
}

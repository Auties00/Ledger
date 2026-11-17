package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.BatchComputation;

import java.util.Optional;

/// The outcome of one computed write: what a compute function received and what it left under its key.
///
/// A computed write is added with [WriteBatch#compute] or made by [Ledger#compute]. Its outcome is known once the
/// commit that holds it returns; before then, and after a commit that failed, the methods throw.
///
/// @param <V> the value type
public sealed interface Computation<V> permits Computed, BatchComputation {
    /// {@return the value the write left under its key, or an empty optional if the key has none}
    ///
    /// When the function left the key unchanged, it is the value the function received.
    ///
    /// @throws IllegalStateException if the batch holding the write has not been committed, or its commit failed
    Optional<V> value();

    /// {@return the value the function received on the run whose result was kept, or an empty optional if the key had
    /// none}
    ///
    /// It is the value the write replaced: no write of the key came between the two.
    ///
    /// @throws IllegalStateException if the batch holding the write has not been committed, or its commit failed
    Optional<V> previous();

    /// {@return whether the write changed the key: the function returned another instance than the one it received}
    ///
    /// @throws IllegalStateException if the batch holding the write has not been committed, or its commit failed
    boolean changed();
}

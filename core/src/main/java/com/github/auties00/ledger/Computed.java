package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.LiveComputed;

/// The outcome of [Ledger#compute], and its commit.
///
/// When the function left the key unchanged no commit was made: [#sequence()] is then the sequence of the state the
/// function read, and waiting for durability waits until that state is durable.
///
/// @param <V> the value type
public sealed interface Computed<V> extends Commit, Computation<V> permits LiveComputed {
}

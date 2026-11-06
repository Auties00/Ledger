package com.github.auties00.ledger;

import java.time.Duration;

/// A batch the store has applied, and the means to wait until it is durable.
///
/// The batch is visible from the moment its commit exists. Waiting is optional: an application that needs a write
/// on stable storage before it acts, an acknowledgement sent over the network for example, waits; one that does not
/// lets the store make it durable with the commits that follow.
public interface Commit {
    /// {@return the position of this commit in the store's order of commits}
    ///
    /// Sequence numbers grow by one per commit and are not reused, also across reopens.
    long sequence();

    /// {@return whether this commit is on stable storage}
    ///
    /// The answer never changes from `true` to `false`.
    ///
    /// @throws LedgerIoException if the force that should have made it durable failed
    boolean isDurable();

    /// Waits until this commit is on stable storage.
    ///
    /// A commit that waits alone may write and force its group itself ([IoPool]): an interrupt then lets them finish,
    /// and the wait returns once the commit is durable, with the thread's interrupt status set.
    ///
    /// @throws InterruptedException if interrupted while waiting; the commit still becomes durable
    /// @throws LedgerIoException if the force that should have made it durable failed
    void awaitDurable() throws InterruptedException;

    /// Waits until this commit is on stable storage or `timeout` elapses.
    ///
    /// @param timeout the longest wait
    /// @return whether the commit is durable
    /// @throws InterruptedException if interrupted while waiting; the commit still becomes durable
    /// @throws LedgerIoException if the force that should have made it durable failed
    boolean awaitDurable(Duration timeout) throws InterruptedException;
}

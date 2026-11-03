package com.github.auties00.ledger;

/// A state of a [Ledger] that stays readable, unchanged by later commits, until it is closed.
///
/// A snapshot keeps the pages of its state from being reused: under steady writes the store's files grow while it
/// stays open. Close it when its reads are done. A snapshot may be read from any thread, but must not be read after
/// it is closed.
public non-sealed interface Snapshot extends View, AutoCloseable {
    /// {@return the sequence number of the last commit this snapshot sees}
    ///
    /// Sequence numbers are those of [Commit#sequence()].
    long sequence();

    /// Releases the state; closing a closed snapshot does nothing.
    @Override
    void close();
}

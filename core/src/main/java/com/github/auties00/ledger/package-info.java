/// An embedded storage engine: named, byte-ordered key spaces of typed entries, written in atomic batches by one
/// process, made durable through group commit and sealed with AES-GCM.
///
/// Start from [Ledger#builder()]. A store holds the key spaces it was opened with ([Keyspace]); entries are
/// written in batches ([WriteBatch], [Transaction]) that return a [Commit] to wait on for durability, and read from
/// the latest state ([Ledger]) or from a fixed one ([Snapshot]).
///
/// Unless otherwise noted, passing `null` to a method or constructor of a type in this package throws
/// [NullPointerException].
package com.github.auties00.ledger;

package com.github.auties00.ledger.engine;

/// The root of a key space's tree as of a checkpoint.
///
/// @param ref the root node
/// @param count the entries of the tree
/// @param blobs whether a leaf of the tree holds a value in a blob
record TreeRoot(PageRef ref, long count, boolean blobs) {
}

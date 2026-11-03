package com.github.auties00.ledger.engine;

/// What a read sees: the writes in memory up to a bound, over the trees of a checkpoint.
///
/// @param bound the bound of the stamps the read sees
/// @param tree the trees the read uses, or `null` if it reads memory only
/// @param pages the data file, or `null` for a store in memory
record ReadPoint(long bound, TreeVersion tree, PageFile pages) {
    /// {@return the root of the tree a read of `state` combines with memory, or `null` if it reads memory only}
    TreeRoot root(KeyspaceState state) {
        return tree == null || state.resident ? null : tree.root(state.id);
    }

    /// {@return the bound of the stamps the tree holds}
    long treeBound() {
        return Reads.boundOf(tree.sequence);
    }
}

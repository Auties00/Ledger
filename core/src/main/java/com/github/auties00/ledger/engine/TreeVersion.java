package com.github.auties00.ledger.engine;

/// The trees of a published checkpoint, which reads combine with the writes in memory since.
///
/// A read at sequence `s` uses the newest published version whose sequence is at or below `s`, walking [#older] from
/// the latest one it finds. Since a read may have found the version before this one just before this one was
/// published, the writer keeps the older version, and its pages, until every read is past [#published].
final class TreeVersion {
    /// The number of the checkpoint.
    final long number;
    /// The sequence of the last commit the trees hold.
    final long sequence;
    /// The latest sequence when the version was published.
    final long published;
    /// The root of each key space's tree by id, `null` for an empty tree.
    private final TreeRoot[] roots;
    /// The catalog blob of the checkpoint.
    final PageRef catalog;
    /// The free list blob of the checkpoint.
    final PageRef freeList;
    /// The blob holding the free list of [BlobFile], or `null` if the checkpoint wrote none.
    final PageRef blobFreeList;
    /// The version before this one, or `null` once no read can use it.
    volatile TreeVersion older;

    TreeVersion(long number, long sequence, long published, TreeRoot[] roots, PageRef catalog, PageRef freeList,
                PageRef blobFreeList, TreeVersion older) {
        this.number = number;
        this.sequence = sequence;
        this.published = published;
        this.roots = roots;
        this.catalog = catalog;
        this.freeList = freeList;
        this.blobFreeList = blobFreeList;
        this.older = older;
    }

    /// {@return the root of the tree of key space `id`, or `null` if it is empty}
    TreeRoot root(int id) {
        return id >= 0 && id < roots.length ? roots[id] : null;
    }

    /// {@return a copy of the roots by id, at least `length` long}
    TreeRoot[] roots(int length) {
        return java.util.Arrays.copyOf(roots, Math.max(length, roots.length));
    }
}

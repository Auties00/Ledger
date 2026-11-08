package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// A scan of a resident key space whose [ResidentLoader] has not put every entry in memory.
///
/// It reads a key below the loader's position from memory and any other from the tree of its own checkpoint, and reads
/// the position again before each key and before each leaf, so it leaves the tree as soon as the loader passes it. Both
/// hold what the scan sees. A commit the scan sees wrote no key at or above the position the scan first read, since a
/// write waits for the loader to pass its key: the tree holds those keys as the scan sees them, however far the loader
/// has moved since. Memory holds every entry below the position at a stamp below the bound of every read, and the
/// writes since; a key of a transaction's own scan that its visitor just wrote is below the position by then, so the
/// scan reads the write.
///
/// The tree alone would read and decrypt the leaves one at a time on the calling thread, while the loader reads the
/// same leaves again. So a scan reads [#OWN_LEAVES] leaves on its own, which keeps a short one from waiting for a run
/// many times its size, and from there it [hurries the loader][ResidentLoader#awaitRun(MemorySegment)]: it waits for a
/// run that holds the keys it needs, and reads a leaf itself only when the loader is not reading them. A scan of a whole
/// key space after an open therefore costs about what an open that loaded the key space cost, on as many threads. The
/// wait depends on no role and never fails: a loader that stopped, failed or has yet to reach the scan's keys leaves it
/// reading the tree.
final class ResidentScan {
    /// The leaves of the tree a scan reads before it waits for the loader: half the loader's first run, and the page of
    /// chats of a first screen.
    private static final int OWN_LEAVES = 8;

    private final KeyspaceState state;
    private final ResidentLoader loader;
    private final ReadPoint point;
    private final RangeRemoval[] removals;
    private final TreeRoot root;
    private final MemorySegment from;
    private final MemorySegment to;
    private final boolean descending;
    private final Reads.Visitor consumer;
    // The entries passed to the consumer
    private long visited;
    // Whether the consumer ended the scan
    private boolean stopped;
    // The leaves the scan finished reading from the tree
    private int leaves;

    /// Creates the scan of the entries `point` sees from `from`, included, to `to`, excluded, either `null` for no
    /// bound.
    ///
    /// @param point the entries the scan sees, taken before anything here reads the position of `loader`
    /// @param removals the range removals of `state`, read after `point` was taken
    ResidentScan(KeyspaceState state, ResidentLoader loader, ReadPoint point, RangeRemoval[] removals, MemorySegment from,
                 MemorySegment to, boolean descending, Reads.Visitor consumer) {
        this.state = state;
        this.loader = loader;
        this.point = point;
        this.removals = removals;
        this.root = point.treeRoot(state);
        this.from = from;
        this.to = to;
        this.descending = descending;
        this.consumer = consumer;
    }

    /// Passes the entries to the consumer, in the order of the scan, until it returns `false`.
    ///
    /// @return the number of entries passed
    long run() {
        if (descending) {
            scanDownwards();
        } else {
            scanUpwards();
        }
        return visited;
    }

    /// Takes turns between memory, up to the loader's position, and the tree from there.
    private void scanUpwards() {
        var position = from;
        while (!stopped) {
            var limit = loader.limit();
            if (limit == null || (to != null && Segments.compare(limit, to) >= 0)) {
                memory(position, to);
                return;
            }
            if (position == null || Segments.compare(position, limit) < 0) {
                memory(position, limit);
                position = limit;
            } else if (leaves < OWN_LEAVES || !loader.awaitRun(position)) {
                position = tree(position, to);
                if (position == null) {
                    return;
                }
            }
        }
    }

    /// Reads the tree down to the loader's position, then memory: the position only grows, so once a scan going down
    /// meets it, memory holds every key left.
    private void scanDownwards() {
        var high = to;
        var limit = loader.limit();
        if (limit != null && (to == null || Segments.compare(limit, to) < 0)) {
            var low = from == null || Segments.compare(limit, from) > 0 ? limit : from;
            var rest = tree(low, to);
            high = rest != null ? rest : low;
        }
        if (!stopped) {
            memory(from, high);
        }
    }

    /// Passes the entries memory holds from `low`, included, to `high`, excluded.
    private void memory(MemorySegment low, MemorySegment high) {
        var slice = state.slice(low, high);
        for (var entry : (descending ? slice.descendingMap() : slice).entrySet()) {
            var version = KeyspaceState.resolve(entry.getValue().head, entry.getKey(), point.bound(), removals);
            if (version != null && version != Version.HIDDEN) {
                visited++;
                if (!consumer.visit(entry.getKey(), version.value(), version.decoded)) {
                    stopped = true;
                    return;
                }
            }
        }
    }

    /// Passes the entries the tree holds from `low`, included, to `high`, excluded, until the loader passes the scan.
    ///
    /// @return the bound of the keys left when the loader passed the scan: the first of them going up, included, and the
    /// end of them going down, excluded; or `null` if the scan ended or none is left
    private MemorySegment tree(MemorySegment low, MemorySegment high) {
        if (root == null) {
            return null;
        }
        var cursor = new TreeCursor(point.pages(), root, low, high, descending);
        while (cursor.next()) {
            if (passed(cursor)) {
                return rest(cursor.key(), false);
            }
            var key = cursor.key();
            if (!Reads.hiddenSinceTree(key, point, removals)) {
                visited++;
                if (!consumer.visit(key, cursor.value(), null)) {
                    stopped = true;
                    return null;
                }
            }
            if (cursor.lastOfLeaf()) {
                if (++leaves >= OWN_LEAVES) {
                    loader.awaitRun(key);
                }
                // Before the cursor reads another leaf, which memory may hold by now
                if (passed(cursor)) {
                    return rest(key, true);
                }
            }
        }
        return null;
    }

    /// {@return whether memory holds the entry of the cursor's current key}
    private boolean passed(TreeCursor cursor) {
        var limit = loader.limit();
        return limit == null || cursor.compareKey(limit) < 0;
    }

    /// {@return the bound of the keys left after a stop at `key`, as [#tree(MemorySegment, MemorySegment)] returns it}
    ///
    /// @param done whether the scan has passed `key` on
    private MemorySegment rest(MemorySegment key, boolean done) {
        return descending == done ? key : ResidentLoader.after(key);
    }
}

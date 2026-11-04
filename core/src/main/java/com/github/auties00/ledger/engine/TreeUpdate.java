package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.engine.TreeNodes.Child;
import com.github.auties00.ledger.engine.TreeNodes.LeafEntry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/// Applies the writes of one checkpoint to a key space's tree, copy on write.
///
/// The writes are the state of each key written since the last checkpoint, in key order, and the ranges removed since
/// then, sorted and disjoint. A node none of them reaches is kept as it is; every other node on their paths is written
/// anew into free pages, so the tree before the update stays whole for its readers and for recovery. A subtree a
/// removed range covers is dropped without being rewritten. A rewritten node left less than a quarter full merges with
/// a neighbour when both fit in one node.
///
/// The units of the old tree the update replaces go to [#freed()]; the pages it takes go to [#allocated()], so a
/// checkpoint that fails gives them back.
final class TreeUpdate {
    private final PageFile pages;
    private final PageFile.Writer writer;
    private final FreeSpace space;
    private final List<PageRef> freed;
    private final List<PageRef> freedExternal;
    private final List<PageRef> allocated;

    TreeUpdate(PageFile pages, PageFile.Writer writer, FreeSpace space) {
        this.pages = pages;
        this.writer = writer;
        this.space = space;
        this.freed = new ArrayList<>();
        this.freedExternal = new ArrayList<>();
        this.allocated = new ArrayList<>();
    }

    /// The state of a key as of a checkpoint.
    ///
    /// @param key the key
    /// @param value its value, or `null` if the key has none
    /// @param external the unit of [BlobFile] a commit wrote the value to, which the tree takes, or `null`
    record Op(MemorySegment key, MemorySegment value, PageRef external) {
        Op(MemorySegment key, MemorySegment value) {
            this(key, value, null);
        }
    }

    /// A range of keys removed.
    ///
    /// @param from the lowest key removed, or `null` for no lower bound
    /// @param to the first key kept, or `null` for no upper bound
    record Range(MemorySegment from, MemorySegment to) {
    }

    /// {@return `ranges` without the empty ones, sorted by their lower bounds and merged where they meet or overlap}
    static List<Range> merge(List<Range> ranges) {
        var sorted = new ArrayList<Range>(ranges.size());
        for (var range : ranges) {
            if (range.from() == null || range.to() == null || Segments.compare(range.from(), range.to()) < 0) {
                sorted.add(range);
            }
        }
        if (sorted.size() < 2) {
            return sorted;
        }
        sorted.sort((left, right) -> left.from() == null ? (right.from() == null ? 0 : -1)
                : right.from() == null ? 1 : Segments.compare(left.from(), right.from()));
        var merged = new ArrayList<Range>(sorted.size());
        var current = sorted.getFirst();
        for (var i = 1; i < sorted.size(); i++) {
            var next = sorted.get(i);
            if (current.to() == null || next.from() == null || Segments.compare(next.from(), current.to()) <= 0) {
                var to = current.to() == null || next.to() == null ? null
                        : Segments.compare(current.to(), next.to()) >= 0 ? current.to() : next.to();
                current = new Range(current.from(), to);
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);
        return merged;
    }

    /// {@return the units of the data file the update replaced}
    List<PageRef> freed() {
        return freed;
    }

    /// {@return the units of [BlobFile] the old tree pointed to and the new one does not}
    List<PageRef> freedExternal() {
        return freedExternal;
    }

    /// {@return the pages the update took}
    List<PageRef> allocated() {
        return allocated;
    }

    /// {@return the root of the tree after `ops` and `removals`, or `null` if it is empty}
    ///
    /// @param root the root before, or `null` for an empty tree
    /// @param ops the states of the keys written, in key order
    /// @param removals the removed ranges, sorted and disjoint; they apply before `ops`
    TreeRoot apply(TreeRoot root, List<Op> ops, List<Range> removals) throws IOException {
        if (ops.isEmpty() && removals.isEmpty()) {
            return root;
        }
        List<Child> result;
        if (root == null) {
            var merged = merge(List.of(), ops, 0, ops.size(), removals, 0, 0);
            if (merged == null) {
                return null;
            }
            result = writeLeaves(merged);
        } else {
            var unit = read(root.ref(), PageFile.NODE, true);
            var node = new Child(null, root.ref(), root.count(), root.blobs(), TreeNodes.level(unit), -1);
            var replaced = update(node, unit, null, null, ops, 0, ops.size(), removals, 0, removals.size());
            if (replaced == null) {
                return root;
            }
            result = new ArrayList<>(replaced);
        }
        mergeUnderfull(result);
        while (result.size() > 1) {
            result = writeBranches(result);
        }
        if (result.isEmpty()) {
            return null;
        }
        var top = result.getFirst();
        // A root with one child is replaced by the child
        while (top.level() > 0) {
            var unit = read(top.ref(), PageFormat.BRANCH, true);
            if (TreeNodes.count(unit) != 1) {
                break;
            }
            freed.add(top.ref());
            top = TreeNodes.children(unit).getFirst();
        }
        return new TreeRoot(top.ref(), top.count(), top.blobs());
    }

    /// {@return the nodes that replace `node`, or `null` if the writes change nothing under it}
    private List<Child> update(Child node, byte[] unit, MemorySegment low, MemorySegment high, List<Op> ops,
                               int opFrom, int opTo, List<Range> removals, int remFrom, int remTo) throws IOException {
        if (TreeNodes.isLeaf(unit)) {
            var merged = merge(TreeNodes.leafEntries(unit), ops, opFrom, opTo, removals, remFrom, remTo);
            if (merged == null) {
                return null;
            }
            freed.add(node.ref());
            return writeLeaves(merged);
        }
        var children = TreeNodes.children(unit);
        var out = new ArrayList<Child>(children.size() + 4);
        var changed = false;
        var op = opFrom;
        var rem = remFrom;
        for (var i = 0; i < children.size(); i++) {
            var child = children.get(i);
            var childLow = i == 0 ? low : child.firstKey();
            var childHigh = i == children.size() - 1 ? high : children.get(i + 1).firstKey();
            var opEnd = op;
            while (opEnd < opTo && (childHigh == null || Segments.compare(ops.get(opEnd).key(), childHigh) < 0)) {
                opEnd++;
            }
            // The removed ranges that meet the keys the child holds, from its first key
            var remStart = rem;
            while (remStart < remTo && removals.get(remStart).to() != null
                    && Segments.compare(removals.get(remStart).to(), child.firstKey()) <= 0) {
                remStart++;
            }
            var remEnd = remStart;
            while (remEnd < remTo && (childHigh == null || removals.get(remEnd).from() == null
                    || Segments.compare(removals.get(remEnd).from(), childHigh) < 0)) {
                remEnd++;
            }
            // A range reaching past this child may reach the next one too
            rem = Math.max(remStart, remEnd - 1);
            if (op == opEnd && remStart == remEnd) {
                out.add(child);
            } else if (op == opEnd && remEnd - remStart == 1 && covers(removals.get(remStart), child.firstKey(), childHigh)) {
                drop(child);
                changed = true;
            } else {
                var childUnit = read(child.ref(), PageFile.NODE, true);
                var replaced = update(child, childUnit, childLow, childHigh, ops, op, opEnd, removals, remStart, remEnd);
                if (replaced == null) {
                    out.add(child);
                } else {
                    out.addAll(replaced);
                    changed = true;
                }
            }
            op = opEnd;
        }
        if (!changed) {
            return null;
        }
        mergeUnderfull(out);
        freed.add(node.ref());
        return out.isEmpty() ? List.of() : writeBranches(out);
    }

    /// {@return the plaintext of the unit `ref` names: queued by this checkpoint, cached or in the file}
    private byte[] read(PageRef ref, byte kind, boolean cached) {
        var unit = writer.staged(ref);
        return unit != null ? unit : pages.read(ref, kind, cached);
    }

    private static boolean covers(Range range, MemorySegment from, MemorySegment to) {
        return (range.from() == null || Segments.compare(range.from(), from) <= 0)
                && (range.to() == null || (to != null && Segments.compare(to, range.to()) <= 0));
    }

    /// Frees every unit of the subtree under `node`.
    private void drop(Child node) {
        if (node.level() == 0) {
            if (node.blobs()) {
                var unit = read(node.ref(), PageFormat.LEAF, false);
                for (var i = 0; i < TreeNodes.count(unit); i++) {
                    var entry = TreeNodes.entry(unit, i);
                    if (TreeNodes.isBlob(unit, entry)) {
                        freed.add(TreeNodes.blobRef(unit, entry));
                    } else if (TreeNodes.isExternal(unit, entry)) {
                        freedExternal.add(TreeNodes.blobRef(unit, entry));
                    }
                }
            }
        } else {
            var unit = read(node.ref(), PageFormat.BRANCH, false);
            for (var child : TreeNodes.children(unit)) {
                drop(child);
            }
        }
        freed.add(node.ref());
    }

    /// {@return the entries of a leaf after the writes that reach it, or `null` if they change none}
    private List<LeafEntry> merge(List<LeafEntry> entries, List<Op> ops, int opFrom, int opTo, List<Range> removals,
                                  int remFrom, int remTo) throws IOException {
        var out = new ArrayList<LeafEntry>(entries.size() + (opTo - opFrom));
        var changed = false;
        var op = opFrom;
        var rem = remFrom;
        for (var entry : entries) {
            while (op < opTo && Segments.compare(ops.get(op).key(), entry.key()) < 0) {
                var put = ops.get(op++);
                if (put.value() != null) {
                    out.add(entry(put));
                    changed = true;
                }
            }
            if (op < opTo && Segments.compare(ops.get(op).key(), entry.key()) == 0) {
                // The key's state as of the checkpoint, which already takes the removals into account
                var state = ops.get(op++);
                // A version the tree took already, back after a commit that replaced it rolled back, keeps its unit
                if (!(entry.external() && entry.blob().equals(state.external()))) {
                    release(entry);
                }
                changed = true;
                if (state.value() != null) {
                    out.add(entry(state));
                }
                continue;
            }
            while (rem < remTo && removals.get(rem).to() != null && Segments.compare(removals.get(rem).to(), entry.key()) <= 0) {
                rem++;
            }
            if (rem < remTo && (removals.get(rem).from() == null || Segments.compare(removals.get(rem).from(), entry.key()) <= 0)) {
                release(entry);
                changed = true;
                continue;
            }
            out.add(entry);
        }
        while (op < opTo) {
            var put = ops.get(op++);
            if (put.value() != null) {
                out.add(entry(put));
                changed = true;
            }
        }
        return changed ? out : null;
    }

    private void release(LeafEntry entry) {
        if (entry.blob() != null) {
            (entry.external() ? freedExternal : freed).add(entry.blob());
        }
    }

    private LeafEntry entry(Op op) throws IOException {
        var length = op.value().byteSize();
        if (op.external() != null) {
            return new LeafEntry(op.key(), null, op.external(), (int) length, true);
        }
        if (LeafEntry.fitsInline(op.key().byteSize(), length)) {
            return new LeafEntry(op.key(), op.value(), null, (int) length);
        }
        var unitPages = PageFormat.pagesFor(length);
        var page = allocate(unitPages);
        var unit = new byte[unitPages * PageFormat.SIZE];
        MemorySegment.copy(op.value(), 0, MemorySegment.ofArray(unit), PageFormat.HEADER, length);
        var ref = writer.write(PageFormat.VALUE, 0, page, unitPages, unit, (int) length, true);
        return new LeafEntry(op.key(), null, ref, (int) length);
    }

    private int allocate(int unitPages) {
        var page = space.allocate(unitPages);
        allocated.add(new PageRef(page, unitPages, 0, 0));
        return page;
    }

    private List<Child> writeLeaves(List<LeafEntry> entries) throws IOException {
        var sizes = new int[entries.size()];
        for (var i = 0; i < sizes.length; i++) {
            sizes[i] = entries.get(i).size();
        }
        var out = new ArrayList<Child>();
        for (var run : TreeNodes.split(sizes, sizes.length)) {
            var page = allocate(1);
            var unit = new byte[PageFormat.SIZE];
            var length = TreeNodes.writeLeaf(unit, entries, run[0], run[1]);
            var blobs = false;
            for (var i = run[0]; i < run[1]; i++) {
                blobs |= entries.get(i).blob() != null;
            }
            var ref = writer.write(PageFormat.LEAF, 0, page, 1, unit, length, true);
            out.add(new Child(TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0)), ref, run[1] - run[0], blobs, 0, length));
        }
        return out;
    }

    private List<Child> writeBranches(List<Child> children) throws IOException {
        var sizes = new int[children.size()];
        for (var i = 0; i < sizes.length; i++) {
            sizes[i] = children.get(i).size();
        }
        var level = children.getFirst().level() + 1;
        var out = new ArrayList<Child>();
        for (var run : TreeNodes.split(sizes, sizes.length)) {
            var page = allocate(1);
            var unit = new byte[PageFormat.SIZE];
            var length = TreeNodes.writeBranch(unit, children, run[0], run[1]);
            var count = 0L;
            var blobs = false;
            for (var i = run[0]; i < run[1]; i++) {
                count += children.get(i).count();
                blobs |= children.get(i).blobs();
            }
            var ref = writer.write(PageFormat.BRANCH, level, page, 1, unit, length, true);
            out.add(new Child(TreeNodes.childKey(unit, 0), ref, count, blobs, level, length));
        }
        return out;
    }

    /// Merges each node of `nodes` a checkpoint just wrote less than a quarter full with a neighbour, when both fit in
    /// one node; every merge leaves one node fewer, so the loop ends.
    private void mergeUnderfull(List<Child> nodes) throws IOException {
        var i = 0;
        while (i < nodes.size() && nodes.size() > 1) {
            var node = nodes.get(i);
            if (node.fill() < 0 || node.fill() >= TreeNodes.MIN_FILL) {
                i++;
                continue;
            }
            var merged = false;
            for (var neighbour : new int[]{i + 1, i - 1}) {
                if (neighbour < 0 || neighbour >= nodes.size()) {
                    continue;
                }
                var left = Math.min(i, neighbour);
                var a = nodes.get(left);
                var b = nodes.get(left + 1);
                var unitA = read(a.ref(), PageFile.NODE, true);
                var unitB = read(b.ref(), PageFile.NODE, true);
                // Both contents count the entry count field, which the merged node keeps once
                if (Bytes.i32(unitA, PageFormat.LENGTH) + Bytes.i32(unitB, PageFormat.LENGTH) - 2 > PageFormat.capacity(1)) {
                    continue;
                }
                List<Child> replacement;
                if (a.level() == 0) {
                    var entries = new ArrayList<>(TreeNodes.leafEntries(unitA));
                    entries.addAll(TreeNodes.leafEntries(unitB));
                    replacement = writeLeaves(entries);
                } else {
                    var children = new ArrayList<>(TreeNodes.children(unitA));
                    children.addAll(TreeNodes.children(unitB));
                    replacement = writeBranches(children);
                }
                freed.add(a.ref());
                freed.add(b.ref());
                nodes.remove(left + 1);
                nodes.remove(left);
                nodes.addAll(left, replacement);
                i = left;
                merged = true;
                break;
            }
            if (!merged) {
                i++;
            }
        }
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.engine.TreeNodes.Child;
import com.github.auties00.ledger.engine.TreeNodes.LeafEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

/// Applies the writes of one checkpoint to a key space's tree, copy on write.
///
/// The writes are the state of each key written since the last checkpoint, in key order, and the ranges removed since
/// then, sorted and disjoint. A node none of them reaches is kept as it is; every other node on their paths is written
/// anew into free pages, so the tree before the update stays whole for its readers and for recovery. A subtree a
/// removed range covers is dropped without being rewritten. A rewritten node left less than a quarter full merges with
/// a neighbour when both fit in one node.
///
/// A leaf below the root that the writes change by a little is written as a delta record of what changed since its
/// base leaf ([DeltaLeaves]); one that changes more, splits, or falls under a quarter full is written whole.
///
/// The units of the old tree the update replaces go to [#freed()]; the pages it takes go to [#allocated()], so a
/// checkpoint that fails gives them back. The delta records it writes and those it drops are counted by page, which
/// [#finishDeltas(DeltaTable)] applies to the table of delta pages once every tree is updated. A branch with many
/// writes below it updates its children's subtrees in parallel, in the common fork-join pool.
final class TreeUpdate {
    /// The writes below a branch from which its children's subtrees are updated on every processor.
    private static final int PARALLEL_OPS = 4_096;

    private final PageFile pages;
    private final PageFile.Writer writer;
    private final FreeSpace space;
    private final List<PageRef> freed;
    private final List<PageRef> freedExternal;
    private final List<PageRef> allocated;
    // The records each delta page gained or lost, by page
    private final Map<Integer, Integer> records;
    // Fills the delta pages of this update's thread
    private final DeltaLeaves.Packer packer;

    TreeUpdate(PageFile pages, PageFile.Writer writer, FreeSpace space) {
        this.pages = pages;
        this.writer = writer;
        this.space = space;
        this.freed = new ArrayList<>();
        this.freedExternal = new ArrayList<>();
        this.allocated = new ArrayList<>();
        this.records = new HashMap<>();
        this.packer = new DeltaLeaves.Packer(writer, new DeltaLeaves.Packer.Space() {
            @Override
            public long take(int least, int most) {
                var run = space.allocateRun(least, most);
                // The whole run, though the packer gives some back: a page given back and taken again by another unit
                // of the checkpoint goes back with it, if the checkpoint fails
                allocated.add(new PageRef((int) (run >>> 32), (int) run, 0, 0));
                return run;
            }

            @Override
            public void giveBack(int page, int pages) {
                space.release(page, pages);
            }
        }, (page, written) -> records.merge(page, written, Integer::sum));
    }

    /// The state of a key as of a checkpoint.
    ///
    /// @param key the key
    /// @param value its value, or `null` if the key has none
    /// @param external the unit of [BlobFile] a commit wrote the value to, which the tree takes, or `null`
    /// @param blind the chain of the key, if a blind write the checkpoint takes needs to know whether the tree held
    ///              the key ([BlindWrites]), which the update records on it; or `null`
    record Op(MemorySegment key, MemorySegment value, PageRef external, Chain blind) {
        Op(MemorySegment key, MemorySegment value) {
            this(key, value, null, null);
        }

        /// Records on the chain of a blind write whether the tree held the key.
        void settle(boolean inTree) {
            if (blind != null) {
                blind.blindState = inTree ? TreeSearch.FOUND : TreeSearch.ABSENT;
            }
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

    /// Queues the delta page being filled and applies the records written and dropped to `table`, once every tree is
    /// updated; the delta pages left with no record go to [#freed()].
    ///
    /// @return the table of delta pages after the update
    DeltaTable finishDeltas(DeltaTable table) throws IOException {
        packer.close();
        var next = table.apply(records, freed);
        records.clear();
        return next;
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
            var replaced = update(node, unit, null, null, ops, 0, ops.size(), removals, 0, removals.size(), true);
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
        if (top.ref().isRecord()) {
            // A root is a unit of its own: a leaf left alone at the top is written whole
            var unit = read(top.ref(), PageFormat.LEAF, true);
            retire(top.ref(), TreeNodes.base(unit, top.ref()));
            top = writeLeaves(TreeNodes.leafEntries(unit)).getFirst();
        }
        return new TreeRoot(top.ref(), top.count(), top.blobs());
    }

    /// {@return the nodes that replace `node`, or `null` if the writes change nothing under it}
    ///
    /// @param root whether `node` is the root, which is never a delta record
    private List<Child> update(Child node, byte[] unit, MemorySegment low, MemorySegment high, List<Op> ops,
                               int opFrom, int opTo, List<Range> removals, int remFrom, int remTo, boolean root) throws IOException {
        if (TreeNodes.isLeaf(unit)) {
            var entries = TreeNodes.leafEntries(unit);
            var merged = merge(entries, ops, opFrom, opTo, removals, remFrom, remTo);
            if (merged == null) {
                return null;
            }
            var base = TreeNodes.base(unit, node.ref());
            if (!root) {
                var delta = writeDelta(node.ref(), base, entries, merged);
                if (delta != null) {
                    return delta.isEmpty() ? null : delta;
                }
            }
            retire(node.ref(), base);
            return writeLeaves(merged);
        }
        var children = TreeNodes.children(unit);
        // What each child takes: none of the writes, a removal of all of it, or a share of them
        var plans = new Plan[children.size()];
        var updates = 0;
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
                plans[i] = null;
            } else if (op == opEnd && remEnd - remStart == 1 && covers(removals.get(remStart), child.firstKey(), childHigh)) {
                plans[i] = Plan.DROP;
            } else {
                plans[i] = new Plan(childLow, childHigh, op, opEnd, remStart, remEnd);
                updates++;
            }
            op = opEnd;
        }
        var replacements = updates > 1 && opTo - opFrom >= PARALLEL_OPS
                ? updateInParallel(children, plans, ops, removals)
                : updateInTurn(children, plans, ops, removals);
        var out = new ArrayList<Child>(children.size() + 4);
        var changed = false;
        for (var i = 0; i < children.size(); i++) {
            var child = children.get(i);
            if (plans[i] == Plan.DROP) {
                drop(child);
                changed = true;
            } else if (replacements[i] == null) {
                out.add(child);
            } else {
                out.addAll(replacements[i]);
                changed = true;
            }
        }
        if (!changed) {
            return null;
        }
        mergeUnderfull(out);
        freed.add(node.ref());
        return out.isEmpty() ? List.of() : writeBranches(out);
    }

    /// {@return what replaces each child that `plans` gives a share of the writes to, `null` where nothing does}
    @SuppressWarnings("unchecked")
    private List<Child>[] updateInTurn(List<Child> children, Plan[] plans, List<Op> ops, List<Range> removals) throws IOException {
        var replacements = (List<Child>[]) new List<?>[children.size()];
        for (var i = 0; i < children.size(); i++) {
            var plan = plans[i];
            if (plan != null && plan != Plan.DROP) {
                replacements[i] = update(children.get(i), ops, removals, plan);
            }
        }
        return replacements;
    }

    /// Updates, on every processor, each child that `plans` gives a share of the writes to.
    ///
    /// Each subtree is a task with an update of its own, whose freed and taken units join this one's; the writer and
    /// the free space are shared, and safe for concurrent use. A checkpoint of a write rate that rewrites most leaves
    /// would otherwise rewrite them on one processor.
    ///
    /// @return what replaces each child, `null` where nothing does
    @SuppressWarnings("unchecked")
    private List<Child>[] updateInParallel(List<Child> children, Plan[] plans, List<Op> ops, List<Range> removals) throws IOException {
        var replacements = (List<Child>[]) new List<?>[children.size()];
        var tasks = new ArrayList<Subtree>();
        for (var i = 0; i < children.size(); i++) {
            var plan = plans[i];
            if (plan != null && plan != Plan.DROP) {
                tasks.add(new Subtree(i, new TreeUpdate(pages, writer, space), children.get(i), ops, removals, plan));
            }
        }
        try {
            ForkJoinTask.invokeAll(tasks);
        } catch (UncheckedIOException exception) {
            throw exception.getCause();
        } finally {
            // Whatever the tasks took, even those of a failed update, goes back if the checkpoint fails
            for (var task : tasks) {
                allocated.addAll(task.update.allocated);
            }
        }
        for (var task : tasks) {
            freed.addAll(task.update.freed);
            freedExternal.addAll(task.update.freedExternal);
            task.update.records.forEach((page, change) -> records.merge(page, change, Integer::sum));
            replacements[task.index] = task.getRawResult();
        }
        return replacements;
    }

    /// {@return the nodes that replace `child`, or `null` if the writes `plan` gives it change nothing under it}
    private List<Child> update(Child child, List<Op> ops, List<Range> removals, Plan plan) throws IOException {
        var unit = read(child.ref(), PageFile.NODE, true);
        return update(child, unit, plan.low(), plan.high(), ops, plan.opFrom(), plan.opTo(), removals, plan.remFrom(), plan.remTo(), false);
    }

    /// Writes the leaf `ref`, made from the leaf `base`, as a delta record if what changed since `base` makes a small
    /// one and `merged`, the entries after the writes, fill one leaf at least a quarter.
    ///
    /// The record is the one `ref` names, if it is a record, with what the writes changed: the base is not read. It
    /// may remove a key the base lacks, or put one the base holds as it is, which reads the same. The leaf the record
    /// makes goes to the page cache, as a leaf written whole does: the next checkpoint and the readers find it there
    /// rather than reading the record and its base.
    ///
    /// @param entries the entries of the leaf `ref` names, before the writes
    /// @return the record, alone; an empty list if `merged` is what `ref` holds already; or `null` if the leaf is to
    ///         be written whole
    private List<Child> writeDelta(PageRef ref, PageRef base, List<LeafEntry> entries, List<LeafEntry> merged) throws IOException {
        if (merged.isEmpty()) {
            return null;
        }
        var fill = 2;
        var blobs = false;
        for (var entry : merged) {
            fill += entry.size();
            blobs |= entry.blob() != null;
        }
        if (fill - 2 > TreeNodes.CAPACITY || fill < TreeNodes.MIN_FILL) {
            return null;
        }
        var touched = DeltaLeaves.diff(entries, merged);
        if (touched.isEmpty()) {
            return List.of();
        }
        var changes = ref.isRecord() ? DeltaLeaves.combine(DeltaLeaves.record(read(ref.unit(), PageFormat.DELTA, true), ref.slot()), touched)
                : touched;
        var size = DeltaLeaves.recordSize(changes);
        if (size > DeltaLeaves.MAX_RECORD) {
            return null;
        }
        if (ref.isRecord()) {
            dropRecord(ref);
        }
        var record = packer.append(base, changes, size);
        var leaf = new byte[PageFormat.SIZE];
        TreeNodes.deltaHeader(leaf, base, TreeNodes.writeLeaf(leaf, merged, 0, merged.size()));
        pages.cache().put(record, leaf, false);
        return List.of(new Child(merged.getFirst().key(), record, merged.size(), blobs, 0, fill));
    }

    /// Gives up the leaf `ref`, made from the leaf `base`: frees a unit, or drops a delta record and frees its base.
    private void retire(PageRef ref, PageRef base) {
        if (ref.isRecord()) {
            dropRecord(ref);
        }
        freed.add(base);
    }

    private void dropRecord(PageRef ref) {
        records.merge(ref.page(), -1, Integer::sum);
    }

    /// The share of the writes of one child of a branch.
    ///
    /// @param low the lowest key the child may hold, or `null`
    /// @param high the first key past the child, or `null`
    private record Plan(MemorySegment low, MemorySegment high, int opFrom, int opTo, int remFrom, int remTo) {
        /// A child a removed range covers whole.
        static final Plan DROP = new Plan(null, null, 0, 0, 0, 0);
    }

    /// The update of one subtree, run on a processor of its own.
    private static final class Subtree extends RecursiveTask<List<Child>> {
        private final int index;
        private final TreeUpdate update;
        private final Child child;
        private final List<Op> ops;
        private final List<Range> removals;
        private final Plan plan;

        Subtree(int index, TreeUpdate update, Child child, List<Op> ops, List<Range> removals, Plan plan) {
            this.index = index;
            this.update = update;
            this.child = child;
            this.ops = ops;
            this.removals = removals;
            this.plan = plan;
        }

        @Override
        protected List<Child> compute() {
            try {
                var replacement = update.update(child, ops, removals, plan);
                // The records of the subtree end its own pages, so the branch above reads them back from the writer
                update.packer.close();
                return replacement;
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }

    /// {@return the plaintext of the unit `ref` names: queued by this checkpoint, cached or in the file; for a delta
    /// record, the leaf it makes, from a delta page this update may still be filling}
    private byte[] read(PageRef ref, byte kind, boolean cached) {
        if (!ref.isRecord()) {
            var unit = writer.staged(ref);
            return unit != null ? unit : pages.read(ref, kind, cached);
        }
        var hit = pages.cached(ref, kind);
        if (hit != null) {
            return hit;
        }
        return DeltaLeaves.materialize(ref, (unit, unitKind) -> {
            var filling = packer.filling(unit);
            if (filling != null) {
                return filling;
            }
            var staged = writer.staged(unit);
            return staged != null ? staged : pages.read(unit, unitKind, cached);
        });
    }

    private static boolean covers(Range range, MemorySegment from, MemorySegment to) {
        return (range.from() == null || Segments.compare(range.from(), from) <= 0)
                && (range.to() == null || (to != null && Segments.compare(to, range.to()) <= 0));
    }

    /// Frees every unit of the subtree under `node`.
    private void drop(Child node) {
        if (node.level() == 0) {
            var unit = node.blobs() || node.ref().isRecord() ? read(node.ref(), PageFormat.LEAF, false) : null;
            if (node.blobs()) {
                for (var i = 0; i < TreeNodes.count(unit); i++) {
                    var entry = TreeNodes.entry(unit, i);
                    if (TreeNodes.isBlob(unit, entry)) {
                        freed.add(TreeNodes.blobRef(unit, entry));
                    } else if (TreeNodes.isExternal(unit, entry)) {
                        freedExternal.add(TreeNodes.blobRef(unit, entry));
                    }
                }
            }
            retire(node.ref(), unit == null ? node.ref() : TreeNodes.base(unit, node.ref()));
        } else {
            var unit = read(node.ref(), PageFormat.BRANCH, false);
            for (var child : TreeNodes.children(unit)) {
                drop(child);
            }
            freed.add(node.ref());
        }
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
                put.settle(false);
                if (put.value() != null) {
                    out.add(entry(put));
                    changed = true;
                }
            }
            if (op < opTo && Segments.compare(ops.get(op).key(), entry.key()) == 0) {
                // The key's state as of the checkpoint, which already takes the removals into account
                var state = ops.get(op++);
                state.settle(true);
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
            put.settle(false);
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

    /// {@return the first of `unitPages` free pages, which the update took}
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
                retire(a.ref(), TreeNodes.base(unitA, a.ref()));
                retire(b.ref(), TreeNodes.base(unitB, b.ref()));
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

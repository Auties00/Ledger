package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.engine.TreeNodes.Child;
import com.github.auties00.ledger.engine.TreeNodes.LeafEntry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicReference;

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
/// Sibling leaves written whole next to each other split together: a leaf that splits alone leaves two half-full
/// leaves, and one whose entries are only overwritten keeps its fill forever, so every checkpoint that rewrites it
/// reads, seals and forces its slack. How full the run's leaves end up depends on how the writes reached them
/// ([#target(List)]), and a run that would keep its number of leaves keeps its boundaries
/// ([#splitsTogether(List, int\[\], int)]). A leaf written whole with no such neighbour splits on its own.
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
    private final PageCounts records;
    // Fills the delta pages of this update's thread
    private final DeltaLeaves.Packer packer;
    // The first failure of a task of this update or of the updates of its subtrees, which all share it
    private final AtomicReference<Throwable> failure;

    TreeUpdate(PageFile pages, PageFile.Writer writer, FreeSpace space) {
        this(pages, writer, space, new AtomicReference<>());
    }

    private TreeUpdate(PageFile pages, PageFile.Writer writer, FreeSpace space, AtomicReference<Throwable> failure) {
        this.failure = failure;
        this.pages = pages;
        this.writer = writer;
        this.space = space;
        this.freed = new ArrayList<>();
        this.freedExternal = new ArrayList<>();
        this.allocated = new ArrayList<>();
        this.records = new PageCounts();
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
        }, records::add);
    }

    /// The state of a key as of a checkpoint.
    ///
    /// It keeps a chain of the key and the version rather than segments of the key and the value, which it makes when
    /// it writes the key: a checkpoint holds one state per key it takes, through the whole update.
    ///
    /// @param keyed a chain of the key, whose bytes the state compares and writes
    /// @param version its version as of the checkpoint, or `null` if the key has none
    /// @param blind the chain of the key, if a blind write the checkpoint takes needs to know whether the tree held
    ///              the key ([BlindWrites]), which the update records on it; or `null`
    record Op(Chain keyed, Version version, Chain blind) {
        /// Makes the state of `key` with `value`, or with none if `null`.
        Op(MemorySegment key, MemorySegment value) {
            this(new Chain(key, 0), value == null ? null : new Version(0, value, null, null), null);
        }

        /// {@return the key, a segment made for the call}
        MemorySegment key() {
            return keyed.key();
        }

        /// {@return the order of the key and `other`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
        int compareKey(MemorySegment other) {
            return keyed.compareKey(other);
        }

        /// {@return the value, or `null` if the key has none}
        MemorySegment value() {
            return version == null ? null : version.value();
        }

        /// {@return whether the key has no value}
        boolean removes() {
            return version == null || version.removes();
        }

        /// {@return the unit of [BlobFile] a commit wrote the value to, which the tree takes, or `null`}
        PageRef external() {
            return version == null ? null : version.external();
        }

        /// Records on the chain of a blind write whether the tree held the key.
        void settle(boolean inTree) {
            if (blind != null) {
                blind.settle(inTree);
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
            result = writeLeaves(merged.entries());
        } else {
            var unit = read(root.ref(), PageFile.NODE, true);
            var node = new Child(null, root.ref(), root.count(), root.blobs(), TreeNodes.level(unit), -1);
            switch (update(node, unit, null, null, ops, 0, ops.size(), removals, 0, removals.size(), true)) {
                case null -> {
                    return root;
                }
                case Written written -> result = new ArrayList<>(written.nodes());
                case Rewrite rewrite -> {
                    retire(rewrite.ref(), rewrite.base());
                    result = writeLeaves(rewrite.merged());
                }
            }
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

    /// {@return what replaces `node`, or `null` if the writes change nothing under it}
    ///
    /// A leaf to write whole comes back unwritten, for the caller to write and give up: only a leaf does, as a branch
    /// writes those among its children itself.
    ///
    /// @param root whether `node` is the root, which is never a delta record
    private Replacement update(Child node, byte[] unit, MemorySegment low, MemorySegment high, List<Op> ops,
                               int opFrom, int opTo, List<Range> removals, int remFrom, int remTo, boolean root) throws IOException {
        if (TreeNodes.isLeaf(unit)) {
            var entries = TreeNodes.leafEntries(unit);
            var merged = merge(entries, ops, opFrom, opTo, removals, remFrom, remTo);
            if (merged == null) {
                return null;
            }
            var base = TreeNodes.base(unit, node.ref());
            if (!root) {
                var delta = writeDelta(node.ref(), base, entries, merged.entries());
                if (delta != null) {
                    return delta.isEmpty() ? null : new Written(delta);
                }
            }
            var size = 2;
            for (var entry : merged.entries()) {
                size += entry.size();
            }
            return new Rewrite(node.ref(), base, merged.entries(), merged.inserted(), merged.gaps(), size != Bytes.i32(unit, PageFormat.LENGTH));
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
            while (opEnd < opTo && (childHigh == null || ops.get(opEnd).compareKey(childHigh) < 0)) {
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
        var replacements = updates > 1 && opTo - opFrom >= PARALLEL_OPS ? updateInParallel(children, plans, ops, removals) : null;
        var out = new ArrayList<Child>(children.size() + 4);
        // The leaves to write whole since the last child that stays or takes a delta record, written as soon as that
        // child comes; the children of a branch hold their keys in order, and a dropped child leaves its neighbours
        // next to each other
        var run = new ArrayList<Rewrite>();
        var changed = false;
        for (var i = 0; i < children.size(); i++) {
            var child = children.get(i);
            var plan = plans[i];
            if (plan == Plan.DROP) {
                drop(child);
                changed = true;
                continue;
            }
            var replacement = replacements != null ? replacements[i] : plan == null ? null : update(child, ops, removals, plan);
            if (replacement instanceof Rewrite rewrite) {
                run.add(rewrite);
                changed = true;
                continue;
            }
            writeRun(run, out);
            if (replacement instanceof Written written) {
                out.addAll(written.nodes());
                changed = true;
            } else {
                out.add(child);
            }
        }
        writeRun(run, out);
        if (!changed) {
            return null;
        }
        mergeUnderfull(out);
        freed.add(node.ref());
        return new Written(out.isEmpty() ? List.of() : writeBranches(out));
    }

    /// Writes the leaves of `run`, next to each other under one branch, to `out`, gives up the units they replace and
    /// empties `run`.
    private void writeRun(List<Rewrite> run, List<Child> out) throws IOException {
        for (var rewrite : run) {
            retire(rewrite.ref(), rewrite.base());
        }
        if (run.size() > 1) {
            var entries = new ArrayList<LeafEntry>();
            for (var rewrite : run) {
                entries.addAll(rewrite.merged());
            }
            var sizes = sizes(entries);
            var target = target(run);
            if (splitsTogether(run, sizes, target)) {
                out.addAll(writeLeaves(entries, sizes, TreeNodes.split(sizes, sizes.length, target)));
                run.clear();
                return;
            }
        }
        for (var rewrite : run) {
            out.addAll(writeLeaves(rewrite.merged()));
        }
        run.clear();
    }

    /// {@return the sixteenths of a leaf to fill the leaves of `run` to}
    ///
    /// New keys scattered over the run, as random ids, keep coming, so its leaves keep room for the next checkpoint's
    /// as delta records: a full leaf takes none, and the first splits it in two halves. New keys in blocks, as the
    /// messages a chat appends, are followed by more past them rather than among them, so their leaves are left full.
    /// At most two new keys a gap, over two gaps or more, tells the first from the second. Entries changed in place
    /// that take the bytes they took have no reason to grow; any other change may grow a little.
    private static int target(List<Rewrite> run) {
        var inserted = 0;
        var gaps = 0;
        var resized = false;
        for (var rewrite : run) {
            inserted += rewrite.inserted();
            gaps += rewrite.gaps();
            resized |= rewrite.resized();
        }
        if (gaps >= 2 && inserted <= 2 * gaps) {
            return TreeNodes.TARGET_ROOMY;
        }
        return (gaps > 0 ? inserted > 2 * gaps : !resized) ? TreeNodes.TARGET_FULL : TreeNodes.TARGET_NEAR_FULL;
    }

    /// {@return whether the leaves of `run`, whose entries take `sizes` in order, split together to `target` rather
    /// than each on its own}
    ///
    /// They do if that saves a leaf. If it takes more they do only once the leaves on their own would be fuller than
    /// [TreeNodes#TARGET_NEAR_FULL]: a few leaves round up to a leaf more than their fill asks for, which would halve
    /// their room for nothing. Otherwise the boundaries stay, as moving them would gain nothing.
    private static boolean splitsTogether(List<Rewrite> run, int[] sizes, int target) {
        var alone = 0;
        var total = 0L;
        var from = 0;
        for (var rewrite : run) {
            var to = from + rewrite.merged().size();
            var size = 0L;
            for (var i = from; i < to; i++) {
                size += sizes[i];
            }
            alone += size == 0 ? 0 : size <= TreeNodes.CAPACITY ? 1 : TreeNodes.split(Arrays.copyOfRange(sizes, from, to), to - from).size();
            total += size;
            from = to;
        }
        var together = TreeNodes.nodesFor(sizes, sizes.length, target);
        return together < alone || (together > alone && TreeNodes.TARGET_FULL * total > TreeNodes.TARGET_NEAR_FULL * (long) TreeNodes.CAPACITY * alone);
    }

    /// Updates, on every processor, each child that `plans` gives a share of the writes to.
    ///
    /// Each subtree is a task with an update of its own, whose freed and taken units join this one's; the writer and
    /// the free space are shared, and safe for concurrent use. A task for a leaf only merges its writes: the leaves of
    /// one branch are built and written by the task of the branch, as a run of siblings splits together, and the
    /// writer seals their pages on every processor as it flushes them.
    ///
    /// It returns only once every task has ended, even if one failed, and a failure stops the tasks that have not
    /// started yet: a checkpoint that fails gives back the pages its update took and the next one retries at once, so a
    /// task left running would take pages no one gives back, give back pages the retry took and move the position of the
    /// channel under the retry's writes. [java.util.concurrent.ForkJoinTask#invokeAll(java.util.Collection)] does not
    /// wait: it rethrows the first failure it finds and cancels the others, which does not stop a running one.
    ///
    /// @return what replaces each child, `null` where nothing does
    private Replacement[] updateInParallel(List<Child> children, Plan[] plans, List<Op> ops, List<Range> removals) throws IOException {
        var replacements = new Replacement[children.size()];
        var tasks = new ArrayList<Subtree>();
        for (var i = 0; i < children.size(); i++) {
            var plan = plans[i];
            if (plan != null && plan != Plan.DROP) {
                tasks.add(new Subtree(i, new TreeUpdate(pages, writer, space, failure), children.get(i), ops, removals, plan));
            }
        }
        // The tasks before this index stay on this thread: the first, and any the pool refused
        var forked = tasks.size();
        try {
            for (; forked > 1; forked--) {
                tasks.get(forked - 1).fork();
            }
        } catch (Throwable refused) {
            failure.compareAndSet(null, refused);
        }
        for (var i = 0; i < forked; i++) {
            tasks.get(i).quietlyInvoke();
        }
        for (var i = forked; i < tasks.size(); i++) {
            tasks.get(i).quietlyJoin();
        }
        // Whatever the tasks took, even those of a failed update, goes back if the checkpoint fails
        for (var task : tasks) {
            allocated.addAll(task.update.allocated);
        }
        switch (failure.get()) {
            case null -> {
            }
            case IOException exception -> throw exception;
            case UncheckedIOException exception -> throw exception.getCause();
            case RuntimeException exception -> throw exception;
            case Error error -> throw error;
            case Throwable other -> throw new IllegalStateException(other);
        }
        for (var task : tasks) {
            freed.addAll(task.update.freed);
            freedExternal.addAll(task.update.freedExternal);
            records.addAll(task.update.records);
            replacements[task.index] = task.getRawResult();
        }
        return replacements;
    }

    /// {@return what replaces `child`, or `null` if the writes `plan` gives it change nothing under it}
    private Replacement update(Child child, List<Op> ops, List<Range> removals, Plan plan) throws IOException {
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
        if (appends(entries, touched)) {
            // The end of a range that grows, as a chat's newest messages: the leaf most reads start from, which a
            // record would make two reads, and whose writes a checkpoint takes few of
            return null;
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
        var leaf = TreeNodes.writeLeaf(merged, 0, merged.size(), fill);
        TreeNodes.deltaHeader(leaf, base, fill);
        pages.cache().put(record, leaf, false);
        return List.of(new Child(merged.getFirst().key(), record, merged.size(), blobs, 0, fill));
    }

    /// {@return whether every one of `changes` puts a key past the last of `entries`}
    private static boolean appends(List<LeafEntry> entries, List<LeafEntry> changes) {
        if (entries.isEmpty()) {
            return true;
        }
        var last = entries.getLast().key();
        return !changes.getFirst().isRemoved() && Segments.compare(changes.getFirst().key(), last) > 0;
    }

    /// Gives up the leaf `ref`, made from the leaf `base`: frees a unit, or drops a delta record and frees its base.
    private void retire(PageRef ref, PageRef base) {
        if (ref.isRecord()) {
            dropRecord(ref);
        }
        freed.add(base);
    }

    private void dropRecord(PageRef ref) {
        records.add(ref.page(), -1);
    }

    /// The share of the writes of one child of a branch.
    ///
    /// @param low the lowest key the child may hold, or `null`
    /// @param high the first key past the child, or `null`
    private record Plan(MemorySegment low, MemorySegment high, int opFrom, int opTo, int remFrom, int remTo) {
        /// A child a removed range covers whole.
        static final Plan DROP = new Plan(null, null, 0, 0, 0, 0);
    }

    /// What replaces a node of the tree.
    private sealed interface Replacement {
    }

    /// Nodes written already.
    ///
    /// @param nodes the nodes, in key order; empty if none of the node's entries is left
    private record Written(List<Child> nodes) implements Replacement {
    }

    /// A leaf to write whole, together with the sibling leaves next to it to write whole.
    ///
    /// It holds its units until then, so that the leaf a task of [#updateInParallel] updated gives them up in the
    /// update of the branch above, once.
    ///
    /// @param ref the leaf
    /// @param base the leaf `ref` is made from
    /// @param merged the entries after the writes, views of the old leaf and of the writes, which stay as they are
    ///               while the checkpoint reads and writes other units
    /// @param inserted the keys the writes added
    /// @param gaps the places between two old keys that new keys landed in, the one past the last old key included
    /// @param resized whether the entries take other bytes than the leaf's did
    private record Rewrite(PageRef ref, PageRef base, List<LeafEntry> merged, int inserted, int gaps, boolean resized) implements Replacement {
    }

    /// The entries of a leaf after the writes that change it.
    ///
    /// @param entries the entries
    /// @param inserted the keys the writes added
    /// @param gaps the places between two old keys that new keys landed in, the one past the last old key included
    private record Merged(List<LeafEntry> entries, int inserted, int gaps) {
    }

    /// The update of one subtree, run on a processor of its own.
    private static final class Subtree extends RecursiveTask<Replacement> {
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
        protected Replacement compute() {
            // The update has failed, so nothing reads the result: the branch that waits for this task throws instead
            if (update.failure.get() != null) {
                return null;
            }
            try {
                var replacement = update.update(child, ops, removals, plan);
                // The records of the subtree end its own pages, so the branch above reads them back from the writer
                update.packer.close();
                return replacement;
            } catch (Throwable throwable) {
                update.failure.compareAndSet(null, throwable);
                return null;
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
    private Merged merge(List<LeafEntry> entries, List<Op> ops, int opFrom, int opTo, List<Range> removals,
                         int remFrom, int remTo) throws IOException {
        var out = new ArrayList<LeafEntry>(entries.size() + (opTo - opFrom));
        var changed = false;
        var inserted = 0;
        var gaps = 0;
        var op = opFrom;
        var rem = remFrom;
        for (var entry : entries) {
            var before = inserted;
            while (op < opTo && ops.get(op).compareKey(entry.key()) < 0) {
                var put = ops.get(op++);
                put.settle(false);
                if (!put.removes()) {
                    out.add(entry(put));
                    changed = true;
                    inserted++;
                }
            }
            if (inserted != before) {
                gaps++;
            }
            if (op < opTo && ops.get(op).compareKey(entry.key()) == 0) {
                // The key's state as of the checkpoint, which already takes the removals into account
                var state = ops.get(op++);
                state.settle(true);
                // A version the tree took already, back after a commit that replaced it rolled back, keeps its unit
                if (!(entry.external() && entry.blob().equals(state.external()))) {
                    release(entry);
                }
                changed = true;
                if (!state.removes()) {
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
        var before = inserted;
        while (op < opTo) {
            var put = ops.get(op++);
            put.settle(false);
            if (!put.removes()) {
                out.add(entry(put));
                changed = true;
                inserted++;
            }
        }
        if (inserted != before) {
            gaps++;
        }
        return changed ? new Merged(out, inserted, gaps) : null;
    }

    private void release(LeafEntry entry) {
        if (entry.blob() != null) {
            (entry.external() ? freedExternal : freed).add(entry.blob());
        }
    }

    private LeafEntry entry(Op op) throws IOException {
        var key = op.key();
        var value = op.value();
        var length = value.byteSize();
        var external = op.external();
        if (external != null) {
            return new LeafEntry(key, null, external, (int) length, true);
        }
        if (LeafEntry.fitsInline(key.byteSize(), length)) {
            return new LeafEntry(key, value, null, (int) length);
        }
        var unitPages = PageFormat.pagesFor(length);
        var page = allocate(unitPages);
        // The writer builds the unit from the version's value in the array it seals it into: nothing reads it before
        // the checkpoint is published, and a read after goes through the file
        var ref = writer.writeValue(page, unitPages, value);
        return new LeafEntry(key, null, ref, (int) length);
    }

    /// {@return the first of `unitPages` free pages, which the update took}
    private int allocate(int unitPages) {
        var page = space.allocate(unitPages);
        allocated.add(new PageRef(page, unitPages, 0, 0));
        return page;
    }

    private static int[] sizes(List<LeafEntry> entries) {
        var sizes = new int[entries.size()];
        for (var i = 0; i < sizes.length; i++) {
            sizes[i] = entries.get(i).size();
        }
        return sizes;
    }

    private List<Child> writeLeaves(List<LeafEntry> entries) throws IOException {
        var sizes = sizes(entries);
        return writeLeaves(entries, sizes, TreeNodes.split(sizes, sizes.length));
    }

    /// {@return the leaves written of `entries`, which take `sizes`, one per run of `runs`, pairs of first and last
    /// index}
    private List<Child> writeLeaves(List<LeafEntry> entries, int[] sizes, List<int[]> runs) throws IOException {
        var out = new ArrayList<Child>(runs.size());
        for (var run : runs) {
            var page = allocate(1);
            var length = TreeNodes.contentLength(sizes, run[0], run[1]);
            var unit = TreeNodes.writeLeaf(entries, run[0], run[1], length);
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
            var length = TreeNodes.contentLength(sizes, run[0], run[1]);
            var unit = TreeNodes.writeBranch(children, run[0], run[1], length);
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

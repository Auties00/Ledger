package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.engine.TreeNodes.LeafEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/// Leaves a checkpoint writes as the entries that changed since a base leaf, in a record that shares a page with the
/// records of other leaves.
///
/// A checkpoint rewrites every leaf a write reaches, copy on write, so writes spread over more leaves than a checkpoint
/// takes keys rewrite a page for each key or two: at a few hundred bytes an entry, ten times the bytes the commits
/// wrote. A leaf a few writes reach is written instead as a record of what changed since its base leaf, which stays
/// in place; the records of the leaves a checkpoint changes fill its delta pages one after another. The record holds
/// every change since the base, so a leaf is always its base and one record: a checkpoint that changes the leaf again
/// replaces the record, and one whose record would pass [#MAX_RECORD] writes the leaf whole, freeing the base. This
/// is how Berkeley DB Java Edition logs a bottom internal node as a delta while under a quarter of it changed.
///
/// A reader reads the leaf as a whole one ([#materialize(PageRef, Source)]): the base with the record applied, which
/// the page cache keeps under the record's reference, without the base and the delta page it is made of. A delta page
/// is free once no branch refers to a record it holds, which [DeltaTable] counts.
///
/// Content of a delta page, after the [PageFormat] header; the records fill the page from its end:
///
/// ```text
/// 0  records, 2 bytes
/// 2  offset of each record in the unit, 2 bytes each, by slot
///    free space, then the records
/// ```
///
/// A record:
///
/// ```text
/// base leaf: page (4), nonce (12)
/// entries (2), then each entry in key order: as a leaf holds it (TreeNodes), or key length (2), key, 3 for a key
/// the base holds and the leaf does not
/// ```
final class DeltaLeaves {
    /// The most bytes a record takes: past them, the leaf is written whole. Each change of a leaf of entries of a few
    /// hundred bytes rewrites the record with the changes before it, so a larger record would cost more than it saves.
    static final int MAX_RECORD = (PageFormat.capacity(1) - 2) * 3 / 10;
    private static final int RECORD_FIXED = 16 + 2;
    private static final int COUNT = PageFormat.HEADER;
    /// The bit of an entry [#materialize(PageRef, Source)] merges that tells an offset in the record's page from one in
    /// the base: offsets in a unit of one page take 12 bits.
    private static final int FROM_RECORD = 1 << 16;

    private DeltaLeaves() {
    }

    /// Reads the units a leaf is made of.
    @FunctionalInterface
    interface Source {
        /// {@return the plaintext of the unit `ref` names, of kind `kind`}
        byte[] read(PageRef ref, byte kind);
    }

    /// {@return the leaf the delta record `ref` makes of its base, as a leaf unit whose header names the base, in an
    /// array of its header and content}
    ///
    /// The leaf holds a search index ([NodeIndex]) if it fits in a page with one. A record an older version wrote may
    /// make a leaf that fits only without one, which is of the first format.
    ///
    /// A record holds its entries as a leaf does, so the leaf is made of the bytes of the entries of the base and of the
    /// record, each copied once as it is: the leaf [TreeNodes#writeLeaf(List, int, int, boolean)] would write of the
    /// base's entries with the record's applied, with no view of an entry in between.
    ///
    /// @throws LedgerCorruptedException if the page does not hold the record, the record runs past the page, or the leaf
    ///                                  would not fit in a page
    static byte[] materialize(PageRef ref, Source source) {
        var page = source.read(ref.unit(), PageFormat.DELTA);
        if (ref.slot() >= Bytes.u16(page, COUNT)) {
            throw new LedgerCorruptedException("A delta page holds no record " + ref.slot() + " at page " + ref.page(), null);
        }
        var record = Bytes.u16(page, COUNT + 2 + 2 * ref.slot());
        if (record + RECORD_FIXED > page.length) {
            throw pastPage(ref);
        }
        var base = new PageRef(Bytes.i32(page, record), 1, Bytes.i64(page, record + 4), Bytes.i32(page, record + 12));
        var leaf = source.read(base, PageFormat.LEAF);
        var baseCount = TreeNodes.count(leaf);
        var changes = Bytes.u16(page, record + 16);
        // Each entry of the leaf to make, in key order: its offset, in the record's page if FROM_RECORD is set
        var merged = new int[baseCount + changes];
        var count = 0;
        var bytes = 0;
        var at = 0;
        var position = record + RECORD_FIXED;
        for (var i = 0; i < changes; i++) {
            var length = recordEntryLength(page, position, ref);
            var keyAt = TreeNodes.leafKeyOffset(position);
            var keyLength = TreeNodes.leafKeyLength(page, position);
            // The base's entries before the change, and the one it replaces or removes
            while (at < baseCount) {
                var entry = TreeNodes.entry(leaf, at);
                var order = Segments.compare(leaf, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(leaf, entry),
                        page, keyAt, keyLength);
                if (order > 0) {
                    break;
                }
                at++;
                if (order == 0) {
                    break;
                }
                merged[count++] = entry;
                bytes += TreeNodes.entryLength(leaf, entry);
            }
            if (!TreeNodes.isRemoved(page, position)) {
                merged[count++] = position | FROM_RECORD;
                bytes += length;
            }
            position += length;
        }
        for (; at < baseCount; at++) {
            var entry = TreeNodes.entry(leaf, at);
            merged[count++] = entry;
            bytes += TreeNodes.entryLength(leaf, entry);
        }
        var indexed = TreeNodes.SLOT * count + bytes <= TreeNodes.CAPACITY;
        // Without an index, the leaf takes the count of its entries, their offsets and the entries
        if (!indexed && 2 + 2 * count + bytes > PageFormat.capacity(1)) {
            throw new LedgerCorruptedException("A delta record at page " + ref.page() + " makes a leaf too large", null);
        }
        var offsets = PageFormat.HEADER + 2 + 2 * count;
        byte[] unit;
        if (indexed) {
            var prefix = 0;
            var firstUnit = page;
            var firstAt = 0;
            if (count > 0) {
                firstUnit = unitOf(merged[0], leaf, page);
                var first = offsetOf(merged[0]);
                var lastUnit = unitOf(merged[count - 1], leaf, page);
                var last = offsetOf(merged[count - 1]);
                firstAt = TreeNodes.leafKeyOffset(first);
                prefix = NodeIndex.prefix(firstUnit, firstAt, TreeNodes.leafKeyLength(firstUnit, first),
                        lastUnit, TreeNodes.leafKeyOffset(last), TreeNodes.leafKeyLength(lastUnit, last));
            }
            var heads = new long[count];
            for (var i = 0; i < count; i++) {
                var from = unitOf(merged[i], leaf, page);
                var entry = offsetOf(merged[i]);
                heads[i] = NodeIndex.head(from, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(from, entry), prefix);
            }
            var wide = NodeIndex.wide(heads, offsets - PageFormat.HEADER + NodeIndex.length(count, prefix, true) + (long) bytes);
            unit = new byte[offsets + NodeIndex.length(count, prefix, wide) + bytes];
            // Before the header, which keeps the format the index marks
            position = NodeIndex.write(unit, offsets, firstUnit, firstAt, prefix, heads, wide);
        } else {
            unit = new byte[offsets + bytes];
            position = offsets;
        }
        Bytes.u16(unit, PageFormat.HEADER, count);
        for (var i = 0; i < count; i++) {
            var from = unitOf(merged[i], leaf, page);
            var entry = offsetOf(merged[i]);
            var length = TreeNodes.entryLength(from, entry);
            Bytes.u16(unit, PageFormat.HEADER + 2 + 2 * i, position);
            System.arraycopy(from, entry, unit, position, length);
            position += length;
        }
        TreeNodes.filled(unit, position);
        TreeNodes.deltaHeader(unit, base, unit.length - PageFormat.HEADER);
        return unit;
    }

    /// {@return the bytes the entry at `position` of the record of `ref` in its delta page `page` takes, checked to end
    /// in the page}
    ///
    /// A store that encrypts authenticates the page, but a plain store's check sum passes any bytes it was written
    /// with, and a record written wrong would otherwise be copied from past the page.
    private static int recordEntryLength(byte[] page, int position, PageRef ref) {
        // The key's length, the key and the byte of the entry's form, then the 2 bytes of an inline value's length
        if (position + 3 > page.length) {
            throw pastPage(ref);
        }
        var field = TreeNodes.leafKeyOffset(position) + TreeNodes.leafKeyLength(page, position);
        if (field >= page.length || TreeNodes.isInline(page, position) && field + 3 > page.length) {
            throw pastPage(ref);
        }
        var length = TreeNodes.entryLength(page, position);
        if (position + length > page.length) {
            throw pastPage(ref);
        }
        return length;
    }

    /// {@return the failure of a delta record of `ref` that runs past its page}
    private static LedgerCorruptedException pastPage(PageRef ref) {
        return new LedgerCorruptedException("A delta record runs past its page, at page " + ref.page(), null);
    }

    /// {@return the unit the entry `merged` of [#materialize(PageRef, Source)] is in: `page` if it is of the record,
    /// `leaf` if it is of the base}
    private static byte[] unitOf(int merged, byte[] leaf, byte[] page) {
        return (merged & FROM_RECORD) != 0 ? page : leaf;
    }

    /// {@return the offset of the entry `merged` of [#materialize(PageRef, Source)] in its unit}
    private static int offsetOf(int merged) {
        return merged & ~FROM_RECORD;
    }

    /// {@return the entries of record `slot` of the delta page `page`, as views of it}
    static List<LeafEntry> record(byte[] page, int slot) {
        if (slot >= Bytes.u16(page, COUNT)) {
            throw new LedgerCorruptedException("A delta page holds no record " + slot, null);
        }
        return changes(page, Bytes.u16(page, COUNT + 2 + 2 * slot));
    }

    /// {@return the entries of the record at `record` of the delta page `page`, as views of it}
    private static List<LeafEntry> changes(byte[] page, int record) {
        var count = Bytes.u16(page, record + 16);
        var changes = new ArrayList<LeafEntry>(count);
        var position = record + RECORD_FIXED;
        for (var i = 0; i < count; i++) {
            changes.add(TreeNodes.entryAt(page, position));
            position += TreeNodes.entryLength(page, position);
        }
        return changes;
    }

    /// {@return what turns `base` into `leaf`, both in key order: the entries of `leaf` that `base` lacks or holds
    /// otherwise, and the removal of the keys `base` holds and `leaf` does not}
    static List<LeafEntry> diff(List<LeafEntry> base, List<LeafEntry> leaf) {
        var changes = new ArrayList<LeafEntry>();
        var at = 0;
        for (var entry : leaf) {
            while (at < base.size() && Segments.compare(base.get(at).key(), entry.key()) < 0) {
                changes.add(LeafEntry.removed(base.get(at++).key()));
            }
            if (at < base.size() && Segments.compare(base.get(at).key(), entry.key()) == 0) {
                // A leaf's entries the writes left alone are the very entries of its base, when the base is the leaf
                var same = base.get(at++);
                if (same != entry && !same.sameAs(entry)) {
                    changes.add(entry);
                }
            } else {
                changes.add(entry);
            }
        }
        while (at < base.size()) {
            changes.add(LeafEntry.removed(base.get(at++).key()));
        }
        return changes;
    }

    /// {@return the changes of `older` followed by those of `newer`, both in key order: `newer` wins a key both hold}
    static List<LeafEntry> combine(List<LeafEntry> older, List<LeafEntry> newer) {
        var out = new ArrayList<LeafEntry>(older.size() + newer.size());
        var at = 0;
        for (var change : newer) {
            while (at < older.size() && Segments.compare(older.get(at).key(), change.key()) < 0) {
                out.add(older.get(at++));
            }
            if (at < older.size() && Segments.compare(older.get(at).key(), change.key()) == 0) {
                at++;
            }
            out.add(change);
        }
        while (at < older.size()) {
            out.add(older.get(at++));
        }
        return out;
    }

    /// {@return the bytes the record of `changes` takes}
    static int recordSize(List<LeafEntry> changes) {
        var size = RECORD_FIXED;
        for (var change : changes) {
            size += change.bytes();
        }
        return size;
    }

    /// Fills delta pages with records, one page at a time, for one thread of a checkpoint.
    ///
    /// It takes its pages in runs, so the delta pages of a checkpoint reach the file as long writes rather than one
    /// page in each hole the last checkpoints left.
    static final class Packer {
        /// The fewest free pages a run of delta pages takes, and the most.
        private static final int LEAST_RUN = 8;
        private static final int MOST_RUN = 64;

        private final PageFile.Writer writer;
        // Takes and gives back runs of free pages
        private final Space space;
        // Receives each page written and the records it holds
        private final Written written;
        private byte[] unit;
        private int page;
        private int nonce;
        private int records;
        // The lowest byte the records take; the offsets grow up to it
        private int low;
        // The pages of the current run not used yet
        private int next;
        private int end;

        /// Takes and gives back the runs of free pages delta pages are written to.
        interface Space {
            /// {@return a run of free pages, as [FreeSpace#allocateRun(int, int)] gives it}
            long take(int least, int most);

            /// Gives back the `pages` pages from `page`, which the packer did not use.
            void giveBack(int page, int pages);
        }

        /// Receives a delta page once it is queued.
        @FunctionalInterface
        interface Written {
            /// Accounts for the page `page`, which holds `records` records.
            void accept(int page, int records);
        }

        Packer(PageFile.Writer writer, Space space, Written written) {
            this.writer = writer;
            this.space = space;
            this.written = written;
        }

        /// Adds the record of `changes` to `base`, `size` bytes as [#recordSize(List)] counts them.
        ///
        /// @return the reference of the record
        PageRef append(PageRef base, List<LeafEntry> changes, int size) throws IOException {
            if (unit == null || COUNT + 2 + 2 * (records + 1) > low - size) {
                finish();
                unit = new byte[PageFormat.SIZE];
                page = nextPage();
                nonce = writer.nextNonce();
                records = 0;
                low = PageFormat.SIZE - PageFormat.TAG;
            }
            low -= size;
            Bytes.i32(unit, low, base.page());
            Bytes.i64(unit, low + 4, base.nonceHigh());
            Bytes.i32(unit, low + 12, base.nonceLow());
            Bytes.u16(unit, low + 16, changes.size());
            var position = low + RECORD_FIXED;
            for (var change : changes) {
                position = TreeNodes.writeEntry(unit, position, change);
            }
            Bytes.u16(unit, COUNT + 2 + 2 * records, low);
            // Kept current, so the page reads back while it fills
            Bytes.u16(unit, COUNT, records + 1);
            return new PageRef(page, 1, writer.nonceHigh(), nonce, records++);
        }

        /// {@return the delta page being filled if `ref` names it, or `null`}
        byte[] filling(PageRef ref) {
            return unit != null && ref.page() == page && ref.nonceHigh() == writer.nonceHigh() && ref.nonceLow() == nonce
                    ? unit
                    : null;
        }

        private int nextPage() {
            if (next == end) {
                var run = space.take(LEAST_RUN, MOST_RUN);
                next = (int) (run >>> 32);
                end = next + (int) run;
            }
            return next++;
        }

        /// Queues the delta page being filled, if any, and gives back the pages of the run it did not use.
        void close() throws IOException {
            finish();
            if (next < end) {
                space.giveBack(next, end - next);
                next = end;
            }
        }

        /// Queues the delta page being filled, if any.
        private void finish() throws IOException {
            if (unit == null) {
                return;
            }
            writer.write(PageFormat.DELTA, 0, page, 1, unit, PageFormat.capacity(1), writer.admits(unit.length), nonce);
            written.accept(page, records);
            unit = null;
        }
    }
}

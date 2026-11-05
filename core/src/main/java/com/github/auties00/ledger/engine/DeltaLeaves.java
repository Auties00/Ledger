package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.engine.TreeNodes.LeafEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

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
/// the page cache keeps under the record's reference. A delta page is free once no branch refers to a record it
/// holds, which [DeltaTable] counts.
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
    static final int MAX_RECORD = TreeNodes.CAPACITY * 3 / 10;
    private static final int RECORD_FIXED = 16 + 2;
    private static final int COUNT = PageFormat.HEADER;

    private DeltaLeaves() {
    }

    /// Reads the units a leaf is made of.
    @FunctionalInterface
    interface Source {
        /// {@return the plaintext of the unit `ref` names, of kind `kind`}
        byte[] read(PageRef ref, byte kind);
    }

    /// {@return the leaf the delta record `ref` makes of its base, as a leaf unit whose header names the base}
    ///
    /// @throws LedgerCorruptedException if the page does not hold the record, or the leaf would not fit in a page
    static byte[] materialize(PageRef ref, Source source) {
        var page = source.read(ref.unit(), PageFormat.DELTA);
        if (ref.slot() >= Bytes.u16(page, COUNT)) {
            throw new LedgerCorruptedException("A delta page holds no record " + ref.slot() + " at page " + ref.page(), null);
        }
        var record = Bytes.u16(page, COUNT + 2 + 2 * ref.slot());
        var base = new PageRef(Bytes.i32(page, record), 1, Bytes.i64(page, record + 4), Bytes.i32(page, record + 12));
        var entries = apply(TreeNodes.leafEntries(source.read(base, PageFormat.LEAF)), changes(page, record));
        var size = 0;
        for (var entry : entries) {
            size += entry.size();
        }
        if (size > TreeNodes.CAPACITY) {
            throw new LedgerCorruptedException("A delta record at page " + ref.page() + " makes a leaf too large", null);
        }
        var unit = new byte[PageFormat.SIZE];
        var length = TreeNodes.writeLeaf(unit, entries, 0, entries.size());
        TreeNodes.deltaHeader(unit, base, length);
        return unit;
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

    /// {@return `base` with `changes` applied, both in key order}
    private static List<LeafEntry> apply(List<LeafEntry> base, List<LeafEntry> changes) {
        var out = new ArrayList<LeafEntry>(base.size() + changes.size());
        var at = 0;
        for (var change : changes) {
            while (at < base.size() && Segments.compare(base.get(at).key(), change.key()) < 0) {
                out.add(base.get(at++));
            }
            if (at < base.size() && Segments.compare(base.get(at).key(), change.key()) == 0) {
                at++;
            }
            if (!change.isRemoved()) {
                out.add(change);
            }
        }
        while (at < base.size()) {
            out.add(base.get(at++));
        }
        return out;
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
                if (!base.get(at++).sameAs(entry)) {
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

    /// {@return the bytes the record of `changes` takes}
    static int recordSize(List<LeafEntry> changes) {
        var size = RECORD_FIXED;
        for (var change : changes) {
            // An entry of a leaf counts its offset, which a record has not
            size += change.size() - 2;
        }
        return size;
    }

    /// Fills delta pages with records, one page at a time, for one thread of a checkpoint.
    static final class Packer {
        private final PageFile.Writer writer;
        // Takes a free page for the next delta page
        private final IntUnaryOperator allocate;
        // Receives each page written and the records it holds
        private final Written written;
        private byte[] unit;
        private int page;
        private int nonce;
        private int records;
        // The lowest byte the records take; the offsets grow up to it
        private int low;

        /// Receives a delta page once it is queued.
        @FunctionalInterface
        interface Written {
            /// Accounts for the page `page`, which holds `records` records.
            void accept(int page, int records);
        }

        Packer(PageFile.Writer writer, IntUnaryOperator allocate, Written written) {
            this.writer = writer;
            this.allocate = allocate;
            this.written = written;
        }

        /// Adds the record of `changes` to `base`, `size` bytes as [#recordSize(List)] counts them.
        ///
        /// @return the reference of the record
        PageRef append(PageRef base, List<LeafEntry> changes, int size) throws IOException {
            if (unit == null || COUNT + 2 + 2 * (records + 1) > low - size) {
                finish();
                unit = new byte[PageFormat.SIZE];
                page = allocate.applyAsInt(1);
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

        /// Queues the delta page being filled, if any.
        void finish() throws IOException {
            if (unit == null) {
                return;
            }
            writer.write(PageFormat.DELTA, 0, page, 1, unit, PageFormat.capacity(1), true, nonce);
            written.accept(page, records);
            unit = null;
        }
    }
}

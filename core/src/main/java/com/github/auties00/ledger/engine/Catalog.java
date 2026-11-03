package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/// The blobs a checkpoint writes besides its trees: the catalog of key spaces and the free list.
///
/// Catalog content:
///
/// ```text
/// highest id ever given (4), entries (4), then per key space:
///   id (4), name length (2), name (ASCII), has a tree (1)[, root reference (20), entries (8), has blobs (1)]
/// ```
///
/// Free list content: runs (4), then per run its first page (4) and length (4).
final class Catalog {
    private Catalog() {
    }

    /// A key space as a checkpoint records it.
    ///
    /// @param id the id the log knows it by
    /// @param name its name
    /// @param root its tree, or `null` if empty
    record Entry(int id, String name, TreeRoot root) {
    }

    /// A catalog read back.
    ///
    /// @param maxId the highest id ever given
    /// @param entries the key spaces
    record Contents(int maxId, List<Entry> entries) {
    }

    /// {@return a unit holding the catalog of `entries`, its content filled in, and its content length in the header}
    static byte[] encode(int maxId, List<Entry> entries) {
        var length = 8;
        for (var entry : entries) {
            length += 4 + 2 + entry.name().length() + 1 + (entry.root() == null ? 0 : PageRef.BYTES + 8 + 1);
        }
        var unit = new byte[PageFormat.pagesFor(length) * PageFormat.SIZE];
        var at = PageFormat.HEADER;
        Bytes.i32(unit, at, maxId);
        Bytes.i32(unit, at + 4, entries.size());
        at += 8;
        for (var entry : entries) {
            Bytes.i32(unit, at, entry.id());
            var name = entry.name().getBytes(StandardCharsets.US_ASCII);
            Bytes.u16(unit, at + 4, name.length);
            System.arraycopy(name, 0, unit, at + 6, name.length);
            at += 6 + name.length;
            if (entry.root() == null) {
                unit[at++] = 0;
                continue;
            }
            unit[at++] = 1;
            entry.root().ref().write(unit, at);
            Bytes.i64(unit, at + PageRef.BYTES, entry.root().count());
            unit[at + PageRef.BYTES + 8] = (byte) (entry.root().blobs() ? 1 : 0);
            at += PageRef.BYTES + 9;
        }
        Bytes.i32(unit, PageFormat.LENGTH, length);
        return unit;
    }

    /// {@return the catalog in `unit`, an opened catalog blob}
    ///
    /// @throws LedgerCorruptedException if the content is malformed
    static Contents decode(byte[] unit) {
        try {
            var at = PageFormat.HEADER;
            var maxId = Bytes.i32(unit, at);
            var count = Bytes.i32(unit, at + 4);
            at += 8;
            var entries = new ArrayList<Entry>(count);
            for (var i = 0; i < count; i++) {
                var id = Bytes.i32(unit, at);
                var nameLength = Bytes.u16(unit, at + 4);
                var name = new String(unit, at + 6, nameLength, StandardCharsets.US_ASCII);
                at += 6 + nameLength;
                TreeRoot root = null;
                if (unit[at++] == 1) {
                    root = new TreeRoot(PageRef.read(unit, at), Bytes.i64(unit, at + PageRef.BYTES), unit[at + PageRef.BYTES + 8] == 1);
                    at += PageRef.BYTES + 9;
                }
                entries.add(new Entry(id, name, root));
            }
            return new Contents(maxId, entries);
        } catch (IndexOutOfBoundsException exception) {
            throw new LedgerCorruptedException("The catalog of the data file is malformed", exception);
        }
    }

    /// {@return a unit holding the free list of `runs`, pairs of first page and length, its content length in the
    /// header}
    ///
    /// @param pages the pages of the unit
    static byte[] encodeFreeList(List<int[]> runs, int pages) {
        var unit = new byte[pages * PageFormat.SIZE];
        var at = PageFormat.HEADER;
        Bytes.i32(unit, at, runs.size());
        at += 4;
        for (var run : runs) {
            Bytes.i32(unit, at, run[0]);
            Bytes.i32(unit, at + 4, run[1]);
            at += 8;
        }
        Bytes.i32(unit, PageFormat.LENGTH, 4 + 8 * runs.size());
        return unit;
    }

    /// {@return the bytes the free list of `runs` runs takes}
    static long freeListLength(int runs) {
        return 4 + 8L * runs;
    }

    /// {@return the free pages the free list in `unit` holds}
    ///
    /// @throws LedgerCorruptedException if the content is malformed
    static BitSet decodeFreeList(byte[] unit, int pageCount) {
        try {
            var free = new BitSet(pageCount);
            var at = PageFormat.HEADER;
            var runs = Bytes.i32(unit, at);
            at += 4;
            for (var i = 0; i < runs; i++) {
                var start = Bytes.i32(unit, at);
                var length = Bytes.i32(unit, at + 4);
                if (start < PageFormat.FIRST_UNIT_PAGE || length < 1 || (long) start + length > pageCount) {
                    throw new LedgerCorruptedException("The free list of the data file names pages outside the file", null);
                }
                free.set(start, start + length);
                at += 8;
            }
            return free;
        } catch (IndexOutOfBoundsException exception) {
            throw new LedgerCorruptedException("The free list of the data file is malformed", exception);
        }
    }
}

package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/// The delta pages a checkpoint made durable and the records of each that its trees refer to.
///
/// A record dies when the checkpoint after it replaces it, writes its leaf whole or drops it, so its page holds fewer
/// live records with every checkpoint; a page holding none is free. Nothing in the trees says which records of a page
/// are live without reading every branch, so each checkpoint writes the table beside its free list, and an open reads
/// it back.
///
/// Content of the table blob, after the [PageFormat] header:
///
/// ```text
/// pages (4), then per page its number (4) and its live records (2), by number
/// ```
///
/// An instance is immutable.
final class DeltaTable {
    /// The table of a store with no delta page.
    static final DeltaTable EMPTY = new DeltaTable(new int[0], new int[0]);

    // By page number
    private final int[] pages;
    private final int[] records;

    private DeltaTable(int[] pages, int[] records) {
        this.pages = pages;
        this.records = records;
    }

    /// {@return whether no delta page is in use}
    boolean isEmpty() {
        return pages.length == 0;
    }

    /// {@return the delta pages in use}
    int size() {
        return pages.length;
    }

    /// {@return the live records of delta page `page`, 0 if it is not in use}
    int records(int page) {
        var index = Arrays.binarySearch(pages, page);
        return index < 0 ? 0 : records[index];
    }

    /// {@return this table after `changes`, by page the records it gained (positive) or lost (negative)}
    ///
    /// @param freed receives each delta page left with no live record
    /// @throws IllegalStateException if a page would hold fewer than no records, so the trees and the table disagree
    DeltaTable apply(Map<Integer, Integer> changes, List<PageRef> freed) {
        if (changes.isEmpty()) {
            return this;
        }
        var changed = changes.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
        var outPages = new int[pages.length + changed.length];
        var outRecords = new int[outPages.length];
        var size = 0;
        var i = 0;
        var j = 0;
        while (i < pages.length || j < changed.length) {
            int page;
            int count;
            if (j == changed.length || (i < pages.length && pages[i] < changed[j])) {
                page = pages[i];
                count = records[i++];
            } else {
                page = changed[j++];
                count = changes.get(page);
                if (i < pages.length && pages[i] == page) {
                    count += records[i++];
                }
            }
            if (count < 0) {
                throw new IllegalStateException("Delta page " + page + " would hold " + count + " records");
            }
            if (count == 0) {
                freed.add(new PageRef(page, 1, 0, 0));
                continue;
            }
            outPages[size] = page;
            outRecords[size++] = count;
        }
        return new DeltaTable(Arrays.copyOf(outPages, size), Arrays.copyOf(outRecords, size));
    }

    /// {@return the bytes of the table's content}
    int length() {
        return 4 + 6 * pages.length;
    }

    /// {@return a unit of `unitPages` pages holding the table, its content filled in}
    byte[] encode(int unitPages) {
        var unit = new byte[unitPages * PageFormat.SIZE];
        var at = PageFormat.HEADER;
        Bytes.i32(unit, at, pages.length);
        at += 4;
        for (var i = 0; i < pages.length; i++) {
            Bytes.i32(unit, at, pages[i]);
            Bytes.u16(unit, at + 4, records[i]);
            at += 6;
        }
        return unit;
    }

    /// {@return the table held by `unit`, an opened table blob}
    static DeltaTable decode(byte[] unit) {
        var at = PageFormat.HEADER;
        var count = Bytes.i32(unit, at);
        if (count < 0 || 4 + 6L * count > Bytes.i32(unit, PageFormat.LENGTH)) {
            throw new LedgerCorruptedException("The table of delta pages is cut short", null);
        }
        at += 4;
        var pages = new int[count];
        var records = new int[count];
        for (var i = 0; i < count; i++) {
            pages[i] = Bytes.i32(unit, at);
            records[i] = Bytes.u16(unit, at + 4);
            at += 6;
        }
        return new DeltaTable(pages, records);
    }

    /// {@return the delta pages in use, by number, for tests}
    int[] pagesForTests() {
        return pages.clone();
    }
}

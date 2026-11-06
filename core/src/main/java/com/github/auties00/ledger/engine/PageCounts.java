package com.github.auties00.ledger.engine;

import java.util.Arrays;

/// A count by page number, which a checkpoint keeps of the delta records each delta page gained or lost.
///
/// An open-addressing table of `int`s: a boxed map spent a tenth of a checkpoint's update counting the records it
/// dropped. Not safe for concurrent use.
final class PageCounts {
    // Below this many pages an empty table is not shrunk back when cleared
    private static final int INITIAL = 64;

    // The page number plus one at each slot, 0 if the slot is empty
    private int[] keys;
    private int[] counts;
    private int size;

    PageCounts() {
        this.keys = new int[INITIAL];
        this.counts = new int[INITIAL];
    }

    /// Adds `change` to the count of `page`, a page number.
    void add(int page, int change) {
        var mask = keys.length - 1;
        var key = page + 1;
        var slot = mix(key) & mask;
        while (true) {
            var at = keys[slot];
            if (at == key) {
                counts[slot] += change;
                return;
            }
            if (at == 0) {
                keys[slot] = key;
                counts[slot] = change;
                if (++size * 2 > keys.length) {
                    grow();
                }
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    /// Adds every count of `other` to this one's.
    void addAll(PageCounts other) {
        for (var slot = 0; slot < other.keys.length; slot++) {
            if (other.keys[slot] != 0) {
                add(other.keys[slot] - 1, other.counts[slot]);
            }
        }
    }

    /// {@return whether no page has a count}
    boolean isEmpty() {
        return size == 0;
    }

    /// {@return the pages that have a count, ascending}
    int[] pages() {
        var pages = new int[size];
        var next = 0;
        for (var key : keys) {
            if (key != 0) {
                pages[next++] = key - 1;
            }
        }
        Arrays.sort(pages);
        return pages;
    }

    /// {@return the count of `page`, 0 if it has none}
    int count(int page) {
        var mask = keys.length - 1;
        var key = page + 1;
        for (var slot = mix(key) & mask; ; slot = (slot + 1) & mask) {
            var at = keys[slot];
            if (at == key) {
                return counts[slot];
            }
            if (at == 0) {
                return 0;
            }
        }
    }

    /// Forgets every count.
    void clear() {
        if (keys.length > INITIAL) {
            keys = new int[INITIAL];
            counts = new int[INITIAL];
        } else {
            Arrays.fill(keys, 0);
        }
        size = 0;
    }

    private void grow() {
        var oldKeys = keys;
        var oldCounts = counts;
        keys = new int[oldKeys.length * 2];
        counts = new int[oldKeys.length * 2];
        var mask = keys.length - 1;
        for (var i = 0; i < oldKeys.length; i++) {
            var key = oldKeys[i];
            if (key != 0) {
                var slot = mix(key) & mask;
                while (keys[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = key;
                counts[slot] = oldCounts[i];
            }
        }
    }

    // Spreads page numbers, which a checkpoint allocates in runs, over the table
    private static int mix(int key) {
        var mixed = key * 0x9E3779B9;
        return mixed ^ (mixed >>> 16);
    }
}

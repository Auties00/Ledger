package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/// The versions of a key space whose values units of [BlobFile] hold, and which no published checkpoint has taken, in
/// the order of their keys for the range removals that replace them.
///
/// A commit adds its versions at the end, in constant time each, unordered; the first lookup of a range after some
/// were added sorts them and merges them with the others. A range removal then visits the versions in its range only,
/// where a history sync can leave tens of thousands in flight until the next checkpoint, and commits that add
/// thousands at a time and remove no range pay no order.
///
/// It is not safe for concurrent use: the store calls it holding the publishing role.
final class InFlightUnits {
    private static final Comparator<Entry> BY_KEY = (left, right) -> Chain.compareKeys(left.chain(), right.chain());

    // In the order of their keys, those of a key in the order they were added
    private ArrayList<Entry> ordered;
    // Added since the last lookup of a range, in the order of their commits
    private ArrayList<Entry> added;

    InFlightUnits() {
        this.ordered = new ArrayList<>();
        this.added = new ArrayList<>();
    }

    /// Adds `version`, a version of the key of `chain` that a commit published.
    void add(Chain chain, Version.External version) {
        added.add(new Entry(chain, version));
    }

    /// {@return the versions held}
    int size() {
        return ordered.size() + added.size();
    }

    /// Gives `action` every version held, in no particular order.
    void forEach(Consumer<Version.External> action) {
        for (var entry : ordered) {
            action.accept(entry.version());
        }
        for (var entry : added) {
            action.accept(entry.version());
        }
    }

    /// Gives `action` every version of a key from `from` to `to`, in the order of their keys.
    ///
    /// @param from the lowest key, or `null` for no lower bound
    /// @param to the first key past the range, or `null` for no upper bound
    void forEachIn(MemorySegment from, MemorySegment to, Consumer<Version.External> action) {
        order();
        var low = 0;
        var high = ordered.size();
        if (from != null) {
            while (low < high) {
                var middle = (low + high) >>> 1;
                if (ordered.get(middle).chain().compareKey(from) < 0) {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
        }
        for (var i = low; i < ordered.size(); i++) {
            var entry = ordered.get(i);
            if (to != null && entry.chain().compareKey(to) >= 0) {
                return;
            }
            action.accept(entry.version());
        }
    }

    /// Removes every version `filter` accepts.
    void removeIf(Predicate<Version.External> filter) {
        // An emptied list lets go of its array, which the versions of a history sync grew to tens of thousands
        if (ordered.removeIf(entry -> filter.test(entry.version())) && ordered.isEmpty()) {
            ordered = new ArrayList<>();
        }
        if (added.removeIf(entry -> filter.test(entry.version())) && added.isEmpty()) {
            added = new ArrayList<>();
        }
    }

    /// Sorts the versions added since the last lookup and merges them with the others.
    private void order() {
        if (added.isEmpty()) {
            return;
        }
        // Stable, so the versions of a key keep the order of their commits
        added.sort(BY_KEY);
        if (ordered.isEmpty()) {
            var sorted = added;
            added = ordered;
            ordered = sorted;
            return;
        }
        var merged = new ArrayList<Entry>(ordered.size() + added.size());
        var left = 0;
        var right = 0;
        while (left < ordered.size() && right < added.size()) {
            // An earlier version of a key goes first
            if (BY_KEY.compare(added.get(right), ordered.get(left)) < 0) {
                merged.add(added.get(right++));
            } else {
                merged.add(ordered.get(left++));
            }
        }
        while (left < ordered.size()) {
            merged.add(ordered.get(left++));
        }
        while (right < added.size()) {
            merged.add(added.get(right++));
        }
        ordered = merged;
        added.clear();
    }

    /// A version in flight and the chain of its key.
    private record Entry(Chain chain, Version.External version) {
    }
}

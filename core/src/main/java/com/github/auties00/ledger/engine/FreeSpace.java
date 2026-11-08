package com.github.auties00.ledger.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/// The pages of the data file a checkpoint may write.
///
/// A page is free when no durable state uses it and no reader may read it. A checkpoint frees the pages of the units
/// its tree replaces, but readers that started before it was published may still read them through the tree they
/// pinned, so those pages wait in a pending list stamped with the sequence the store had when the checkpoint was
/// published, until every reader is past it ([#releasePassed(long)]). Pages a checkpoint took and then gave up never
/// reached a durable state, and are free at once.
///
/// It is safe for concurrent use: the writer releases pages while a checkpoint takes them.
final class FreeSpace {
    private final Object lock;
    private final BitSet free;
    // No page below it is free: a checkpoint takes hundreds of thousands of pages one at a time, and each search from
    // the first page of the file walked every word of the pages in use before the first free one
    private int lowestFree;
    // Where the next search for a run of free pages starts: the runs before it were too short when a search of this
    // pass went past them, and searching them again for every run cost more than the writes the runs saved
    private int runCursor;
    private int pageCount;
    private final ArrayDeque<Pending> pending;

    /// Creates the free space of a file of `pageCount` pages whose free pages are the set bits of `free`.
    FreeSpace(int pageCount, BitSet free) {
        this.lock = new Object();
        this.free = free;
        this.lowestFree = PageFormat.FIRST_UNIT_PAGE;
        this.pageCount = pageCount;
        this.pending = new ArrayDeque<>();
    }

    /// {@return the first page of a run of `pages` free pages, which are no longer free}
    ///
    /// It takes the first run that fits, or the pages past the end of the file.
    int allocate(int pages) {
        synchronized (lock) {
            var start = free.nextSetBit(lowestFree);
            lowestFree = start < 0 ? Math.max(lowestFree, pageCount) : start;
            while (start >= 0 && start < pageCount) {
                var end = Math.min(free.nextClearBit(start), pageCount);
                if (end - start >= pages) {
                    free.clear(start, start + pages);
                    return start;
                }
                if (end == pageCount) {
                    // The last run reaches the end of the file: grow it
                    free.clear(start, end);
                    pageCount = start + pages;
                    return start;
                }
                start = free.nextSetBit(end);
            }
            var page = pageCount;
            pageCount += pages;
            return page;
        }
    }

    /// {@return the first page of each of the units of `pages` pages a caller writes together, none of them free any more}
    ///
    /// They take the first run that holds them all, one after another. Without one they take the runs too short for
    /// all of them, from the first, as far as those go, then the pages past the end of the file: what a removal of some
    /// units written together frees is as short as those units, and a file that took only runs holding a whole commit
    /// would grow for good beside them.
    ///
    /// @param total the pages of all the units
    int[] allocateAll(int[] pages, int total) {
        synchronized (lock) {
            var firsts = new int[pages.length];
            var lowest = free.nextSetBit(lowestFree);
            lowestFree = lowest < 0 ? Math.max(lowestFree, pageCount) : lowest;
            var start = lowest;
            while (start >= 0 && start < pageCount) {
                var end = Math.min(free.nextClearBit(start), pageCount);
                if (end - start >= total) {
                    free.clear(start, start + total);
                    for (var i = 0; i < pages.length; i++) {
                        firsts[i] = start;
                        start += pages[i];
                    }
                    return firsts;
                }
                start = free.nextSetBit(end);
            }
            // Where the search of the next unit starts, and the fewest pages no run from there holds: each unit moves
            // the first on, so a commit walks the runs of the file about once, however many units it holds
            var from = lowest;
            var noRun = Integer.MAX_VALUE;
            for (var i = 0; i < pages.length; i++) {
                var run = pages[i] < noRun ? from : -1;
                while (run >= 0 && run < pageCount && Math.min(free.nextClearBit(run), pageCount) - run < pages[i]) {
                    run = free.nextSetBit(free.nextClearBit(run));
                }
                if (run >= 0 && run < pageCount) {
                    free.clear(run, run + pages[i]);
                    firsts[i] = run;
                    from = free.nextSetBit(run + pages[i]);
                } else {
                    noRun = Math.min(noRun, pages[i]);
                    // Past the end of the file, from the free pages that reach it
                    var tail = free.previousClearBit(pageCount - 1) + 1;
                    free.clear(tail, pageCount);
                    firsts[i] = tail;
                    pageCount = tail + pages[i];
                }
            }
            return firsts;
        }
    }

    /// Takes a run of free pages for units written one after another: the next run of at least `least` pages since
    /// the last [#rewindRuns()], up to `most` of them, or `most` pages past the end of the file.
    ///
    /// @return the first page of the run in the high 32 bits, and its pages in the low ones
    long allocateRun(int least, int most) {
        synchronized (lock) {
            var start = free.nextSetBit(Math.max(lowestFree, runCursor));
            while (start >= 0 && start < pageCount) {
                var end = Math.min(free.nextClearBit(start), pageCount);
                if (end == pageCount && end - start < most) {
                    // The last run reaches the end of the file: grow it
                    free.clear(start, end);
                    pageCount = start + most;
                    runCursor = pageCount;
                    return (long) start << 32 | most;
                }
                if (end - start >= least) {
                    var pages = Math.min(end - start, most);
                    free.clear(start, start + pages);
                    runCursor = start + pages;
                    return (long) start << 32 | pages;
                }
                start = free.nextSetBit(end);
            }
            var first = pageCount;
            pageCount += most;
            runCursor = pageCount;
            return (long) first << 32 | most;
        }
    }

    /// Makes the next [#allocateRun(int, int)] search from the first free page again, once per checkpoint.
    void rewindRuns() {
        synchronized (lock) {
            runCursor = 0;
        }
    }

    /// Takes the `pages` pages from `page`, growing the file to reach them, while the store opens.
    ///
    /// @return whether they were all free
    boolean claim(int page, int pages) {
        synchronized (lock) {
            if (page < PageFormat.FIRST_UNIT_PAGE) {
                return false;
            }
            if (page + pages > pageCount) {
                // Pages past the file are free; those between its end and the claim stay so
                free.set(pageCount, page + pages);
                lowestFree = Math.min(lowestFree, pageCount);
                pageCount = page + pages;
            }
            var taken = free.get(page, page + pages);
            if (taken.cardinality() != pages) {
                return false;
            }
            free.clear(page, page + pages);
            return true;
        }
    }

    /// Makes the `pages` pages from `page` free at once.
    void release(int page, int pages) {
        synchronized (lock) {
            free.set(page, page + pages);
            lowestFree = Math.min(lowestFree, page);
        }
    }

    /// Frees `units` once every reader is past `published`.
    void pend(long published, List<PageRef> units) {
        if (units.isEmpty()) {
            return;
        }
        synchronized (lock) {
            pending.add(new Pending(published, units));
        }
    }

    /// Frees the pending pages of every checkpoint published at a sequence below `oldestReader`.
    ///
    /// @param oldestReader the lowest sequence a reader reads at, or [Long#MAX_VALUE] if none is reading
    void releasePassed(long oldestReader) {
        synchronized (lock) {
            while (!pending.isEmpty() && pending.peekFirst().published() < oldestReader) {
                for (var unit : pending.pollFirst().units()) {
                    free.set(unit.page(), unit.page() + unit.pages());
                    lowestFree = Math.min(lowestFree, unit.page());
                }
            }
        }
    }

    /// {@return the pages of the file in use, those past it being free}
    int pageCount() {
        synchronized (lock) {
            return pageCount;
        }
    }

    /// Ends the file before its trailing free pages.
    ///
    /// @return the new page count, or -1 if the file ends with a page in use
    int trim() {
        return trim(0);
    }

    /// Ends the file before its trailing free pages, if there are at least `slack` of them.
    ///
    /// @return the new page count, or -1 if the file keeps its length
    int trim(int slack) {
        synchronized (lock) {
            var last = free.previousClearBit(pageCount - 1);
            var end = Math.max(PageFormat.FIRST_UNIT_PAGE, last + 1);
            if (end >= pageCount || pageCount - end < slack) {
                return -1;
            }
            free.clear(end, pageCount);
            pageCount = end;
            return end;
        }
    }

    /// {@return the runs of pages a store reopened at the checkpoint being written may use, as pairs of first page and
    /// length: the free pages, the pending ones and `alsoFree`, since a reopened store has no reader}
    ///
    /// @param alsoFree units the checkpoint frees
    List<int[]> durableRuns(List<PageRef> alsoFree) {
        synchronized (lock) {
            var all = (BitSet) free.clone();
            for (var waiting : pending) {
                for (var unit : waiting.units()) {
                    all.set(unit.page(), unit.page() + unit.pages());
                }
            }
            for (var unit : alsoFree) {
                all.set(unit.page(), unit.page() + unit.pages());
            }
            var runs = new ArrayList<int[]>();
            var start = all.nextSetBit(PageFormat.FIRST_UNIT_PAGE);
            while (start >= 0 && start < pageCount) {
                var end = Math.min(all.nextClearBit(start), pageCount);
                runs.add(new int[]{start, end - start});
                start = all.nextSetBit(end);
            }
            return runs;
        }
    }

    /// {@return whether `page` is free or pending, for tests}
    boolean isUnused(int page) {
        synchronized (lock) {
            if (free.get(page) || page >= pageCount) {
                return true;
            }
            for (var waiting : pending) {
                for (var unit : waiting.units()) {
                    if (page >= unit.page() && page < unit.page() + unit.pages()) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /// Pages a published checkpoint freed, waiting for the readers of the tree before it.
    ///
    /// @param published the latest sequence when the checkpoint was published
    /// @param units the units it freed
    private record Pending(long published, List<PageRef> units) {
    }
}

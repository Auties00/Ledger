package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The free pages of the data file as checkpoints take, stage, pend and release them.
class FreeSpaceTest {
    private static final int FIRST = PageFormat.FIRST_UNIT_PAGE;

    private static BitSet listed(List<int[]> runs) {
        var listed = new BitSet();
        for (var run : runs) {
            listed.set(run[0], run[0] + run[1]);
        }
        return listed;
    }

    private static List<PageRef> units(int... pages) {
        var units = new ArrayList<PageRef>();
        for (var page : pages) {
            units.add(new PageRef(page, 1, 0, 0));
        }
        return units;
    }

    @Test
    @DisplayName("staged pages are listed free in every free list, but no allocation takes them until they are pended and released")
    void stagedPages() {
        var space = new FreeSpace(FIRST, new BitSet());
        var first = space.allocate(4);
        assertEquals(FIRST, first);
        var staged = units(first, first + 1);
        space.stage(staged);
        var runs = listed(space.durableRuns(List.of()));
        assertTrue(runs.get(first) && runs.get(first + 1), "the staged pages are not listed free");
        assertFalse(runs.get(first + 2), "a page in use is listed free");
        assertEquals(first + 4, space.allocate(2), "an allocation took a staged page");
        space.pend(10, staged);
        runs = listed(space.durableRuns(List.of()));
        assertTrue(runs.get(first) && runs.get(first + 1), "the pended pages are not listed free");
        assertEquals(first + 6, space.allocate(1), "an allocation took a pending page");
        space.releasePassed(11);
        assertEquals(first, space.allocate(2), "the released pages were not taken first");
        runs = listed(space.durableRuns(List.of()));
        assertFalse(runs.get(first) || runs.get(first + 1), "pages taken again are still listed free, as staged ones");
    }

    @Test
    @DisplayName("unstaged pages stay in use, as a checkpoint that will not publish leaves them")
    void unstagedPages() {
        var space = new FreeSpace(FIRST, new BitSet());
        var first = space.allocate(3);
        var staged = units(first + 1);
        space.stage(staged);
        space.stage(List.of());
        assertTrue(listed(space.durableRuns(List.of())).get(first + 1));
        space.unstage(staged);
        var runs = listed(space.durableRuns(units(first + 2)));
        assertFalse(runs.get(first + 1), "an unstaged page is listed free");
        assertTrue(runs.get(first + 2), "a unit the checkpoint frees is not listed free");
        assertFalse(space.isUnused(first + 1));
        assertEquals(first + 3, space.allocate(1), "an allocation took an unstaged page");
    }
}

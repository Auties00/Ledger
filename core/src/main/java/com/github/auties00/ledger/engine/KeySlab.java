package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/// The copies of the keys a key space's chains hold, packed into small shared arrays.
///
/// A key copied into an array of its own costs the array's header and, for the read-only view of it, a segment and a
/// memory session of the segment's own: about a hundred bytes for a 16-byte key, on every key a commit writes. A copy
/// here is a slice of a chunk's one read-only view, which shares that view's session. A chunk stays reachable while a
/// chain holds a key in it, so chunks are small: a long-lived chain keeps a few kilobytes, not more.
///
/// It is safe for concurrent use: a copy claims its bytes with one atomic add, and a full chunk is replaced with a
/// compare-and-set. Each thread copies into the chunk of a stripe of its own, by its id, as threads that shared one
/// chunk queued on its counter: eight writers committing new keys lost a tenth of their rate.
final class KeySlab {
    /// The bytes of a chunk.
    private static final int CHUNK = 4_096;
    /// The longest key a chunk takes; a longer one gets an array of its own, as the chunk would waste more than it saves.
    private static final int LARGEST = CHUNK / 16;
    /// The stripes, a power of two: twice the processors, so threads running at once seldom share one.
    private static final int STRIPES = Integer.highestOneBit(Math.clamp(2L * Runtime.getRuntime().availableProcessors() - 1, 1, 63)) << 1;

    private final AtomicReferenceArray<Chunk> stripes;

    KeySlab() {
        this.stripes = new AtomicReferenceArray<>(STRIPES);
        for (var i = 0; i < STRIPES; i++) {
            stripes.set(i, new Chunk());
        }
    }

    /// {@return a read-only copy of `key`}
    MemorySegment copy(MemorySegment key) {
        var length = (int) key.byteSize();
        if (length > LARGEST) {
            return Segments.copy(key);
        }
        // Thread ids are consecutive: a multiplicative hash spreads them over the stripes
        var stripe = (int) ((Thread.currentThread().threadId() * 0x9E3779B97F4A7C15L) >>> 40) & (STRIPES - 1);
        while (true) {
            var chunk = stripes.get(stripe);
            var start = chunk.used.getAndAdd(length);
            if (start + length <= CHUNK) {
                MemorySegment.copy(key, 0, chunk.writable, start, length);
                return chunk.view.asSlice(start, length);
            }
            // Full: the claim past its end is lost, and every thread that saw it full tries to replace it
            stripes.compareAndSet(stripe, chunk, new Chunk());
        }
    }

    /// An array keys are copied into, and the views of it.
    private static final class Chunk {
        private final MemorySegment writable;
        private final MemorySegment view;
        private final AtomicInteger used;

        private Chunk() {
            this.writable = MemorySegment.ofArray(new byte[CHUNK]);
            this.view = writable.asReadOnly();
            this.used = new AtomicInteger();
        }
    }
}

package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReferenceArray;

/// The copies of the keys a key space's chains hold, packed into small shared arrays.
///
/// A key copied into an array of its own costs the array's header and, for the read-only view of it, a segment and a
/// memory session of the segment's own: about a hundred bytes for a 16-byte key, on every key a commit writes. A copy
/// here is a span of a chunk's one read-only view, which its chain holds as the view, an offset and a length: no
/// object of its own. A chunk stays reachable while a chain holds a key in it, so chunks are small: a long-lived chain
/// keeps a few kilobytes, not more. A stripe's first chunk is small too, and each that fills is followed by one twice
/// its size: a key space of a few keys keeps a few hundred bytes of chunks, not kilobytes.
///
/// It is safe for concurrent use: a copy claims its bytes with a compare-and-set, and a full chunk is replaced with
/// another. Every key goes to one stripe until two threads claim from its chunk at once; from then on each thread
/// copies into the chunk of a stripe of its own, by its id, as threads that shared one chunk queued on its counter:
/// eight writers committing new keys lost a tenth of their rate. Striping from the start had a key space written by
/// many threads at different times, as virtual threads write it, keep a chunk for every stripe.
final class KeySlab {
    /// The bytes of a chunk, at most.
    private static final int CHUNK = 4_096;
    /// The longest key a chunk takes; a longer one gets an array of its own, as the chunk would waste more than it saves.
    private static final int LARGEST = CHUNK / 16;
    /// The bytes of a stripe's first chunk, which takes the longest key.
    private static final int FIRST_CHUNK = LARGEST;
    /// The stripes, a power of two: twice the processors, so threads running at once seldom share one.
    private static final int STRIPES = Integer.highestOneBit(Math.clamp(2L * Runtime.getRuntime().availableProcessors() - 1, 1, 63)) << 1;
    private static final VarHandle USED;

    static {
        try {
            USED = MethodHandles.lookup().findVarHandle(Chunk.class, "used", int.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private final AtomicReferenceArray<Chunk> stripes;
    // Whether two threads claimed from one chunk at once, after which each thread copies into the stripe of its id;
    // only ever set
    private volatile boolean contended;

    /// Makes a slab whose stripes take their first chunk with their first key.
    KeySlab() {
        this.stripes = new AtomicReferenceArray<>(STRIPES);
    }

    /// {@return the chain of a read-only copy of `key`, whose hash is `hash`}
    Chain chain(MemorySegment key, int hash) {
        var length = (int) key.byteSize();
        if (length > LARGEST) {
            return new Chain(Segments.copy(key), hash);
        }
        var stripe = contended ? stripe() : 0;
        while (true) {
            var chunk = stripes.get(stripe);
            if (chunk == null) {
                stripes.compareAndSet(stripe, null, new Chunk(FIRST_CHUNK));
                continue;
            }
            var start = (int) USED.getVolatile(chunk);
            if (start + length > chunk.array.length) {
                // Full: every thread that saw it full tries to replace it, with a chunk twice as large
                stripes.compareAndSet(stripe, chunk, new Chunk(Math.min(CHUNK, 2 * chunk.array.length)));
                continue;
            }
            if (USED.compareAndSet(chunk, start, start + length)) {
                MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, chunk.array, start, length);
                return new Chain(chunk.view, start, length, hash);
            }
            // Another thread claimed from the chunk meanwhile
            if (!contended) {
                contended = true;
            }
            stripe = stripe();
        }
    }

    /// {@return the bytes claimed in the chunks the stripes hold now, not counting those of chunks replaced or keys
    /// given arrays of their own; for tests}
    int usedForTests() {
        var used = 0;
        for (var stripe = 0; stripe < STRIPES; stripe++) {
            var chunk = stripes.get(stripe);
            if (chunk != null) {
                used += (int) USED.getVolatile(chunk);
            }
        }
        return used;
    }

    // Thread ids are consecutive: a multiplicative hash spreads them over the stripes
    private static int stripe() {
        return (int) ((Thread.currentThread().threadId() * 0x9E3779B97F4A7C15L) >>> 40) & (STRIPES - 1);
    }

    /// An array keys are copied into, and the read-only view of it chains hold.
    private static final class Chunk {
        private final byte[] array;
        private final MemorySegment view;
        // The bytes claimed, through USED
        @SuppressWarnings("unused")
        private volatile int used;

        private Chunk(int capacity) {
            this.array = new byte[capacity];
            this.view = MemorySegment.ofArray(array).asReadOnly();
        }
    }
}

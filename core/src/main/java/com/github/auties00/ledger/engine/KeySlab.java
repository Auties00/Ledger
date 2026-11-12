package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReferenceArray;

/// The copies of the keys a key space's chains hold, packed into small shared arrays.
///
/// A key of at most [Chain#INLINE] bytes goes into the words of its chain instead, which a comparison reads without a
/// load that waits for the chain's own. A holder that keeps such a key as a segment for as long as the slab's other
/// copies live takes a copy of it here ([#key(Chain)]): the ordered map of a key space, whose keys are segments, as the
/// chain enters it, and the update of a tree, with a slab of its own, for the entries it writes. A copy only a short
/// lookup needs is made elsewhere ([Chain#key()]), since a copy here stays reachable while another copy in its chunk
/// does: the copies of a key space's scans and removals would stay with its map's keys.
///
/// A key copied into an array of its own costs the array's header and, for the read-only view of it, a segment and a
/// memory session of the segment's own: about a hundred bytes for a 16-byte key, on every key a commit writes. A copy
/// here is a span of a chunk's one read-only view, which its chain holds as the view, an offset and a length: no
/// object of its own. A chunk stays reachable while a chain or a map holds a key in it, so chunks are small: a
/// long-lived key keeps a few kilobytes, not more. A stripe's first chunk is small too, and each that fills is followed
/// by one twice its size: a key space of a few keys keeps a few hundred bytes of chunks, not kilobytes.
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
        if (length <= Chain.INLINE) {
            return Chain.inline(key, hash);
        }
        if (length > LARGEST) {
            return new Chain(Segments.copy(key), hash);
        }
        var stripe = contended ? stripe() : 0;
        while (true) {
            var chunk = room(stripe);
            var start = (int) USED.getVolatile(chunk);
            if (start + length <= chunk.array.length && USED.compareAndSet(chunk, start, start + length)) {
                MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, chunk.array, start, length);
                return new Chain(chunk.view, start, length, hash);
            }
            stripe = retry(stripe, chunk, start, length);
        }
    }

    /// {@return the key of `chain`, a read-only segment the ordered map may keep as long as it holds the chain}
    ///
    /// For a chain whose key is in a segment, a view of that segment; for one whose key is in its own words, a copy
    /// here, which costs the map the bytes of the key and a view, as a key in a slab does, rather than an array of its
    /// own as well. A lookup that drops the key when it returns takes [Chain#key()] instead, as a copy here stays
    /// reachable while any other copy in its chunk does.
    MemorySegment key(Chain chain) {
        if (!chain.inlineKey()) {
            return chain.key();
        }
        var length = chain.keyLength();
        var stripe = contended ? stripe() : 0;
        while (true) {
            var chunk = room(stripe);
            var start = (int) USED.getVolatile(chunk);
            if (start + length <= chunk.array.length && USED.compareAndSet(chunk, start, start + length)) {
                chain.copyKey(chunk.array, start);
                return chunk.view.asSlice(start, length);
            }
            stripe = retry(stripe, chunk, start, length);
        }
    }

    // The chunk of `stripe`, made if it has none yet
    private Chunk room(int stripe) {
        while (true) {
            var chunk = stripes.get(stripe);
            if (chunk != null) {
                return chunk;
            }
            stripes.compareAndSet(stripe, null, new Chunk(FIRST_CHUNK));
        }
    }

    // What a claim of `length` bytes at `start` of `chunk`, the chunk of `stripe`, that failed does next: it replaces a
    // full chunk, and moves to the stripe of its thread if another thread claimed from the chunk meanwhile
    private int retry(int stripe, Chunk chunk, int start, int length) {
        if (start + length > chunk.array.length) {
            // Full: every thread that saw it full tries to replace it, with a chunk twice as large
            stripes.compareAndSet(stripe, chunk, new Chunk(Math.min(CHUNK, 2 * chunk.array.length)));
            return stripe;
        }
        if (!contended) {
            contended = true;
        }
        return stripe();
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

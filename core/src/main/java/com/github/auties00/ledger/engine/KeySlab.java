package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

/// The copies of the keys a key space's chains hold, packed into small shared arrays.
///
/// A key copied into an array of its own costs the array's header and, for the read-only view of it, a segment and a
/// memory session of the segment's own: about a hundred bytes for a 16-byte key, on every key a commit writes. A copy
/// here is a slice of a chunk's one read-only view, which shares that view's session. A chunk stays reachable while a
/// chain holds a key in it, so chunks are small: a long-lived chain keeps a few kilobytes, not more.
///
/// It is safe for concurrent use: a copy claims its bytes with one atomic add, and a full chunk is replaced with a
/// compare-and-set.
final class KeySlab {
    /// The bytes of a chunk.
    private static final int CHUNK = 4_096;
    /// The longest key a chunk takes; a longer one gets an array of its own, as the chunk would waste more than it saves.
    private static final int LARGEST = CHUNK / 16;

    @SuppressWarnings("unused")
    private volatile Chunk current;

    private static final VarHandle CURRENT;

    static {
        try {
            CURRENT = MethodHandles.lookup().findVarHandle(KeySlab.class, "current", Chunk.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    KeySlab() {
        this.current = new Chunk();
    }

    /// {@return a read-only copy of `key`}
    MemorySegment copy(MemorySegment key) {
        var length = (int) key.byteSize();
        if (length > LARGEST) {
            return Segments.copy(key);
        }
        while (true) {
            var chunk = current;
            var start = chunk.used.getAndAdd(length);
            if (start + length <= CHUNK) {
                MemorySegment.copy(key, 0, chunk.writable, start, length);
                return chunk.view.asSlice(start, length);
            }
            // Full: the claim past its end is lost, and every thread that saw it full tries to replace it
            CURRENT.compareAndSet(this, chunk, new Chunk());
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

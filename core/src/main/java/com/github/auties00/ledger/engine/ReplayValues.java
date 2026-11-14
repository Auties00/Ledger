package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/// The arrays an open copies the values it replays to paged key spaces into, so their versions keep none of the
/// decrypted frame bodies they came in.
///
/// A body also holds the commit's keys, which the chains copy, its resident values, which the resident versions copy,
/// and its own encoding: a version on a slice of it would keep about twice the bytes of its value, and the slice, until
/// a checkpoint takes it. Here a value goes, whole, into the current chunk of [#CHUNK_BYTES], whose read-only view its
/// version keeps, as a version on the [ValueSlab] or a batch's rope does; one longer than [#SHARED_BYTES], or an empty
/// one, takes an array of its own, so a chunk leaves fewer than that many bytes unused at its end. A chunk is a new array
/// no pool hands out or takes back, and nothing writes again where a value was copied, so it stays as the copy left it
/// until a checkpoint takes the last version on it.
///
/// A store that moves values off the heap copies here only the values of the log's last budget, and those too short for
/// the log: it moves the others to the log at once, tagged where they lie in their bodies with the [SpillQueue.Crypto]
/// this holds for the replay, so the bytes of most of a long log's values are never copied.
///
/// One open makes one for its replay, which runs on the opening thread before the store starts its threads, and drops
/// it after: the last chunk is then reachable through its versions alone.
final class ReplayValues {
    /// The bytes of a chunk: below half a G1 region of a 4 GiB heap, as the batches' chunks are, so not humongous.
    static final int CHUNK_BYTES = FrameBody.CHUNK_BYTES;
    /// The most bytes of a value that shares a chunk, a sixteenth of one, which bounds what a chunk leaves unused.
    static final int SHARED_BYTES = CHUNK_BYTES / 16;

    // The chunk values are copied into, its read-only view, and the bytes of it they took; null before the first one
    private byte[] chunk;
    private MemorySegment view;
    private int fill;
    // The frame body the last commit replayed came in and its read-only view, which its commits share; null once the
    // replay ends
    private byte[] frame;
    private MemorySegment frameView;
    // The ciphers that tag the values moved to the log, taken at the first, or null
    private SpillQueue.Crypto crypto;

    /// {@return whether a value of `length` bytes shares a chunk, rather than taking an array of its own}
    static boolean shared(int length) {
        return length > 0 && length <= SHARED_BYTES;
    }

    /// Copies the `length` bytes at `offset` of `value`, which [#shared(int)] lets share a chunk, into the current
    /// chunk, after the values copied there before, or into a new chunk if it has no room for them.
    ///
    /// @return where the copy starts in [#view()]
    int copy(MemorySegment value, long offset, int length) {
        if (chunk == null || fill > CHUNK_BYTES - length) {
            chunk = new byte[CHUNK_BYTES];
            view = MemorySegment.ofArray(chunk).asReadOnly();
            fill = 0;
        }
        var at = fill;
        MemorySegment.copy(value, ValueLayout.JAVA_BYTE, offset, chunk, at, length);
        fill = at + length;
        return at;
    }

    /// {@return the read-only view of the chunk the last [#copy(MemorySegment, long, int)] copied into}
    MemorySegment view() {
        return view;
    }

    /// {@return a read-only segment of the whole of `body`, a frame body, the same for the commits of a run}
    MemorySegment frame(byte[] body) {
        if (frame != body) {
            frame = body;
            frameView = MemorySegment.ofArray(body).asReadOnly();
        }
        return frameView;
    }

    /// {@return the [SpillQueue.Crypto] of the replay, taken from `queue` at the first call}
    SpillQueue.Crypto crypto(SpillQueue queue) {
        var taken = crypto;
        if (taken == null) {
            taken = crypto = queue.takeCrypto();
        }
        return taken;
    }

    /// Forgets the last frame body and gives the [SpillQueue.Crypto] it took back to `queue`, as the replay ends.
    ///
    /// @param queue the queue the crypto came from, or `null` if the store moves no value
    void end(SpillQueue queue) {
        frame = null;
        frameView = null;
        if (crypto != null && queue != null) {
            queue.giveBack(crypto);
        }
        crypto = null;
    }
}

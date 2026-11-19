package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

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
/// It also holds the [PackedRun]s the replay builds of the writes of paged key spaces before the log's last budget, whose
/// shorter values lie in the same chunks.
///
/// One open makes one for its replay, which runs on the opening thread before the store starts its threads, and drops
/// it after: the last chunk is then reachable through its versions and runs alone.
final class ReplayValues {
    /// The bytes of a chunk: below half a G1 region of a 4 GiB heap, as the batches' chunks are, so not humongous.
    static final int CHUNK_BYTES = FrameBody.CHUNK_BYTES;
    /// The most bytes of a value that shares a chunk, a sixteenth of one, which bounds what a chunk leaves unused.
    static final int SHARED_BYTES = CHUNK_BYTES / 16;
    /// The entries from which a run's builder, once full, grows to the entries the rest of the log is expected to add
    /// rather than to twice its entries ([#reserve(PackedRun.Builder, long)]).
    static final int RESERVE_AT = 4096;
    /// The entries up to which a reservation may give a builder more than twice its entries, at most sixteen times:
    /// the arrays of 262,144 entries take 11.5 MB at most (12 bytes per entry, 32 more per value in the log), so an
    /// estimate too high costs that much at most past what doubling would have taken.
    static final int RESERVE_ENTRIES = 1 << 18;

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
    // Whether something the replay keeps refers to the body of a frame it replayed since releasesFrame last answered
    private boolean frameKept;
    // The runs being built, by key space, and the key spaces that replay to chains from a range removal on
    private final IdentityHashMap<KeyspaceState, PackedRun.Builder> runs = new IdentityHashMap<>();
    private final Set<KeyspaceState> toChains = Collections.newSetFromMap(new IdentityHashMap<>());
    // The bytes of log at most from the frame being replayed to the end
    private long logFrom;
    // Per run being built, the bytes of log at most from the frame of its first write to the end
    private final IdentityHashMap<PackedRun.Builder, Long> origins = new IdentityHashMap<>();

    /// Records that the replay reaches a frame that `logFrom` bytes of log at most start from: a run made from now on
    /// takes its first write there, which [#reserve(PackedRun.Builder, long)] measures the run's rate from.
    void reached(long logFrom) {
        this.logFrom = logFrom;
    }

    /// Makes `run`, full with at least [#RESERVE_AT] entries, grow next to the entries the frames before the log's last
    /// `budget` bytes are expected to give it: the entries per byte of log it took from the frame of its first write to
    /// the frame being replayed, times the bytes from that first frame to the last budget, and a sixteenth more.
    ///
    /// The room is at least a quarter more than the run holds, so the growth stays geometric when the estimate is low,
    /// and at most sixteen times the entries up to [#RESERVE_ENTRIES] entries and twice them past that, so an estimate
    /// too high (a log whose later frames overwrite the keys of its first ones) takes little more than doubling would.
    /// Nothing for a run that is not full, nor while the replay is still in the frame of the run's first write, whose
    /// writes give no rate: the arrays then double.
    void reserve(PackedRun.Builder run, long budget) {
        var size = run.size();
        if (size < RESERVE_AT || size < run.capacity()) {
            return;
        }
        var origin = origins.get(run);
        if (origin == null) {
            return;
        }
        var done = origin - logFrom;
        var moved = origin - budget;
        if (done <= 0 || moved <= done) {
            return;
        }
        var expected = (long) ((double) size * moved / done);
        var most = Math.max(2L * size, Math.min(16L * size, RESERVE_ENTRIES));
        run.reserve((int) Math.min(PackedRun.MAX_ENTRIES, Math.clamp(expected + expected / 16, size + size / 4, most)));
    }

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
    ///
    /// Recovery opens later frames into the arrays of frames nothing refers to ([#releasesFrame()]), so the segment
    /// may be that of an array a frame before held: it views the whole array, whatever frame it holds.
    MemorySegment frame(byte[] body) {
        if (frame != body) {
            frame = body;
            frameView = MemorySegment.ofArray(body).asReadOnly();
        }
        return frameView;
    }

    /// Records that something the replay keeps refers to the body of the frame it replays, whose array recovery then
    /// never opens another frame into.
    void keepFrame() {
        frameKept = true;
    }

    /// {@return whether nothing the replay keeps refers to the bodies of the frames it replayed since the last call},
    /// which the next call answers for the frames after them.
    boolean releasesFrame() {
        var released = !frameKept;
        frameKept = false;
        return released;
    }

    /// {@return the [SpillQueue.Crypto] of the replay, taken from `queue` at the first call}
    SpillQueue.Crypto crypto(SpillQueue queue) {
        var taken = crypto;
        if (taken == null) {
            taken = crypto = queue.takeCrypto();
        }
        return taken;
    }

    /// {@return the builder of the run of `state`, made at the first call, or `null` once the key space replays to
    /// chains ([#stopRun(KeyspaceState)])}
    PackedRun.Builder run(KeyspaceState state) {
        var run = runs.get(state);
        if (run == null && !toChains.contains(state)) {
            run = new PackedRun.Builder();
            runs.put(state, run);
            origins.put(run, logFrom);
        }
        return run;
    }

    /// {@return the builder of the run of `state`, or `null` if it has none, which no write goes to from now on: the
    /// key space replays to chains}
    PackedRun.Builder stopRun(KeyspaceState state) {
        toChains.add(state);
        var run = runs.remove(state);
        if (run != null) {
            origins.remove(run);
        }
        return run;
    }

    /// {@return the builder of the run of `state`, or `null` if it has none: no write went to it yet, or the key space
    /// replays to chains}
    PackedRun.Builder building(KeyspaceState state) {
        return runs.get(state);
    }

    /// {@return the key spaces whose runs are being built, a copy}
    List<KeyspaceState> runStates() {
        return List.copyOf(runs.keySet());
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

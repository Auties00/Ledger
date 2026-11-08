package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Comparator;

/// The versions of one key in memory, newest first, as both maps of its key space hold them.
///
/// A chain is created the first time a key is written since the store last dropped its chain, possibly by a commit
/// that has not been given a sequence yet; a commit attaches its version once the commits before it on the key have.
/// A chain is dropped once the trees hold every version it had, or when it is left empty; a reader that still holds a
/// dropped chain reads the tree instead, which holds what the chain had.
///
/// Every writer claims the chain before it attaches a version: the sequencer for a commit of the pipeline, the
/// writer itself in an exclusive operation. [#claimed] only grows, up to [#DROPPED], so a thread that read it and then
/// the head knows, if a compare-and-set from the value it read succeeds, that no write was claimed in between: that
/// is how any thread, holding no role, drops a chain whose writers have all published.
final class Chain {
    /// Orders chains as their keys.
    static final Comparator<Chain> BY_KEY = Chain::compareKeys;
    // The bits of keySpan that hold a key's length: as many as KeyCodec.MAX_LENGTH needs
    private static final int LENGTH_BITS = Integer.SIZE - Integer.numberOfLeadingZeros(KeyCodec.MAX_LENGTH);
    // The bits of keySpan above the length that hold the key's offset in its chunk, at most 4,096: where an empty key
    // lands in a full chunk
    private static final int OFFSET_BITS = 13;
    // The flags of keySpan, above the offset: the bytes of three fields of their own made the chain 56 bytes, 48 now
    private static final int ORDERED = 1 << 24;
    private static final int TAKEN_OUT = 1 << 25;
    private static final int BLIND = 1 << 26;
    private static final int SETTLED = 1 << 27;
    private static final int IN_TREE = 1 << 28;
    /// What [#blindState()] returns for a chain whose key no commit wrote blind.
    static final byte NOT_BLIND = -1;

    /// What [#claimed] holds once the chain is dropped; above every sequence, so the claims stay monotonic.
    static final long DROPPED = Long.MAX_VALUE;
    /// What [#claim(PendingCommit, long)] returns for a dropped chain.
    static final Object DROPPED_WRITER = new Object();

    // The read-only view of the chunk of a KeySlab, or of an array of its own, that holds the key: a segment of the
    // key's own was 40 of the bytes a buffered write took on the heap
    private final MemorySegment keyChunk;
    // Where the key starts in keyChunk, shifted left by LENGTH_BITS, and its length, which never change; and the flags,
    // which only get set, through SPAN, so a plain read gets the key's bits right whatever flags it sees
    @SuppressWarnings("FieldMayBeFinal")
    private int keySpan;
    /// The hash of the key's bytes, as the hash index uses it.
    final int hash;
    /// The newest version, or `null` before the writer attaches the first one and once it drops the chain.
    volatile Version head;
    /// While a key space lists the chain for a read of a range to order, the next chain listed, or `null`; written
    /// before the chain is listed, and by the thread that takes it off the list. Once the chain is ordered in a key
    /// space that links its chains, the chain below it in the ordered map, read through [#below()].
    ///
    /// A chain leaves the list before it is ordered, and is never listed again, so the two uses never meet: one field
    /// keeps the chain at 48 bytes.
    Chain link;
    /// The sequence of the last write claimed, 0 before any, or [#DROPPED]; only grows.
    @SuppressWarnings("unused")
    private volatile long claimed;
    /// The last commit of the pipeline given a write of the key, until its committing thread forgets it once it is
    /// published, or `null`; set by the sequencer only, so it needs no compare-and-set.
    @SuppressWarnings("unused")
    private volatile PendingCommit lastWriter;
    /// The cut whose dirty keys list the chain, so a chain is listed once per checkpoint; guarded by the publishing
    /// role. An `int`, which a key space's cuts do not outgrow in a lifetime of checkpoints, makes the chain 64 bytes
    /// rather than 72.
    int dirtyCut;

    private static final VarHandle CLAIMED;
    private static final VarHandle LAST_WRITER;
    private static final VarHandle HEAD;
    private static final VarHandle SPAN;
    private static final VarHandle LINK;

    static {
        if (LENGTH_BITS + OFFSET_BITS > Integer.numberOfTrailingZeros(ORDERED)) {
            throw new ExceptionInInitializerError("A key's span overlaps the flags of its chain");
        }
        try {
            var lookup = MethodHandles.lookup();
            SPAN = lookup.findVarHandle(Chain.class, "keySpan", int.class);
            CLAIMED = lookup.findVarHandle(Chain.class, "claimed", long.class);
            LAST_WRITER = lookup.findVarHandle(Chain.class, "lastWriter", PendingCommit.class);
            HEAD = lookup.findVarHandle(Chain.class, "head", Version.class);
            LINK = lookup.findVarHandle(Chain.class, "link", Chain.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    /// What the link of the lowest chain of an ordered map holds: no chain is below it.
    ///
    /// A `null` link is a chain not linked yet, whose reader asks the map for the chain below.
    static final Chain BOTTOM = new Chain(MemorySegment.ofArray(new byte[0]).asReadOnly(), 0);

    /// Makes `version` the newest version, with a release store, which publishes it to the readers that load
    /// [#head] after.
    void attach(Version version) {
        HEAD.setRelease(this, version);
    }

    /// Claims the write of `commit`, at `sequence`, holding the sequencing role.
    ///
    /// @return the commit of the pipeline given a write of the key before, which applies its writes first, or `null`;
    ///         [#DROPPED_WRITER] if the chain is dropped, in which case nothing changed and the key needs a chain of
    ///         its own
    Object claim(PendingCommit commit, long sequence) {
        if (!claimAlone(sequence)) {
            return DROPPED_WRITER;
        }
        var previous = (PendingCommit) LAST_WRITER.getVolatile(this);
        LAST_WRITER.setRelease(this, commit);
        return previous;
    }

    /// Claims a write at `sequence` of an operation that runs alone, or of the sequencer.
    ///
    /// @return whether it did; `false` if the chain is dropped, and the key needs a chain of its own
    boolean claimAlone(long sequence) {
        while (true) {
            var current = (long) CLAIMED.getVolatile(this);
            if (current == DROPPED) {
                return false;
            }
            if (current >= sequence || CLAIMED.compareAndSet(this, current, sequence)) {
                return true;
            }
        }
    }

    /// {@return the sequence of the last write claimed, or [#DROPPED]}
    long claimed() {
        return (long) CLAIMED.getVolatile(this);
    }

    /// Marks the chain dropped if the last write claimed is still the one at `claimed`.
    ///
    /// @return whether it did, so no write is claimed again
    boolean release(long claimed) {
        return CLAIMED.compareAndSet(this, claimed, DROPPED);
    }

    /// Forgets `commit` as the last writer once it is published, so the chain does not keep it.
    void forget(PendingCommit commit) {
        LAST_WRITER.compareAndSet(this, commit, null);
    }

    /// {@return whether the chain is dropped, or being dropped}
    boolean released() {
        return (long) CLAIMED.getVolatile(this) == DROPPED;
    }

    /// Makes the chain of `key`, a read-only segment of its own.
    Chain(MemorySegment key, int hash) {
        this(key, 0, Math.toIntExact(key.byteSize()), hash);
    }

    /// Makes the chain of the `length` bytes at `offset` of `keyChunk`, a read-only segment no one writes them in.
    Chain(MemorySegment keyChunk, long offset, int length, int hash) {
        if (length >>> LENGTH_BITS != 0) {
            throw new IllegalArgumentException("Keys are at most " + KeyCodec.MAX_LENGTH + " bytes: " + length);
        }
        if (offset >>> OFFSET_BITS != 0) {
            throw new IllegalArgumentException("A key starts within " + (1 << OFFSET_BITS) + " bytes of its chunk: " + offset);
        }
        this.keyChunk = keyChunk;
        this.keySpan = (int) offset << LENGTH_BITS | length;
        this.hash = hash;
        this.dirtyCut = -1;
    }

    /// {@return the key, a read-only segment made for the call}
    MemorySegment key() {
        return keyChunk.asSlice(keyOffset(), keyLength());
    }

    /// {@return the bytes of the key}
    int keyLength() {
        return keySpan & ((1 << LENGTH_BITS) - 1);
    }

    private long keyOffset() {
        return (keySpan >>> LENGTH_BITS) & ((1 << OFFSET_BITS) - 1);
    }

    /// {@return whether the ordered map holds the chain}
    ///
    /// In a key space that orders its chains eagerly, the writer attaches no version before it does; in one that
    /// orders them lazily, the chain is listed among those a read of a range orders first ([KeyspaceState#slice]).
    /// Either way every committed version a scan may see is in the map it walks.
    boolean ordered() {
        return ((int) SPAN.getVolatile(this) & ORDERED) != 0;
    }

    /// Records that the ordered map holds the chain, with a volatile update, which a read of [#dropped()] after it
    /// follows.
    void markOrdered() {
        SPAN.getAndBitwiseOr(this, ORDERED);
    }

    /// {@return the chain below this one in the ordered map, [#BOTTOM] if none is, or `null` if the key space did not
    /// link it, or the chain left the map and a repair let go of its link}
    ///
    /// An acquire load, which sees the link of the chain returned as it was when this link was set to it. The chain
    /// returned may have left the map since: its own link leads on down it.
    Chain below() {
        return (Chain) LINK.getAcquire(this);
    }

    /// Links `chain`, [#BOTTOM] for none or `null` to clear the link, as the one below this chain in the ordered map,
    /// with a release store, which publishes the link of `chain` to the readers that load this one after.
    void linkBelow(Chain chain) {
        LINK.setRelease(this, chain);
    }

    /// {@return whether the writer took the chain out of the maps, after which no version is attached to it again}
    boolean dropped() {
        return ((int) SPAN.getVolatile(this) & TAKEN_OUT) != 0;
    }

    /// Records that the writer took the chain out of the maps, with a volatile update, which a read of [#ordered()]
    /// after it follows.
    void markDropped() {
        SPAN.getAndBitwiseOr(this, TAKEN_OUT);
    }

    /// Records that a commit wrote the key without knowing whether the tree held it ([BlindWrites]).
    ///
    /// A chain has at most one such write, its first, since every later one finds a version in memory: so a
    /// checkpoint whose cut sees a version of the chain takes the blind write too.
    void markBlind() {
        SPAN.getAndBitwiseOr(this, BLIND);
    }

    /// Records whether the tree the blind write would have read held the key; every thread that finds out finds the
    /// same answer.
    void settle(boolean inTree) {
        SPAN.getAndBitwiseOr(this, BLIND | SETTLED | (inTree ? IN_TREE : 0));
    }

    /// {@return whether the tree the blind write would have read held the key, as [TreeSearch#FOUND] or
    /// [TreeSearch#ABSENT], [TreeSearch#UNKNOWN] until the checkpoint that takes the write, or a count, finds out, or
    /// [#NOT_BLIND] if no commit wrote the key blind}
    byte blindState() {
        var span = (int) SPAN.getVolatile(this);
        if ((span & SETTLED) != 0) {
            return (span & IN_TREE) != 0 ? TreeSearch.FOUND : TreeSearch.ABSENT;
        }
        return (span & BLIND) != 0 ? TreeSearch.UNKNOWN : NOT_BLIND;
    }

    /// {@return whether the key's bytes are those of `key`}
    boolean keyEquals(MemorySegment key) {
        return Segments.equal(keyChunk, keyOffset(), keyLength(), key, 0, key.byteSize());
    }

    /// {@return whether the keys of `left` and `right` are equal}
    static boolean sameKey(Chain left, Chain right) {
        return Segments.equal(left.keyChunk, left.keyOffset(), left.keyLength(), right.keyChunk, right.keyOffset(), right.keyLength());
    }

    /// {@return the order of the keys of `left` and `right`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    static int compareKeys(Chain left, Chain right) {
        return Segments.compare(left.keyChunk, left.keyOffset(), left.keyLength(), right.keyChunk, right.keyOffset(), right.keyLength());
    }

    /// {@return the order of the key and `other`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareKey(MemorySegment other) {
        return Segments.compare(keyChunk, keyOffset(), keyLength(), other, 0, other.byteSize());
    }

    /// {@return the 8 bytes of the key at `at`, big-endian, zero past its end}
    long word(long at) {
        var length = keyLength();
        var offset = keyOffset();
        if (at + Long.BYTES <= length) {
            return keyChunk.get(Segments.BIG_ENDIAN_LONG, offset + at);
        }
        var word = 0L;
        for (var i = at; i < length; i++) {
            word |= (keyChunk.get(ValueLayout.JAVA_BYTE, offset + i) & 0xFFL) << (56 - 8 * (i - at));
        }
        return word;
    }

}

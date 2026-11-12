package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Comparator;
import java.util.Objects;

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
///
/// A chain of a key of at most [#INLINE] bytes holds the key itself, in big-endian words of its own ([Inline8] to
/// [Inline32], zero past the key's end); a longer one, or a chain made of a segment, holds a span of a read-only
/// segment ([KeySlab]). A lookup in the hash index compares the key of every chain whose tag and hash match: a key in
/// words of the chain is on a line the lookup can load as soon as it has the chain, where one in a slab is a load that
/// waits for the chain, then for the slab's view, then for its array. Every comparison of keys branches on how each
/// side holds its key, so chains of either kind and of the same key meet and compare as their bytes.
sealed class Chain permits Chain.Inline8 {
    /// Orders chains as their keys.
    static final Comparator<Chain> BY_KEY = Chain::compareKeys;
    /// The longest key a chain holds in words of its own.
    static final int INLINE = 4 * Long.BYTES;
    // The bits of keySpan that hold a key's length: as many as KeyCodec.MAX_LENGTH needs
    private static final int LENGTH_BITS = Integer.SIZE - Integer.numberOfLeadingZeros(KeyCodec.MAX_LENGTH);
    // The bits of keySpan above the length that hold the key's offset in its chunk, at most 4,096: where an empty key
    // lands in a full chunk
    private static final int OFFSET_BITS = 13;
    // The flags of keySpan, above the offset: three fields of their own would make every chain 8 bytes larger
    private static final int ORDERED = 1 << 24;
    private static final int TAKEN_OUT = 1 << 25;
    private static final int BLIND = 1 << 26;
    private static final int SETTLED = 1 << 27;
    private static final int IN_TREE = 1 << 28;
    private static final int LISTED = 1 << 29;
    /// What [#blindState()] returns for a chain whose key no commit wrote blind.
    static final byte NOT_BLIND = -1;

    /// What [#claimed] holds once the chain is dropped; above every sequence, so the claims stay monotonic.
    static final long DROPPED = Long.MAX_VALUE;
    /// What [#claim(PendingCommit, long)] returns for a dropped chain.
    static final Object DROPPED_WRITER = new Object();

    // The read-only view of the chunk of a KeySlab, or of an array of its own, that holds the key, or null for a key in
    // the words of the chain: a segment of the key's own would be 40 of the bytes a buffered write takes on the heap
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
    /// role. An `int`, which a key space's cuts do not outgrow in a lifetime of checkpoints, keeps a chain whose key
    /// is in a slab at 48 bytes rather than 56, and one whose key is in words of its own at 56 to 80.
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
        this.keyChunk = Objects.requireNonNull(keyChunk, "keyChunk");
        this.keySpan = (int) offset << LENGTH_BITS | length;
        this.hash = hash;
        this.dirtyCut = -1;
    }

    // Makes the chain of a key of `length` bytes, at most INLINE, that its subclass holds in words of its own
    private Chain(int length, int hash) {
        if (length < 0 || length > INLINE) {
            throw new IllegalArgumentException("A chain holds keys of at most " + INLINE + " bytes in its words: " + length);
        }
        this.keyChunk = null;
        this.keySpan = length;
        this.hash = hash;
        this.dirtyCut = -1;
    }

    /// {@return the chain of a copy of `key`, at most [#INLINE] bytes, in words of its own, whose hash is `hash`}
    ///
    /// The copy is made before it returns, so the caller may reuse `key`, which may be a segment of any kind and
    /// alignment.
    static Chain inline(MemorySegment key, int hash) {
        var length = Math.toIntExact(key.byteSize());
        var k0 = Segments.word(key, 0, length);
        if (length <= Long.BYTES) {
            return new Inline8(length, hash, k0);
        }
        var k1 = Segments.word(key, Long.BYTES, length);
        if (length <= 2 * Long.BYTES) {
            return new Inline16(length, hash, k0, k1);
        }
        var k2 = Segments.word(key, 2 * Long.BYTES, length);
        if (length <= 3 * Long.BYTES) {
            return new Inline24(length, hash, k0, k1, k2);
        }
        return new Inline32(length, hash, k0, k1, k2, Segments.word(key, 3 * Long.BYTES, length));
    }

    /// {@return the key, a read-only segment made for the call}
    ///
    /// For a key in words of the chain, it copies the key into an array of its own: a caller that only compares the
    /// key, or copies it, asks the chain ([#compareKey(MemorySegment)], [#copyKey(byte\[\], int)]).
    MemorySegment key() {
        if (keyChunk != null) {
            return keyChunk.asSlice(keyOffset(), keyLength());
        }
        var bytes = new byte[keyLength()];
        copyKey(bytes, 0);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    /// {@return whether the chain holds its key in words of its own, rather than in a segment}
    boolean inlineKey() {
        return keyChunk == null;
    }

    /// Copies the bytes of the key into `target` at `at`.
    ///
    /// @throws IndexOutOfBoundsException if `target` holds fewer than [#keyLength()] bytes from `at`
    void copyKey(byte[] target, int at) {
        var length = keyLength();
        Objects.checkFromIndexSize(at, length, target.length);
        if (keyChunk != null) {
            MemorySegment.copy(keyChunk, ValueLayout.JAVA_BYTE, keyOffset(), target, at, length);
            return;
        }
        var i = 0;
        for (; i + Long.BYTES <= length; i += Long.BYTES) {
            Bytes.i64(target, at + i, inlineWord(i >>> 3));
        }
        if (i < length) {
            var word = inlineWord(i >>> 3);
            for (var shift = Long.SIZE - Byte.SIZE; i < length; i++, shift -= Byte.SIZE) {
                target[at + i] = (byte) (word >>> shift);
            }
        }
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

    /// {@return whether a read of a range finds the chain: the ordered map holds it, or the list of the chains to order
    /// holds it, or did until a thread ordered it}
    ///
    /// A commit attaches no version to a chain before this holds, so every committed version a scan may see is in the
    /// map it walks, once it ordered the listed chains.
    boolean reachable() {
        return ((int) SPAN.getVolatile(this) & (ORDERED | LISTED)) != 0;
    }

    /// Records that the list of the chains to order holds the chain, once it does, with a volatile update.
    void markListed() {
        SPAN.getAndBitwiseOr(this, LISTED);
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
        return keyChunk == null ? inlineEquals(key) : Segments.equal(keyChunk, keyOffset(), keyLength(), key, 0, key.byteSize());
    }

    // As keyEquals for a key in words of the chain: kept apart, so the test every lookup of the hash index makes stays
    // small enough to inline into it
    private boolean inlineEquals(MemorySegment key) {
        var length = keyLength();
        if (key.byteSize() != length) {
            return false;
        }
        for (var at = 0; at < length; at += Long.BYTES) {
            if (inlineWord(at >>> 3) != Segments.word(key, at, length)) {
                return false;
            }
        }
        return true;
    }

    /// {@return whether the key's bytes are those of `key`}
    boolean keyEquals(byte[] key) {
        if (keyChunk != null) {
            return Segments.equal(keyChunk, keyOffset(), keyLength(), MemorySegment.ofArray(key), 0, key.length);
        }
        var length = keyLength();
        if (key.length != length) {
            return false;
        }
        for (var at = 0; at < length; at += Long.BYTES) {
            if (inlineWord(at >>> 3) != Segments.word(key, at, length)) {
                return false;
            }
        }
        return true;
    }

    /// {@return whether the keys of `left` and `right` are equal}
    static boolean sameKey(Chain left, Chain right) {
        if (left.keyChunk != null && right.keyChunk != null) {
            return Segments.equal(left.keyChunk, left.keyOffset(), left.keyLength(), right.keyChunk, right.keyOffset(), right.keyLength());
        }
        // One key is at most INLINE bytes, so equal keys take a few words
        var length = left.keyLength();
        if (right.keyLength() != length) {
            return false;
        }
        for (var at = 0; at < length; at += Long.BYTES) {
            if (left.word(at) != right.word(at)) {
                return false;
            }
        }
        return true;
    }

    /// {@return the order of the keys of `left` and `right`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    static int compareKeys(Chain left, Chain right) {
        if (left.keyChunk != null && right.keyChunk != null) {
            return Segments.compare(left.keyChunk, left.keyOffset(), left.keyLength(), right.keyChunk, right.keyOffset(), right.keyLength());
        }
        // Words zero past a key's end order as its bytes up to there, and keys whose common words are equal order as
        // their lengths, the shorter first; one key is at most INLINE bytes, so the common bytes take a few words
        var leftLength = left.keyLength();
        var rightLength = right.keyLength();
        var common = Math.min(leftLength, rightLength);
        for (var at = 0; at < common; at += Long.BYTES) {
            var leftWord = left.word(at);
            var rightWord = right.word(at);
            if (leftWord != rightWord) {
                return Long.compareUnsigned(leftWord, rightWord);
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    /// {@return the order of the key and `other`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareKey(MemorySegment other) {
        return keyChunk == null ? compareInline(other) : Segments.compare(keyChunk, keyOffset(), keyLength(), other, 0, other.byteSize());
    }

    // As compareKey for a key in words of the chain, in the order compareKeys gives
    private int compareInline(MemorySegment other) {
        var length = keyLength();
        var otherLength = other.byteSize();
        var common = Math.min(length, otherLength);
        for (var at = 0; at < common; at += Long.BYTES) {
            var word = inlineWord(at >>> 3);
            var otherWord = Segments.word(other, at, otherLength);
            if (word != otherWord) {
                return Long.compareUnsigned(word, otherWord);
            }
        }
        return Long.compare(length, otherLength);
    }

    /// {@return the 8 bytes of the key at `at`, big-endian, zero past its end}
    long word(long at) {
        var length = keyLength();
        if (keyChunk == null) {
            if (at >= length) {
                return 0;
            }
            var index = (int) (at >>> 3);
            var shift = (int) (at & (Long.BYTES - 1)) * Byte.SIZE;
            var word = inlineWord(index);
            if (shift == 0) {
                return word;
            }
            // The next word, read only if the key reaches it, gives the bytes past this one's end
            var next = (index + 1) * Long.BYTES < length ? inlineWord(index + 1) : 0;
            return word << shift | next >>> (Long.SIZE - shift);
        }
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

    // Word `index` of a key in words of the chain, which the key reaches: the length picked the subclass, which has it
    private long inlineWord(int index) {
        return switch (index) {
            case 0 -> ((Inline8) this).k0;
            case 1 -> ((Inline16) this).k1;
            case 2 -> ((Inline24) this).k2;
            default -> ((Inline32) this).k3;
        };
    }

    /// A chain of a key of at most 8 bytes, in a word of its own: 56 bytes.
    static sealed class Inline8 extends Chain permits Inline16 {
        // The key's bytes 0 to 7, big-endian, zero past its end
        private final long k0;

        private Inline8(int length, int hash, long k0) {
            super(length, hash);
            this.k0 = k0;
        }
    }

    /// A chain of a key of 9 to 16 bytes, in words of its own: 64 bytes.
    static sealed class Inline16 extends Inline8 permits Inline24 {
        // The key's bytes 8 to 15, big-endian, zero past its end
        private final long k1;

        private Inline16(int length, int hash, long k0, long k1) {
            super(length, hash, k0);
            this.k1 = k1;
        }
    }

    /// A chain of a key of 17 to 24 bytes, in words of its own: 72 bytes.
    static sealed class Inline24 extends Inline16 permits Inline32 {
        // The key's bytes 16 to 23, big-endian, zero past its end
        private final long k2;

        private Inline24(int length, int hash, long k0, long k1, long k2) {
            super(length, hash, k0, k1);
            this.k2 = k2;
        }
    }

    /// A chain of a key of 25 to 32 bytes, in words of its own: 80 bytes.
    static final class Inline32 extends Inline24 {
        // The key's bytes 24 to 31, big-endian, zero past its end
        private final long k3;

        private Inline32(int length, int hash, long k0, long k1, long k2, long k3) {
            super(length, hash, k0, k1, k2);
            this.k3 = k3;
        }
    }
}

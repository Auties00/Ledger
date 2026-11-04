package com.github.auties00.ledger.engine;

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
    /// Orders chains as their keys: by [#prefix] first, which decides most comparisons without reading the keys.
    static final Comparator<Chain> BY_KEY = (left, right) -> {
        var order = Long.compareUnsigned(left.prefix, right.prefix);
        return order != 0 ? order : Segments.compare(left.key, right.key);
    };

    /// What [#claimed] holds once the chain is dropped; above every sequence, so the claims stay monotonic.
    static final long DROPPED = Long.MAX_VALUE;
    /// What [#claim(PendingCommit, long)] returns for a dropped chain.
    static final Object DROPPED_WRITER = new Object();

    /// The key, in an array of its own.
    final MemorySegment key;
    /// The hash of the key's bytes, as the hash index uses it.
    final int hash;
    /// The first 8 bytes of the key, big-endian, zero after a shorter key's last.
    final long prefix;
    /// The newest version, or `null` before the writer attaches the first one and once it drops the chain.
    volatile Version head;
    /// Whether the ordered map holds the chain: the writer attaches no version before it does, so every committed
    /// version a scan may see is in the map the scan walks.
    volatile boolean ordered;
    /// Whether the writer took the chain out of the maps; no version is attached to it again.
    volatile boolean dropped;
    /// The sequence of the last write claimed, 0 before any, or [#DROPPED]; only grows.
    @SuppressWarnings("unused")
    private volatile long claimed;
    /// The last commit of the pipeline given a write of the key, until its committing thread forgets it once it is
    /// published, or `null`; set by the sequencer only, so it needs no compare-and-set.
    @SuppressWarnings("unused")
    private volatile PendingCommit lastWriter;
    /// The cut whose dirty keys list the chain, so a chain is listed once per checkpoint; guarded by the publishing
    /// role.
    long dirtyCut;

    private static final VarHandle CLAIMED;
    private static final VarHandle LAST_WRITER;
    private static final VarHandle HEAD;

    static {
        try {
            var lookup = MethodHandles.lookup();
            CLAIMED = lookup.findVarHandle(Chain.class, "claimed", long.class);
            LAST_WRITER = lookup.findVarHandle(Chain.class, "lastWriter", PendingCommit.class);
            HEAD = lookup.findVarHandle(Chain.class, "head", Version.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

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

    Chain(MemorySegment key, int hash) {
        this.key = key;
        this.hash = hash;
        this.prefix = prefix(key);
        this.dirtyCut = -1;
    }

    private static long prefix(MemorySegment key) {
        var prefix = 0L;
        var length = (int) Math.min(Long.BYTES, key.byteSize());
        for (var i = 0; i < length; i++) {
            prefix |= (key.get(ValueLayout.JAVA_BYTE, i) & 0xFFL) << (56 - 8 * i);
        }
        return prefix;
    }
}

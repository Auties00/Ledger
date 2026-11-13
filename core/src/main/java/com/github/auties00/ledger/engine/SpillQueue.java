package com.github.auties00.ledger.engine;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.LongSupplier;
import java.util.zip.CRC32C;

/// The paged values waiting for a checkpoint that may leave the heap for the log, which holds them durably too.
///
/// A value a batch put to a paged key space stays on its batch's array until a checkpoint writes it, and the array
/// with it: up to six intervals of log, hundreds of megabytes after a history sync. Each commit with such values
/// queues a [Record] of them once it is published, which the log writer locates in the frame it seals. Once the queue
/// holds more than its budget of values whose frames are durable, a virtual thread moves the values of the oldest
/// records off their arrays, until three quarters of the budget is left: a value of at least [#LOG_VALUE_MIN] bytes to
/// its place in the log ([LogValue]), with a tag that authenticates it, a shorter one to an array of its own. A
/// batch's array no version needs any more is then garbage. The newest values, those of the budget, stay where they
/// are, so reads of recent writes do not change; a read of a value in the log reads it back with one positioned read.
///
/// A record leaves the queue once its values moved, or once a published checkpoint holds its commit. Values are not
/// moved:
/// - of a key space until samples of its commits show that it replaces or removes few of its values before a
///   checkpoint ([ValueSlab.Overwrites#spills()]): a record lists versions, which keep their batch's arrays alive until
///   the record leaves the queue, and moving a value no reader can reach any more costs a tag of its bytes for
///   nothing. A version listed and then replaced counts in the held bytes until its record leaves, a share of them
///   the samples keep under an eighth;
/// - of a group the log wrote through to stable storage, whose pages that write took out of the system's cache, so a
///   read back would go to the device;
/// - of a batch on a [ValueSlab]'s chunks, which the slab bounds already, of a transaction, or of a value with an
///   array of its own or a unit of [BlobFile].
///
/// A segment holding such values stays in place past the checkpoint that makes it obsolete while a reader that may
/// still need them reads a tree older than that checkpoint ([#retirableBefore(long, ReaderTable)]): log disk use then
/// grows where the heap grew before. Once it retires, a reader that finds such a value reads it from its own tree.
///
/// Nothing of this is durable: a recovery replays the commits as ever, and moves the values it replays the same way
/// ([LiveLedger#replay(long, byte\[\], LogSegmentView, long, int)]).
final class SpillQueue {
    /// The shortest value that moves to the log: a [LogValue] takes 40 bytes, an array of the value 16 more than the
    /// value; below it, the array costs less than a read back from the file.
    static final int LOG_VALUE_MIN = 256;
    /// The bytes of an AES block.
    static final int BLOCK = 16;
    /// The MAC of the tags, HMAC-SHA256, of which a tag keeps the first 96 bits.
    private static final String TAG_ALGORITHM = "HmacSHA256";
    /// The bytes of an HMAC-SHA256 digest, and of the key of the tags.
    private static final int DIGEST_BYTES = 32;
    /// The scratch past which a [Crypto] given back lets go of it: the values of messages take a few kilobytes.
    private static final int KEPT_SCRATCH = 16 << 10;
    private static final SecureRandom KEYS = new SecureRandom();

    private final long budget;
    // The held bytes the thread moves values down to
    private final long lowWater;
    private final boolean encrypted;
    // The key of the tags, drawn for this open at the first value moved or read back and never stored; null before,
    // and without encryption
    private volatile SecretKeySpec tagKey;
    private final Object lock;
    // The records not taken, oldest first; guarded by the lock
    private final ArrayDeque<Record> records;
    // The segments values moved to, by number, which no checkpoint has retired; guarded by the lock
    private final ArrayList<LogSegmentView> segments;
    // The bytes of values the records of the queue list; written holding the lock
    private volatile long held;
    // Whether the thread that moves values runs
    private final AtomicBoolean running;
    private volatile boolean closed;
    // The latest sequence the log holds durably; before the store has a log, the commits queued are those a recovery
    // replays from it, every one durable
    private volatile LongSupplier durable = () -> Long.MAX_VALUE;
    private final AtomicReferenceArray<Crypto> cryptos;
    // The values moved to the log and to arrays of their own, for tests
    private final AtomicLong loggedValues = new AtomicLong();
    private final AtomicLong copiedValues = new AtomicLong();

    /// Creates the queue of a store.
    ///
    /// @param budget the bytes of values whose frames are durable the queue holds before it moves them
    /// @param encrypted whether the store's log is encrypted, so the values read back are authenticated by an HMAC
    SpillQueue(long budget, boolean encrypted) {
        this.budget = budget;
        this.lowWater = budget - budget / 4;
        this.encrypted = encrypted;
        this.lock = new Object();
        this.records = new ArrayDeque<>();
        this.segments = new ArrayList<>();
        this.running = new AtomicBoolean();
        this.cryptos = new AtomicReferenceArray<>(Math.max(4, 2 * Runtime.getRuntime().availableProcessors()));
    }

    /// {@return the key of the tags, drawn at the first call}
    private SecretKeySpec tagKey() {
        var key = tagKey;
        if (key == null) {
            synchronized (lock) {
                key = tagKey;
                if (key == null) {
                    var bytes = new byte[DIGEST_BYTES];
                    KEYS.nextBytes(bytes);
                    key = new SecretKeySpec(bytes, TAG_ALGORITHM);
                    tagKey = key;
                }
            }
        }
        return key;
    }

    /// {@return the budget for a heap of `maxMemory` bytes: a 256th, from 4 MiB to 64 MiB}
    static long defaultBudget(long maxMemory) {
        return Math.clamp(maxMemory / 256, 4L << 20, 64L << 20);
    }

    /// Reads the latest durable sequence from `log` from now on, and moves values whenever the queue is over budget.
    void useLog(LogWriter log) {
        this.durable = log::durableSequence;
        if (over()) {
            start();
        }
    }

    /// {@return the view of the use of segment `number` in `path` whose key is `key`, `null` without encryption}
    LogSegmentView segment(long number, Path path, SecretKeySpec key) {
        return new LogSegmentView(number, path, key, this);
    }

    /// {@return whether the queue holds more than its budget}
    boolean over() {
        return held > budget;
    }

    /// {@return an empty record of the commit at `sequence`, with room for `writes` values}
    Record record(long sequence, int writes) {
        return new Record(sequence, Math.max(1, writes));
    }

    /// Queues `record`, of a published commit after every one queued, and starts moving values if the queue is over
    /// its budget.
    void add(Record record) {
        if (record.count == 0) {
            return;
        }
        synchronized (lock) {
            if (closed) {
                return;
            }
            records.addLast(record);
            held = held + record.bytes;
        }
        if (over()) {
            start();
        }
    }

    /// Starts moving values if the queue is over its budget; called by the log as a group becomes durable, which may
    /// make the oldest record's values movable.
    void durableAdvanced() {
        if (over() && !running.get()) {
            start();
        }
    }

    /// Starts the thread that moves values, unless it runs; a thread the system refuses leaves the values where they
    /// are, until the next start.
    private void start() {
        if (closed || !running.compareAndSet(false, true)) {
            return;
        }
        try {
            Thread.ofVirtual().name("ledger-spill").start(this::run);
        } catch (Throwable _) {
            running.set(false);
        }
    }

    private void run() {
        try {
            drain(durable);
        } finally {
            running.set(false);
        }
        // A group that became durable as the thread stopped found it running
        if (movable()) {
            start();
        }
    }

    /// {@return whether the queue is over its budget and its oldest record's frame is durable}
    private boolean movable() {
        var log = durable;
        if (!over()) {
            return false;
        }
        synchronized (lock) {
            var first = records.peekFirst();
            return first != null && first.sequence <= log.getAsLong();
        }
    }

    /// Moves the values of the oldest records whose commits `durableSequence` gives durable, while the queue holds more
    /// than three quarters of its budget, on the caller's thread.
    void drain(LongSupplier durableSequence) {
        drain(durableSequence, lowWater);
    }

    /// Moves values on the caller's thread while the queue holds more than twice its budget, down to the budget: a
    /// recovery replays commits faster than the queue's thread alone moves their values, which it then helps.
    void keepUp() {
        if (held - budget > budget) {
            drain(durable, budget);
        }
    }

    /// Moves the values of the oldest records whose commits `durableSequence` gives durable, while the queue holds more
    /// than `downTo` bytes, on the caller's thread.
    private void drain(LongSupplier durableSequence, long downTo) {
        var crypto = takeCrypto();
        try {
            while (!closed) {
                Record record;
                synchronized (lock) {
                    record = records.peekFirst();
                    if (record == null || held <= downTo || record.sequence > durableSequence.getAsLong()) {
                        return;
                    }
                    records.pollFirst();
                    held = held - record.bytes;
                    if (record.logs()) {
                        // Before any value moves there, so a checkpoint that retires segments sees the commit
                        var segment = record.segment;
                        if (segment.note(record.sequence)) {
                            segments.add(segment);
                        }
                    }
                }
                record.spill(this, crypto);
            }
        } finally {
            giveBack(crypto);
        }
    }

    /// Drops the records of the commits up to `cut`, which a checkpoint just published holds, and records that it holds
    /// every commit of the segments before `logStart`, `latest` being the latest sequence as it was published; holding
    /// the store's publishing role.
    void published(long cut, long logStart, long latest) {
        synchronized (lock) {
            Record record;
            while ((record = records.peekFirst()) != null && record.sequence <= cut) {
                records.pollFirst();
                held = held - record.bytes;
                record.release();
            }
            for (var segment : segments) {
                if (segment.number() >= logStart) {
                    break;
                }
                segment.cover(latest);
            }
        }
    }

    /// Retires, from the oldest, the segments before `first` that values moved to and that no reader may need any
    /// more, and returns the first segment the log keeps: `first`, or the oldest that a reader may still need.
    ///
    /// A reader may need a segment only if it may read a tree that lacks a commit whose values moved there. Each such
    /// segment holds commits that a published checkpoint holds, its tree published when the latest sequence was
    /// [LogSegmentView#coveredAt()]: a reader registered later reads that tree or a newer one, and a reader at a
    /// sequence before the segment's oldest commit sees none of its versions. So a segment is needed while a reader is
    /// registered between the two, which at the latest sequence of the publication itself may read either tree.
    /// A reader at such a sequence registers before it takes its tree, so one that took the tree before the publication
    /// is in the table when this looks.
    ///
    /// @return the first segment the log keeps
    long retirableBefore(long first, ReaderTable readers) {
        synchronized (lock) {
            var iterator = segments.iterator();
            while (iterator.hasNext()) {
                var segment = iterator.next();
                if (segment.number() >= first) {
                    break;
                }
                if (segment.coveredAt() < 0 || readers.readsWithin(segment.lowest(), segment.coveredAt())) {
                    return segment.number();
                }
                segment.retire();
                iterator.remove();
            }
            return first;
        }
    }

    /// {@return whether a segment values moved to is held past a checkpoint that makes it obsolete}
    boolean holdsBack() {
        synchronized (lock) {
            return !segments.isEmpty() && segments.getFirst().coveredAt() >= 0;
        }
    }

    /// Retires every segment values moved to, as the store closes and no reader reads any more.
    void retireAll() {
        synchronized (lock) {
            for (var segment : segments) {
                segment.retire();
            }
            segments.clear();
        }
    }

    /// Stops moving values and retires every segment, as the store closes.
    void close() {
        closed = true;
        synchronized (lock) {
            for (var record : records) {
                record.release();
            }
            records.clear();
            held = 0;
        }
        retireAll();
    }

    /// {@return the bytes of values the queued records list; for tests}
    long heldForTests() {
        return held;
    }

    /// {@return the values moved to the log so far; for tests}
    long loggedValuesForTests() {
        return loggedValues.get();
    }

    /// {@return the values moved to arrays of their own so far; for tests}
    long copiedValuesForTests() {
        return copiedValues.get();
    }

    /// {@return the segments values moved to that no checkpoint retired, by number; for tests}
    long[] segmentsForTests() {
        synchronized (lock) {
            return segments.stream().mapToLong(LogSegmentView::number).toArray();
        }
    }

    /// Waits until the thread that moves values is not running; for tests.
    void awaitIdleForTests() throws InterruptedException {
        while (running.get()) {
            Thread.sleep(1);
        }
    }

    /// {@return a [Crypto] for the caller alone until [#giveBack(Crypto)]}
    Crypto takeCrypto() {
        for (var i = 0; i < cryptos.length(); i++) {
            var crypto = cryptos.getPlain(i);
            if (crypto != null && cryptos.compareAndSet(i, crypto, null)) {
                return crypto;
            }
        }
        return new Crypto(this);
    }

    /// Gives `crypto` back for another caller, or to the collector if every place is taken.
    void giveBack(Crypto crypto) {
        crypto.trim();
        for (var i = 0; i < cryptos.length(); i++) {
            if (cryptos.getPlain(i) == null && cryptos.compareAndSet(i, null, crypto)) {
                return;
            }
        }
    }

    /// The values of one commit that may move, where its frame lies in the log, and whether they can.
    ///
    /// It lists versions, not their chains: a version a later write replaced moves too, which costs its tag and
    /// spares a walk of the chain per value. The log writer locates it while it seals the frame, before the frame is
    /// durable, which publishes the location to whatever reads the durable sequence after.
    static final class Record {
        private final long sequence;
        private Version[] versions;
        // Where each value starts in the body of its commit
        private int[] offsets;
        private int count;
        // The bytes of the values, and whether one is at least LOG_VALUE_MIN
        private long bytes;
        private boolean large;
        // Set by the log writer as it seals the frame: the segment use, the frame's offset, where the commit's body
        // starts in the frame's body, and whether the group was written through
        private LogSegmentView segment;
        private long frameOffset;
        private int commitStart;
        private boolean through;

        private Record(long sequence, int capacity) {
            this.sequence = sequence;
            this.versions = new Version[capacity];
            this.offsets = new int[capacity];
        }

        /// Lists `version`, whose value of `length` bytes starts at `offset` of the commit's body.
        void add(Version version, int offset, int length) {
            if (count == versions.length) {
                versions = Arrays.copyOf(versions, count * 2);
                offsets = Arrays.copyOf(offsets, count * 2);
            }
            versions[count] = version;
            offsets[count++] = offset;
            bytes += length;
            large |= length >= LOG_VALUE_MIN;
        }

        /// Records where the log wrote the frame of the commit: at `frameOffset` of the use `segment`, the commit's body
        /// at `commitStart` of the frame's body, written through to stable storage if `through`.
        ///
        /// @param segment the use, or `null` if the store moves no value
        void locate(LogSegmentView segment, long frameOffset, int commitStart, boolean through) {
            this.segment = segment;
            this.frameOffset = frameOffset;
            this.commitStart = commitStart;
            this.through = through;
        }

        /// {@return the sequence of the commit}
        long sequence() {
            return sequence;
        }

        /// {@return whether values move to the log: some are long enough, and its frame is in a segment's cache}
        private boolean logs() {
            return large && segment != null && !through;
        }

        /// Moves the values, unless a long one cannot move to the log, then forgets them.
        private void spill(SpillQueue queue, Crypto crypto) {
            if (!large || logs()) {
                var logged = 0;
                var copied = 0;
                for (var i = 0; i < count; i++) {
                    var version = versions[i];
                    if (version.spill(crypto, segment, frameOffset, commitStart + offsets[i])) {
                        if (version.length() >= LOG_VALUE_MIN) {
                            logged++;
                        } else {
                            copied++;
                        }
                    }
                }
                queue.loggedValues.addAndGet(logged);
                queue.copiedValues.addAndGet(copied);
            }
            release();
        }

        /// Forgets the values.
        private void release() {
            versions = null;
            offsets = null;
            count = 0;
        }
    }

    /// The ciphers of one thread at a time: those that authenticate a value moved to the log and decrypt it back.
    ///
    /// Neither allocates per value but the 32-byte digest the JDK's HMAC returns: the keystream of a value is AES-ECB of
    /// its counter blocks under a cipher keyed once per segment use, where a cipher in CTR mode takes a new IV and with
    /// it a new initialization per value, and the tag is an HMAC under a key set once, where a GMAC takes a nonce and an
    /// initialization per value and copies its input twice, as the JDK's GCM buffers what it authenticates.
    static final class Crypto {
        // AES-ECB under the key of the segment use it last decrypted from, which makes keystream blocks, and
        // HMAC-SHA256 under the queue's key, which makes the tags; null without encryption
        private final Cipher blocks;
        private final Mac mac;
        // The key blocks is under, or null before the first decryption
        private SecretKeySpec blocksKey;
        private final CRC32C crc;
        // The stamp a tag authenticates with the value, then the digest of the tag
        private final byte[] stamp;
        private final byte[] digest;
        private byte[] scratch;
        // The counter blocks of a value and the keystream they encrypt to
        private byte[] counters;
        private byte[] keystream;

        private Crypto(SpillQueue queue) {
            if (queue.encrypted) {
                try {
                    this.blocks = Cipher.getInstance("AES/ECB/NoPadding");
                    this.mac = Mac.getInstance(TAG_ALGORITHM);
                    mac.init(queue.tagKey());
                } catch (GeneralSecurityException exception) {
                    throw new IllegalStateException("AES or HMAC-SHA256 is unavailable", exception);
                }
            } else {
                this.blocks = null;
                this.mac = null;
            }
            this.crc = new CRC32C();
            this.stamp = new byte[Long.BYTES];
            this.digest = new byte[DIGEST_BYTES];
        }

        /// {@return an array of at least `length` bytes, which only this holder uses until it is given back}
        byte[] scratch(int length) {
            var array = scratch;
            if (array == null || array.length < length) {
                array = new byte[Math.max(length, 2 << 10)];
                scratch = array;
            }
            return array;
        }

        private void trim() {
            if (scratch != null && scratch.length > KEPT_SCRATCH) {
                scratch = null;
            }
            if (counters != null && counters.length > KEPT_SCRATCH) {
                counters = null;
                keystream = null;
            }
        }

        /// {@return the place in the log of the `length` bytes at `offset` of `home`, the value of the version stamped
        /// `stamp`, which lies at `bodyIndex` of the body of the frame at `frameOffset` of `segment`}
        LogValue logValue(LogSegmentView segment, long frameOffset, int bodyIndex, long stamp, MemorySegment home,
                          int offset, int length) {
            var value = scratch(length);
            MemorySegment.copy(home, offset, MemorySegment.ofArray(value), 0, length);
            if (mac == null) {
                crc.reset();
                crc.update(value, 0, length);
                return new LogValue(segment, frameOffset, bodyIndex, 0, (int) crc.getValue());
            }
            tag(value, length, stamp);
            return new LogValue(segment, frameOffset, bodyIndex, Bytes.i64(digest, 0), Bytes.i32(digest, Long.BYTES));
        }

        /// {@return the `length` bytes after the first `skip` of `sealed`, decrypted, the bytes from the block holding
        /// the value's first byte at `bodyIndex` of the body of the frame at `frameOffset`, sealed under `key`}
        ///
        /// A body is sealed from the counter block after the first, `nonce || 2`, so the block of a byte at `i` of it
        /// is `nonce || 2 + i / 16`; a body holds far fewer than 2^36 bytes, so the counter never carries into the
        /// nonce.
        byte[] decrypt(SecretKeySpec key, long frameOffset, int bodyIndex, byte[] sealed, int skip, int length) {
            var bytes = (skip + length + BLOCK - 1) & -BLOCK;
            if (counters == null || counters.length < bytes) {
                counters = new byte[Math.max(bytes, 2 << 10)];
                keystream = new byte[counters.length];
            }
            var first = 2 + (bodyIndex >>> 4);
            for (var block = 0; block < bytes; block += BLOCK) {
                Bytes.i32(counters, block, 0);
                Bytes.i64(counters, block + Integer.BYTES, frameOffset);
                Bytes.i32(counters, block + Integer.BYTES + Long.BYTES, first + (block >>> 4));
            }
            try {
                if (key != blocksKey) {
                    blocks.init(Cipher.ENCRYPT_MODE, key);
                    blocksKey = key;
                }
                blocks.update(counters, 0, bytes, keystream, 0);
            } catch (GeneralSecurityException exception) {
                blocksKey = null;
                throw new IllegalStateException("AES failed", exception);
            }
            var value = new byte[length];
            for (var i = 0; i < length; i++) {
                value[i] = (byte) (sealed[skip + i] ^ keystream[skip + i]);
            }
            return value;
        }

        /// {@return whether `value` is the value `logged` holds the tag of, of the version stamped `stamp`}
        boolean verifies(byte[] value, long stamp, LogValue logged) {
            if (mac == null) {
                crc.reset();
                crc.update(value, 0, value.length);
                return (int) crc.getValue() == logged.tagLow();
            }
            tag(value, value.length, stamp);
            // Every bit compared whatever the first that differs
            return ((Bytes.i64(digest, 0) ^ logged.tagHigh()) | (Bytes.i32(digest, Long.BYTES) ^ logged.tagLow())) == 0;
        }

        /// Writes to the digest the HMAC of `stamp` and the first `length` bytes of `value`, of which a tag keeps the
        /// first 96 bits.
        private void tag(byte[] value, int length, long stamp) {
            Bytes.i64(this.stamp, 0, stamp);
            try {
                mac.update(this.stamp);
                mac.update(value, 0, length);
                mac.doFinal(digest, 0);
            } catch (GeneralSecurityException exception) {
                mac.reset();
                throw new IllegalStateException("HMAC-SHA256 failed", exception);
            }
        }
    }
}

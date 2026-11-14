package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.util.concurrent.atomic.AtomicReference;

/// One use of a log segment, the writing of one open or the frames one recovery replayed, as the values that left the
/// heap for it are read back from ([LogValue]).
///
/// The segment stays in place while a reader may need such a value from it. A checkpoint retires it through
/// [#retire()] before it renames or deletes the file ([SpillQueue#retirableBefore(long, ReaderTable)]); from then on a
/// read of a value here throws [Retired], and its reader reads the value from its tree, which holds it. A read that
/// the retirement overtakes, as the file closes, is renamed or is prepared again under it, throws [Retired] too, never
/// a failure of the store.
///
/// The file is read through one channel, opened at the first read. An interrupt of a reading thread closes the
/// channel for every thread; the read then opens it again, reads again, and gives the thread its interrupt back.
///
/// The keystream of a value comes from an AES-ECB cipher initialized under the key of the use once, at the first read,
/// and kept for the next: a checkpoint reads values back in key order, which interleaves the uses they moved to, and an
/// initialization per value allocated the key schedule of each.
final class LogSegmentView {
    private final long number;
    private final Path path;
    // The key of the use, or null for a store without encryption
    private final SecretKeySpec key;
    private final SpillQueue spills;
    private final Object lock;
    private volatile FileChannel channel;
    // The cipher that makes the keystream under key, or null before the first read, while a read holds it, and without
    // encryption; a read that finds it taken makes one of its own, which it leaves here if the place is empty
    private final AtomicReference<Cipher> blocks;
    private volatile boolean retired;
    // The lowest and highest sequence of the commits whose values were moved here, written holding the lock of the
    // queue; Long.MAX_VALUE and -1 before any
    private long lowest;
    private long highest;
    // The latest sequence when the checkpoint that holds every commit here was published, or -1 before; guarded by
    // the lock of the queue
    private long coveredAt;

    LogSegmentView(long number, Path path, SecretKeySpec key, SpillQueue spills) {
        this.number = number;
        this.path = path;
        this.key = key;
        this.spills = spills;
        this.lock = new Object();
        this.blocks = new AtomicReference<>();
        this.lowest = Long.MAX_VALUE;
        this.highest = -1;
        this.coveredAt = -1;
    }

    /// {@return the number of the segment}
    long number() {
        return number;
    }

    /// {@return the file of the segment}
    Path path() {
        return path;
    }

    /// {@return the key of the use, or `null` for a store without encryption}
    SecretKeySpec key() {
        return key;
    }

    /// {@return whether a checkpoint retired the segment}
    boolean retired() {
        return retired;
    }

    /// Records that the values of the commit at `sequence` move here; holding the lock of the queue.
    ///
    /// @return whether this is the first commit to
    boolean note(long sequence) {
        var first = highest < 0;
        lowest = Math.min(lowest, sequence);
        highest = Math.max(highest, sequence);
        return first;
    }

    /// {@return the lowest sequence of the commits whose values moved here}
    long lowest() {
        return lowest;
    }

    /// {@return the latest sequence when the checkpoint holding every commit here was published, or -1 before}
    long coveredAt() {
        return coveredAt;
    }

    /// Records that a published checkpoint holds every commit here, the latest sequence being `latest`, unless an
    /// earlier one did; holding the lock of the queue.
    void cover(long latest) {
        if (coveredAt < 0) {
            coveredAt = latest;
        }
    }

    /// Retires the segment: closes its channel, and every later read throws [Retired].
    void retire() {
        synchronized (lock) {
            retired = true;
            blocks.set(null);
            var open = channel;
            channel = null;
            if (open != null) {
                try {
                    open.close();
                } catch (IOException _) {
                    // Opened only to read
                }
            }
        }
    }

    /// Reads the value `logged` places here, `length` bytes of the version stamped `stamp`, into `target` from
    /// `offset`.
    ///
    /// @throws Retired if a checkpoint retired the segment
    /// @throws LedgerIoException if the segment cannot be read
    /// @throws LedgerCorruptedException if the bytes read are not the value
    void read(LogValue logged, long stamp, int length, byte[] target, int offset) {
        if (retired) {
            throw Retired.INSTANCE;
        }
        var crypto = spills.takeCrypto();
        try {
            var start = logged.frameOffset() + LogWriter.FRAME_HEADER + logged.bodyIndex();
            if (key == null) {
                // Through the holder's buffer, which a read wraps once, rather than a buffer of the target per read
                var read = crypto.scratch(length);
                readFully(crypto.scratchBuffer(), 0, length, start);
                System.arraycopy(read, 0, target, offset, length);
            } else {
                // From the first byte of the block holding the value's first, which the keystream starts from
                var skip = logged.bodyIndex() & (SpillQueue.BLOCK - 1);
                var sealed = crypto.scratch(skip + length);
                readFully(crypto.scratchBuffer(), 0, skip + length, start - skip);
                var cipher = takeBlocks();
                // Not given back if it failed, which leaves its state unknown
                crypto.decrypt(cipher, logged.frameOffset(), logged.bodyIndex(), sealed, skip, length, target, offset);
                if (!retired) {
                    blocks.compareAndSet(null, cipher);
                }
            }
            if (!crypto.verifies(target, offset, length, stamp, logged)) {
                if (retired) {
                    // Read as a preparation reused the file
                    throw Retired.INSTANCE;
                }
                throw new LedgerCorruptedException("A value of the log segment " + path
                        + " does not match the value the store wrote", null);
            }
        } finally {
            spills.giveBack(crypto);
        }
    }

    /// {@return the cipher that makes the keystream under the use's key, which the caller alone holds until it gives
    /// it back: the one kept here, or a new one if another read holds it}
    private Cipher takeBlocks() {
        var cipher = blocks.getAndSet(null);
        if (cipher != null) {
            return cipher;
        }
        cipher = SpillQueue.Crypto.newBlocks();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key);
        } catch (InvalidKeyException exception) {
            throw new IllegalStateException("AES failed", exception);
        }
        return cipher;
    }

    /// Reads `length` bytes at `position` of the file into `buffer` from `offset`, whatever the buffer's position and
    /// limit, opening the channel again after an interrupt closed it.
    private void readFully(ByteBuffer buffer, int offset, int length, long position) {
        var interrupted = false;
        try {
            while (true) {
                var open = channel();
                try {
                    buffer.limit(offset + length).position(offset);
                    while (buffer.hasRemaining()) {
                        if (open.read(buffer, position + buffer.position() - offset) < 0) {
                            if (retired) {
                                throw Retired.INSTANCE;
                            }
                            throw new LedgerIoException("The log segment " + path + " ends before a value it holds",
                                    new EOFException(path.toString()));
                        }
                    }
                    return;
                } catch (ClosedByInterruptException _) {
                    // This thread's interrupt closed the channel: read again without it, which it gets back after
                    Thread.interrupted();
                    interrupted = true;
                } catch (ClosedChannelException _) {
                    // Closed by another thread's interrupt, which opens it again, or by the retirement
                    if (retired) {
                        throw Retired.INSTANCE;
                    }
                } catch (IOException exception) {
                    if (retired) {
                        throw Retired.INSTANCE;
                    }
                    throw new LedgerIoException("A value could not be read from the log segment " + path, exception);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// {@return the channel of the file, opened now if none is open}
    ///
    /// @throws Retired if a checkpoint retired the segment
    private FileChannel channel() {
        var open = channel;
        if (open != null && open.isOpen()) {
            return open;
        }
        synchronized (lock) {
            if (retired) {
                throw Retired.INSTANCE;
            }
            open = channel;
            if (open == null || !open.isOpen()) {
                // Not interruptible: a thread interrupted since its last read opens the file as any other
                try {
                    open = FileChannel.open(path, StandardOpenOption.READ);
                } catch (IOException exception) {
                    throw new LedgerIoException("The log segment " + path + " could not be opened to read a value", exception);
                }
                channel = open;
            }
            return open;
        }
    }

    /// What a read of a value in a segment a checkpoint retired throws: the value is in the trees of every reader that
    /// can still find its version, and the reader reads it there.
    static final class Retired extends RuntimeException {
        static final Retired INSTANCE = new Retired();

        private Retired() {
            super("The log segment of the value is retired", null, false, false);
        }
    }
}

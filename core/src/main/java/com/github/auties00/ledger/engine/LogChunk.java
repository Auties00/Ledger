package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

/// Where the log holds the values of one array of a batch that left the heap as one ([SpillQueue]): values a commit put
/// back to back on the array its versions kept, too short to pay a [LogValue] each.
///
/// The values lie on the array in the order of their writes, from [#base()] to [#end()] with no byte between them, and
/// the section of the commit's split frame ([FrameBody#COMMIT_SPLIT]) holds them as the array does, from where the
/// first one starts in the frame's body: the chunk keeps that place, not one per value. The bytes are cut into blocks
/// of [#BLOCK] bytes from [#base()], the last one shorter, and each block has a tag of its own, made from the bytes on
/// the heap while the log wrote the frame or before the versions moved here: the polynomials of [SpillQueue.Crypto]
/// over the block, bound to the commit's sequence and to the block's place in the commit's body, or the CRC-32C of the
/// block in a store without encryption. A read of a value reads the blocks it spans, decrypts them with AES-CTR as a
/// [LogValue]'s are, and checks every tag before it copies a byte out, so a value of a few hundred bytes takes one
/// positioned read of the one or two kilobytes of the blocks it spans.
///
/// A version moved here keeps its offset on the array, which places its value among the chunk's: a version holds no
/// more than the reference.
///
/// A checkpoint, which writes every value of the chunk into its trees, reads the whole chunk once
/// ([#readMaterialized(int, int, byte\[\], int)]): one sequential read of exactly its bytes, every block checked, kept
/// until the checkpoint ends ([SpillQueue#releaseMaterialized()]) so its other values are copied from the heap. So the
/// next checkpoint after a commit of a hundred thousand short values reads them back in about one read per chunk of the
/// batch rather than one per value.
///
/// It takes about 12 bytes per block: about 3 KiB in place of a chunk of 256 KiB of values of 200 bytes.
final class LogChunk {
    /// The bytes of a block: values of a few hundred bytes span one or two.
    static final int BLOCK = 1 << 10;
    /// The most bytes of a chunk: block indexes take 20 bits of the word that binds a tag to its place.
    static final int MAX_BYTES = BLOCK << 20;
    /// The fewest bytes the chunks cut from one array take on average, as each costs about a hundred bytes and the
    /// array is freed only once all of them move: four blocks.
    static final int PIECE_MIN = 4 * BLOCK;
    // The coefficient that sets a block's tag apart from a value's, whose second coefficient is a byte of its stamp
    private static final long BLOCK_MARK = 1L << 60;

    private final LogSegmentView segment;
    private final long frameOffset;
    // Where the first value starts on the array, where the last one ends, and where the first starts in the frame's
    // body
    private final int base;
    private final int end;
    private final int firstBody;
    // The sequence of the commit and where the first value starts in its body, which the tags are bound to
    private final long sequence;
    private final int commitFirst;
    // Per block: the first polynomial of its tag, or null without encryption, and the low 32 bits of the second, or its
    // CRC-32C without encryption
    private final long[] tagHighs;
    private final int[] tagLows;
    private final Object lock;
    // The chunk's bytes from the base, read whole by a checkpoint and dropped as it ends; null otherwise
    private volatile byte[] plain;

    /// Makes the place of the values from `base` to `end` of an array, moved to the frame at `frameOffset` of
    /// `segment`, which holds them back to back from `firstBody` of its body.
    ///
    /// @param sequence the sequence of the commit, which the tags are bound to
    /// @param commitFirst where the first value starts in the commit's body, which the tags are bound to
    /// @param tagHighs the first polynomial of each block's tag, or `null` without encryption
    /// @param tagLows the low 32 bits of the second polynomial of each block's tag, or its CRC-32C
    LogChunk(LogSegmentView segment, long frameOffset, int base, int end, int firstBody, long sequence, int commitFirst,
             long[] tagHighs, int[] tagLows) {
        this.segment = segment;
        this.frameOffset = frameOffset;
        this.base = base;
        this.end = end;
        this.firstBody = firstBody;
        this.sequence = sequence;
        this.commitFirst = commitFirst;
        this.tagHighs = tagHighs;
        this.tagLows = tagLows;
        this.lock = new Object();
    }

    /// {@return the number of blocks of the `length` bytes of a chunk}
    static int blocks(int length) {
        return (length + BLOCK - 1) / BLOCK;
    }

    /// {@return the word that binds the tag of block `block` to its place: the chunk whose first value starts at
    /// `commitFirst` of its commit's body, the commit given apart}
    static long place(int commitFirst, int block) {
        return BLOCK_MARK | (long) commitFirst << 20 | block;
    }

    /// {@return the segment use that wrote the frame}
    LogSegmentView segment() {
        return segment;
    }

    /// {@return where the frame starts in its segment}
    long frameOffset() {
        return frameOffset;
    }

    /// {@return where the first value starts on the array}
    int base() {
        return base;
    }

    /// {@return where the last value ends on the array}
    int end() {
        return end;
    }

    /// {@return the value of `length` bytes at `offset` of the array, in an array of its own}
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the segment cannot be read
    /// @throws LedgerCorruptedException if the bytes read are not those the chunk held
    byte[] read(int offset, int length) {
        var value = new byte[length];
        readInto(offset, length, value, 0);
        return value;
    }

    /// Reads the value of `length` bytes at `offset` of the array into `target` from `targetOffset`, reading only the
    /// blocks it spans unless a checkpoint read the whole chunk.
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the segment cannot be read
    /// @throws LedgerCorruptedException if the bytes read are not those the chunk held
    void readInto(int offset, int length, byte[] target, int targetOffset) {
        var held = plain;
        if (held != null) {
            System.arraycopy(held, offset - base, target, targetOffset, length);
            return;
        }
        if (length == 0) {
            return;
        }
        var from = base + (offset - base) / BLOCK * BLOCK;
        var to = Math.min(end, base + ((offset + length - 1 - base) / BLOCK + 1) * BLOCK);
        var blocks = readBlocks(from, to);
        System.arraycopy(blocks, offset - from, target, targetOffset, length);
    }

    /// Reads the value of `length` bytes at `offset` of the array into `target` from `targetOffset`, for a checkpoint:
    /// the first read of the chunk reads it whole, which the checkpoint's later reads of its values copy from.
    ///
    /// Readers on several threads, the workers of a checkpoint, read the chunk once: the others wait for it.
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the segment cannot be read
    /// @throws LedgerCorruptedException if the bytes read are not those the chunk held
    void readMaterialized(int offset, int length, byte[] target, int targetOffset) {
        var held = plain;
        if (held == null) {
            var read = false;
            synchronized (lock) {
                held = plain;
                if (held == null) {
                    // A fresh array no pool hands out, as a reader may still copy from it once it is dropped
                    held = readBlocks(base, end);
                    plain = held;
                    read = true;
                }
            }
            if (read) {
                segment.spills().materialized(this);
            }
        }
        System.arraycopy(held, offset - base, target, targetOffset, length);
    }

    /// Lets go of the bytes a checkpoint read whole.
    void dropMaterialized() {
        plain = null;
    }

    /// {@return whether a checkpoint read the whole chunk and still keeps its bytes; for tests}
    boolean materializedForTests() {
        return plain != null;
    }

    /// {@return where in its segment's file the frame holds the first byte of the value at `offset` of the array; for
    /// tests}
    long positionForTests(int offset) {
        return frameOffset + LogWriter.FRAME_HEADER + firstBody + (offset - base);
    }

    /// {@return the bytes of the array from `from` to `to`, both on bounds of blocks or the end, read from the frame and
    /// checked against the tags of their blocks}
    private byte[] readBlocks(int from, int to) {
        if (segment.retired()) {
            throw LogSegmentView.Retired.INSTANCE;
        }
        var bytes = new byte[to - from];
        if (bytes.length == 0) {
            return bytes;
        }
        var bodyFrom = firstBody + (from - base);
        segment.readAt(bytes, 0, bytes.length, frameOffset + LogWriter.FRAME_HEADER + bodyFrom);
        var spills = segment.spills();
        spills.readChunkBytes(bytes.length);
        var crypto = spills.takeCrypto();
        try {
            var key = segment.key();
            if (key != null) {
                crypto.decryptInPlace(key, frameOffset, bodyFrom, bytes, 0, bytes.length);
            }
            for (var at = from; at < to; at += BLOCK) {
                var block = (at - base) / BLOCK;
                var length = Math.min(BLOCK, to - at);
                if (!crypto.verifiesBlock(bytes, at - from, length, sequence, place(commitFirst, block),
                        tagHighs == null ? 0 : tagHighs[block], tagLows[block])) {
                    if (segment.retired()) {
                        // Read as a preparation reused the file
                        throw LogSegmentView.Retired.INSTANCE;
                    }
                    throw new LedgerCorruptedException("Values of the log segment " + segment.path()
                            + " do not match the values the store wrote", null);
                }
            }
            return bytes;
        } finally {
            spills.giveBack(crypto);
        }
    }
}

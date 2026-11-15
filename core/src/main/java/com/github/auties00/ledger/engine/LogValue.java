package com.github.auties00.ledger.engine;

/// Where the log holds the value of a version that left the heap ([SpillQueue]), and the tag that authenticates it.
///
/// The value lies at `bodyIndex` of the sealed body of the frame at `frameOffset` of its segment: a frame body is one
/// AES-GCM message whose first byte the keystream block of counter 2 encrypts, so the bytes of the value decrypt
/// alone, with AES-CTR from the block that holds the first of them. CTR alone authenticates nothing; the tag does: a
/// keyed universal hash of the stamp of the version and its plain value ([SpillQueue.Crypto]), under keys that never
/// leave the process, or the CRC-32C of the value in a store without encryption, which seals nothing.
///
/// A value that takes nearly all of its frame in an encrypted segment ([SpillQueue#framesAlone]) has no tag of its
/// own: it is [framed][#framed()], and a read takes the whole frame, header included, and opens it as the AES-GCM
/// message it is, under the key of the segment use and the frame's offset as nonce, so the frame's own tag
/// authenticates the value, at its place, before any byte of it is used. `tagHigh` is then -1, which no tag is, and
/// `tagLow` the bytes of the frame's body.
///
/// It takes 40 bytes, a quarter of what an array of a 1.5 KiB message keeps in its place.
///
/// A value of a group the log wrote through to stable storage, which moves only when it is long
/// ([SpillQueue#THROUGH_VALUE_MIN]), is read back from the device the first time, not from the system's cache.
///
/// @param segment the segment use that wrote the frame
/// @param frameOffset where the frame starts in its segment
/// @param bodyIndex where the value starts in the frame's body
/// @param tagHigh the first polynomial of the tag, or 0 without encryption
/// @param tagLow the low 32 bits of the second polynomial of the tag, or the CRC-32C of the value without encryption
record LogValue(LogSegmentView segment, long frameOffset, int bodyIndex, long tagHigh, int tagLow) {
    /// {@return the place of a value at `bodyIndex` of the body, of `frameBody` bytes, of the frame at `frameOffset` of
    /// `segment`, which the frame's tag authenticates}
    static LogValue framed(LogSegmentView segment, long frameOffset, int bodyIndex, int frameBody) {
        return new LogValue(segment, frameOffset, bodyIndex, -1, frameBody);
    }

    /// {@return whether a read authenticates the value by the tag of its whole frame, which it reads}
    boolean framed() {
        return tagHigh < 0;
    }

    /// {@return the bytes of the body of the value's frame, which is [framed][#framed()]}
    int frameBody() {
        return tagLow;
    }

    /// {@return the value, the `length` bytes this place holds, of the version stamped `stamp`, in an array of its own}
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the segment cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the bytes read are not the value
    byte[] read(long stamp, int length) {
        if (framed()) {
            return segment.readFramed(this, length);
        }
        var value = new byte[length];
        segment.read(this, stamp, length, value, 0);
        return value;
    }

    /// Reads the value, the `length` bytes this place holds, of the version stamped `stamp`, into `target` from
    /// `offset`.
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the segment cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the bytes read are not the value
    void readInto(long stamp, int length, byte[] target, int offset) {
        segment.read(this, stamp, length, target, offset);
    }
}

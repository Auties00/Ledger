package com.github.auties00.ledger.engine;

/// Where the log holds the value of a version that left the heap ([SpillQueue]), and the tag that authenticates it.
///
/// The value lies at `bodyIndex` of the sealed body of the frame at `frameOffset` of its segment: a frame body is one
/// AES-GCM message whose first byte the keystream block of counter 2 encrypts, so the bytes of the value decrypt
/// alone, with AES-CTR from the block that holds the first of them. CTR alone authenticates nothing; the tag does: a
/// keyed universal hash of the stamp of the version and its plain value ([SpillQueue.Crypto]), under keys that never
/// leave the process, or the CRC-32C of the value in a store without encryption, which seals nothing.
///
/// It takes 40 bytes, a quarter of what an array of a 1.5 KiB message keeps in its place.
///
/// @param segment the segment use that wrote the frame
/// @param frameOffset where the frame starts in its segment
/// @param bodyIndex where the value starts in the frame's body
/// @param tagHigh the first polynomial of the tag, or 0 without encryption
/// @param tagLow the low 32 bits of the second polynomial of the tag, or the CRC-32C of the value without encryption
record LogValue(LogSegmentView segment, long frameOffset, int bodyIndex, long tagHigh, int tagLow) {
    /// {@return the value, the `length` bytes this place holds, of the version stamped `stamp`, in an array of its own}
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the segment cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the bytes read are not the value
    byte[] read(long stamp, int length) {
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

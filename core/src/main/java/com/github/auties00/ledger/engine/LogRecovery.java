package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.zip.CRC32C;

/// Replays the log of a store when it opens.
///
/// Segments are read in the order of their numbers, frames in the order of their offsets. Commit frames must carry
/// consecutive sequences; a catalog frame carries the sequence of the commit before it. A frame that is cut short, fails
/// authentication, or carries the tag of another segment's salt (an earlier use of the file) ends its segment: it is
/// the torn tail of a group whose force never completed, or what lies past the last group, and the later segments
/// were written by later opens or after a rollover, which recovered up to the same point. If a group written after a
/// later force follows the frame, the frame was acknowledged and then damaged, and recovery refuses the log rather
/// than drop it.
///
/// A segment whose first frame block is all zeros was prepared and never written. A segment's header is forced before
/// it takes its name, so a last segment no longer than its header is a crash during its creation, and is deleted. Any
/// other segment whose header is damaged, or names another store, another number or another encryption, is refused.
///
/// A store with a checkpoint replays the log from the segment its superblock names, skipping the commits its trees
/// hold; earlier segments hold nothing later, and become spares.
///
/// Segments are read through a buffer, a part at a time.
final class LogRecovery {
    private static final int BUFFER = 1 << 20;

    private LogRecovery() {
    }

    /// Receives what the log holds, in order.
    interface Sink {
        void catalog(int keyspace, String name);

        void commit(long sequence, byte[] body);
    }

    /// What recovery found.
    ///
    /// @param lastSequence the sequence of the last commit replayed, or the checkpoint's
    /// @param lastSegment the number of the last segment, or 0 if there was none
    /// @param lastEmpty whether the last segment holds no frame
    record Result(long lastSequence, long lastSegment, boolean lastEmpty) {
    }

    /// Replays the segments of `files` from `firstSegment`, retiring those before it.
    ///
    /// @param checkpoint the sequence of the last commit the checkpoint holds, or 0; commits up to it are skipped
    /// @throws IOException if a segment cannot be read
    /// @throws LedgerCorruptedException if a segment belongs to another store, or acknowledged data is damaged
    static Result replay(LogFiles files, StoreKeys keys, byte[] storeId, long firstSegment, long checkpoint, Sink sink) throws IOException {
        files.retireBefore(firstSegment, 1);
        var segments = files.segments();
        var cipher = cipher(keys);
        var crc = new CRC32C();
        var last = checkpoint;
        var lastSegment = 0L;
        var lastEmpty = false;
        for (var index = 0; index < segments.size(); index++) {
            var number = segments.get(index);
            var path = files.segment(number);
            try (var reader = new SegmentReader(path)) {
                var header = new byte[LogWriter.SEGMENT_HEADER];
                if (!reader.read(0, header, 0, header.length) || !intactSegmentHeader(header, crc)) {
                    // The header is forced before the segment takes its name, so only a file no longer than its
                    // header can be a crash during its creation; the next open writes a segment in its place
                    if (index == segments.size() - 1 && reader.size() <= LogWriter.SEGMENT_HEADER) {
                        reader.close();
                        Files.delete(path);
                        continue;
                    }
                    throw new LedgerCorruptedException(path + " has a damaged header", null);
                }
                if (!ownSegmentHeader(header, number, storeId, keys)) {
                    throw new LedgerCorruptedException(path + " is not a segment of this store", null);
                }
                lastSegment = number;
                var salt = Arrays.copyOfRange(header, LogWriter.SALT_OFFSET, LogWriter.SALT_OFFSET + LogWriter.SALT_BYTES);
                var tag = ByteBuffer.wrap(salt).getLong();
                var segmentKey = keys.encrypted() ? keys.segmentKey(storeId, number, salt) : null;
                var frame = new byte[LogWriter.FRAME_HEADER];
                long position = LogWriter.SEGMENT_HEADER;
                if (!reader.read(position, frame, 0, LogWriter.FRAME_HEADER) || isZero(frame, 0, LogWriter.FRAME_HEADER)) {
                    // Prepared and never written
                    lastEmpty = true;
                    continue;
                }
                lastEmpty = false;
                var afterPadding = false;
                while (reader.read(position, frame, 0, LogWriter.FRAME_HEADER)) {
                    if (position % LogWriter.ALIGNMENT != 0) {
                        // The padding after a group, which only a group's first frame follows; a frame never starts
                        // with a zero byte, and the padding can be shorter than a frame header
                        var boundary = Math.min((position + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT, reader.size());
                        if (reader.isZero(position, boundary - position)) {
                            position = boundary;
                            afterPadding = true;
                            continue;
                        }
                    }
                    if (!validFrameHeader(frame, tag, crc)
                            || (afterPadding && (frame[17] & LogWriter.GROUP_START) == 0)) {
                        checkNoLaterGroup(reader, position, last, tag, crc, path);
                        break;
                    }
                    afterPadding = false;
                    var header32 = ByteBuffer.wrap(frame);
                    var sealedLength = header32.getInt(4);
                    var sequence = header32.getLong(8);
                    var type = frame[16];
                    var end = position + LogWriter.FRAME_HEADER + sealedLength;
                    byte[] body = null;
                    if (sealedLength >= 0 && end <= reader.size()) {
                        var sealed = new byte[LogWriter.FRAME_HEADER + sealedLength];
                        System.arraycopy(frame, 0, sealed, 0, LogWriter.FRAME_HEADER);
                        if (reader.read(position + LogWriter.FRAME_HEADER, sealed, LogWriter.FRAME_HEADER, sealedLength)) {
                            body = open(sealed, position, sealedLength, keys, segmentKey, cipher, crc);
                        }
                    }
                    if (body == null) {
                        checkNoLaterGroup(reader, position, last, tag, crc, path);
                        break;
                    }
                    switch (type) {
                        case FrameBody.COMMIT -> {
                            if (sequence > checkpoint) {
                                if (sequence != last + 1) {
                                    throw new LedgerCorruptedException(path + " holds commit " + sequence + " after commit " + last, null);
                                }
                                sink.commit(sequence, body);
                                last = sequence;
                            }
                            // Otherwise held by the checkpoint's trees
                        }
                        case FrameBody.CATALOG -> {
                            if (sequence > last) {
                                throw new LedgerCorruptedException(path + " holds a catalog at " + sequence + " after commit " + last, null);
                            }
                            FrameBody.readCatalog(body, sink::catalog);
                        }
                        default -> throw new LedgerCorruptedException(path + " holds a frame of unknown type " + type, null);
                    }
                    position = end;
                }
            }
        }
        return new Result(last, lastSegment, lastEmpty);
    }

    private static Cipher cipher(StoreKeys keys) {
        if (!keys.encrypted()) {
            return null;
        }
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("AES-GCM is unavailable", exception);
        }
    }

    /// {@return whether `header` is a whole segment header of this format}
    private static boolean intactSegmentHeader(byte[] header, CRC32C crc) {
        var buffer = ByteBuffer.wrap(header);
        crc.reset();
        crc.update(header, 0, LogWriter.HEADER_CRC_OFFSET);
        return buffer.getLong(0) == LogWriter.SEGMENT_MAGIC
                && buffer.getInt(8) == LogWriter.SEGMENT_VERSION
                && buffer.getInt(LogWriter.HEADER_CRC_OFFSET) == (int) crc.getValue();
    }

    /// {@return whether the intact segment header `header` names this store, its encryption and `number`}
    private static boolean ownSegmentHeader(byte[] header, long number, byte[] storeId, StoreKeys keys) {
        var buffer = ByteBuffer.wrap(header);
        return header[12] == (keys.encrypted() ? 1 : 0)
                && buffer.getLong(16) == number
                && Arrays.equals(header, 24, 40, storeId, 0, 16);
    }

    /// {@return whether `frame` starts with a frame header of the segment whose salt has `tag`}
    private static boolean validFrameHeader(byte[] frame, long tag, CRC32C crc) {
        var header = ByteBuffer.wrap(frame);
        if (header.getInt(0) != LogWriter.MAGIC || header.getLong(20) != tag) {
            return false;
        }
        crc.reset();
        crc.update(frame, 0, LogWriter.FRAME_HEADER - Integer.BYTES);
        return header.getInt(LogWriter.FRAME_HEADER - Integer.BYTES) == (int) crc.getValue();
    }

    /// {@return the body of the frame in `sealed`, its header then its sealed body, at `position` of its segment, or
    /// `null` if it fails authentication}
    private static byte[] open(byte[] sealed, long position, int sealedLength, StoreKeys keys, SecretKeySpec segmentKey,
                               Cipher cipher, CRC32C crc) {
        if (keys.encrypted()) {
            if (sealedLength < LogWriter.TAG_BYTES) {
                return null;
            }
            try {
                cipher.init(Cipher.DECRYPT_MODE, segmentKey, new GCMParameterSpec(LogWriter.TAG_BYTES * 8, LogWriter.nonce(position)));
                cipher.updateAAD(sealed, 0, LogWriter.FRAME_HEADER);
                return cipher.doFinal(sealed, LogWriter.FRAME_HEADER, sealedLength);
            } catch (AEADBadTagException _) {
                return null;
            } catch (GeneralSecurityException exception) {
                throw new IllegalStateException("AES-GCM failed", exception);
            }
        }
        if (sealedLength < Integer.BYTES) {
            return null;
        }
        var bodyLength = sealedLength - Integer.BYTES;
        crc.reset();
        crc.update(sealed, 0, LogWriter.FRAME_HEADER + bodyLength);
        if (ByteBuffer.wrap(sealed).getInt(LogWriter.FRAME_HEADER + bodyLength) != (int) crc.getValue()) {
            return null;
        }
        return Arrays.copyOfRange(sealed, LogWriter.FRAME_HEADER, LogWriter.FRAME_HEADER + bodyLength);
    }

    /// Throws if a group written after a later force follows the invalid frame at `position`.
    private static void checkNoLaterGroup(SegmentReader reader, long position, long last, long tag, CRC32C crc, Path path) throws IOException {
        var frame = new byte[LogWriter.FRAME_HEADER];
        for (var candidate = (position / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
             reader.read(candidate, frame, 0, LogWriter.FRAME_HEADER);
             candidate += LogWriter.ALIGNMENT) {
            if (validFrameHeader(frame, tag, crc)
                    && (frame[17] & LogWriter.GROUP_START) != 0
                    && ByteBuffer.wrap(frame).getLong(8) > last) {
                throw new LedgerCorruptedException(path + " is damaged at offset " + position + ", before data written after it", null);
            }
        }
    }

    private static boolean isZero(byte[] data, int offset, int length) {
        for (var i = offset; i < offset + length; i++) {
            if (data[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /// A segment read through a buffer of [#BUFFER] bytes, refilled forward as recovery moves through it.
    private static final class SegmentReader implements AutoCloseable {
        private final FileChannel channel;
        private final long size;
        private final byte[] buffer;
        private long start;
        private int length;

        SegmentReader(Path path) throws IOException {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.size = channel.size();
            this.buffer = new byte[(int) Math.min(BUFFER, Math.max(size, 1))];
        }

        long size() {
            return size;
        }

        /// Copies the `count` bytes at `position` into `target` at `offset`.
        ///
        /// @return whether the segment holds them all
        boolean read(long position, byte[] target, int offset, int count) throws IOException {
            if (position < 0 || position + count > size) {
                return false;
            }
            if (position < start || position + count > start + length) {
                if (count > buffer.length) {
                    var direct = ByteBuffer.wrap(target, offset, count);
                    while (direct.hasRemaining()) {
                        if (channel.read(direct, position + direct.position() - offset) < 0) {
                            return false;
                        }
                    }
                    return true;
                }
                fill(position);
            }
            System.arraycopy(buffer, (int) (position - start), target, offset, count);
            return true;
        }

        /// {@return whether the `count` bytes at `position` are all zero; bytes past the end count as zero}
        boolean isZero(long position, long count) throws IOException {
            var end = Math.min(size, position + count);
            for (var at = position; at < end; ) {
                if (at < start || at >= start + length) {
                    fill(at);
                    if (length == 0) {
                        // The file ended early, as a torn write leaves it
                        return true;
                    }
                }
                var limit = (int) Math.min(start + length, end);
                for (var i = (int) (at - start); i < limit - start; i++) {
                    if (buffer[i] != 0) {
                        return false;
                    }
                }
                at = start + (limit - start);
            }
            return true;
        }

        private void fill(long position) throws IOException {
            start = position;
            length = 0;
            var wanted = (int) Math.min(buffer.length, size - position);
            var target = ByteBuffer.wrap(buffer, 0, wanted);
            while (target.hasRemaining()) {
                if (channel.read(target, position + target.position()) < 0) {
                    break;
                }
            }
            length = target.position();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}

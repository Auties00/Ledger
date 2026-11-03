package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

/// Replays the log of a store when it opens.
///
/// Segments are read in the order of their numbers, frames in the order of their offsets. Commit frames must carry
/// consecutive sequences; a catalog frame carries the sequence of the commit before it. A frame that is cut short or
/// fails authentication ends its segment: it is the torn tail of a group whose force never completed, and the later
/// segments were written by later opens, which recovered up to the same point. If a group written after a later force
/// follows the frame, the frame was acknowledged and then damaged, and recovery refuses the log rather than drop it.
///
/// A segment's header is forced before its first frame is written, so a last segment no longer than its header is a
/// crash during its creation, and is deleted. Any other segment whose header is damaged, or names another store,
/// another number or another encryption, is refused.
final class LogRecovery {
    private LogRecovery() {
    }

    /// Receives what the log holds, in order.
    interface Sink {
        void catalog(int keyspace, String name);

        void commit(long sequence, byte[] body);
    }

    /// What recovery found.
    ///
    /// @param lastSequence the sequence of the last commit replayed, or 0
    /// @param lastSegment the number of the last segment, or 0 if there was none
    record Result(long lastSequence, long lastSegment) {
    }

    /// Replays every segment of `directory`.
    ///
    /// @throws IOException if a segment cannot be read
    /// @throws LedgerCorruptedException if a segment belongs to another store, or acknowledged data is damaged
    static Result replay(Path directory, StoreKeys keys, byte[] storeId, Sink sink) throws IOException {
        var segments = segments(directory);
        var cipher = cipher(keys);
        var crc = new CRC32C();
        var last = 0L;
        var lastSegment = 0L;
        for (var index = 0; index < segments.size(); index++) {
            var number = segments.get(index);
            var path = directory.resolve(LogWriter.segmentName(number));
            var data = Files.readAllBytes(path);
            if (!intactSegmentHeader(data, crc)) {
                // The header is forced before the first frame is written, so only a file no longer than its header
                // can be a crash during creation; the next open writes a segment of the same number in its place
                if (index == segments.size() - 1 && data.length <= LogWriter.SEGMENT_HEADER) {
                    Files.delete(path);
                    continue;
                }
                throw new LedgerCorruptedException(path + " has a damaged header", null);
            }
            if (!ownSegmentHeader(data, number, storeId, keys)) {
                throw new LedgerCorruptedException(path + " is not a segment of this store", null);
            }
            lastSegment = number;
            var segmentKey = keys.encrypted() ? keys.segmentKey(storeId, number) : null;
            var position = LogWriter.SEGMENT_HEADER;
            var afterPadding = false;
            while (position + LogWriter.FRAME_HEADER <= data.length) {
                if (position % LogWriter.ALIGNMENT != 0) {
                    // The padding after a force, which only a group's first frame follows; a frame never starts
                    // with a zero byte, and the padding can be shorter than a frame header
                    var boundary = Math.min(align(position), data.length);
                    if (isZero(data, position, boundary - position)) {
                        position = boundary;
                        afterPadding = true;
                        continue;
                    }
                } else if (isZero(data, position, LogWriter.FRAME_HEADER)) {
                    // The end of the log, unless a block of a forced group was lost before a later group
                    checkNoLaterGroup(data, position, last, crc, path);
                    break;
                }
                var header = ByteBuffer.wrap(data, position, LogWriter.FRAME_HEADER);
                if (!validFrameHeader(data, position, crc)
                        || (afterPadding && (data[position + 17] & LogWriter.GROUP_START) == 0)) {
                    checkNoLaterGroup(data, position, last, crc, path);
                    break;
                }
                afterPadding = false;
                var sealedLength = header.getInt(position + 4);
                var sequence = header.getLong(position + 8);
                var type = data[position + 16];
                var end = (long) position + LogWriter.FRAME_HEADER + sealedLength;
                var body = sealedLength < 0 || end > data.length ? null : open(data, position, sealedLength, keys, segmentKey, cipher, crc);
                if (body == null) {
                    checkNoLaterGroup(data, position, last, crc, path);
                    break;
                }
                switch (type) {
                    case FrameBody.COMMIT -> {
                        if (sequence != last + 1) {
                            throw new LedgerCorruptedException(path + " holds commit " + sequence + " after commit " + last, null);
                        }
                        sink.commit(sequence, body);
                        last = sequence;
                    }
                    case FrameBody.CATALOG -> {
                        if (sequence != last) {
                            throw new LedgerCorruptedException(path + " holds a catalog at " + sequence + " after commit " + last, null);
                        }
                        FrameBody.readCatalog(body, sink::catalog);
                    }
                    default -> throw new LedgerCorruptedException(path + " holds a frame of unknown type " + type, null);
                }
                position = (int) end;
            }
        }
        return new Result(last, lastSegment);
    }

    private static List<Long> segments(Path directory) throws IOException {
        var numbers = new ArrayList<Long>();
        if (!Files.isDirectory(directory)) {
            return numbers;
        }
        try (var files = Files.list(directory)) {
            for (var file : (Iterable<Path>) files::iterator) {
                var name = file.getFileName().toString();
                if (name.length() == 20 && name.endsWith(".log")) {
                    try {
                        numbers.add(Long.parseUnsignedLong(name.substring(0, 16), 16));
                    } catch (NumberFormatException _) {
                        // Not a segment
                    }
                }
            }
        }
        numbers.sort(Long::compare);
        return numbers;
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

    /// {@return whether `data` starts with a whole segment header of this format}
    private static boolean intactSegmentHeader(byte[] data, CRC32C crc) {
        if (data.length < LogWriter.SEGMENT_HEADER) {
            return false;
        }
        var header = ByteBuffer.wrap(data);
        crc.reset();
        crc.update(data, 0, 40);
        return header.getLong(0) == LogWriter.SEGMENT_MAGIC
                && header.getInt(8) == LogWriter.SEGMENT_VERSION
                && header.getInt(40) == (int) crc.getValue();
    }

    /// {@return whether the intact segment header of `data` names this store, its encryption and `number`}
    private static boolean ownSegmentHeader(byte[] data, long number, byte[] storeId, StoreKeys keys) {
        var header = ByteBuffer.wrap(data);
        return header.get(12) == (keys.encrypted() ? 1 : 0)
                && header.getLong(16) == number
                && Arrays.equals(data, 24, 40, storeId, 0, 16);
    }

    private static boolean validFrameHeader(byte[] data, int position, CRC32C crc) {
        var header = ByteBuffer.wrap(data);
        if (header.getInt(position) != LogWriter.MAGIC) {
            return false;
        }
        crc.reset();
        crc.update(data, position, LogWriter.FRAME_HEADER - Integer.BYTES);
        return header.getInt(position + LogWriter.FRAME_HEADER - Integer.BYTES) == (int) crc.getValue();
    }

    /// {@return the body of the frame at `position`, or `null` if it fails authentication}
    private static byte[] open(byte[] data, int position, int sealedLength, StoreKeys keys,
                               javax.crypto.spec.SecretKeySpec segmentKey, Cipher cipher, CRC32C crc) {
        var bodyOffset = position + LogWriter.FRAME_HEADER;
        if (keys.encrypted()) {
            if (sealedLength < LogWriter.TAG_BYTES) {
                return null;
            }
            try {
                cipher.init(Cipher.DECRYPT_MODE, segmentKey, new GCMParameterSpec(LogWriter.TAG_BYTES * 8,
                        LogWriter.nonce(position)));
                cipher.updateAAD(data, position, LogWriter.FRAME_HEADER);
                return cipher.doFinal(data, bodyOffset, sealedLength);
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
        crc.update(data, position, LogWriter.FRAME_HEADER + bodyLength);
        if (ByteBuffer.wrap(data).getInt(bodyOffset + bodyLength) != (int) crc.getValue()) {
            return null;
        }
        return Arrays.copyOfRange(data, bodyOffset, bodyOffset + bodyLength);
    }

    /// Throws if a group written after a later force follows the invalid frame at `position`.
    private static void checkNoLaterGroup(byte[] data, int position, long last, CRC32C crc, Path path) {
        for (var candidate = align(position + 1); candidate + LogWriter.FRAME_HEADER <= data.length; candidate += LogWriter.ALIGNMENT) {
            if (validFrameHeader(data, candidate, crc)
                    && (data[candidate + 17] & LogWriter.GROUP_START) != 0
                    && ByteBuffer.wrap(data).getLong(candidate + 8) > last) {
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

    private static int align(int position) {
        return (position + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT;
    }
}

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
/// Segments are read in the order of their numbers, frames in the order of their offsets. Commits must have
/// consecutive sequences, whether each has a frame of its own or a run holds it, whose commits have the sequences from
/// the one its frame carries; a catalog frame carries the sequence of the commit before it. The first frame of a segment,
/// the first of a group, gives the tag of the use of the segment the later ones must carry. A frame that is cut short,
/// fails authentication, or carries the tag of another segment's salt (an earlier use of the file) or of another use
/// of the segment ends its segment: it is the torn tail of a group whose force never completed, or what lies past the
/// last group, and the later segments were written by later opens or after a rollover, which recovered up to the same
/// point. If a group written after a later force follows the frame, the frame was acknowledged and then damaged, and
/// recovery refuses the log rather than drop it. The search for such a group takes the first frame of a group of any
/// use of the segment: at most one use writes past its first group ([LogWriter]).
///
/// A segment whose first frame header is all zeros was prepared and never written, unless such a group follows, and
/// the next open writes into it. Its name is known to be durable only if a close marked it ([LogWriter#KEPT_MARK]): a
/// process may have stopped between its rename and the force of its directory, which the open then forces before a
/// commit depends on it. A segment's header is forced before it takes its name, so a last segment holding
/// nothing past a damaged header is a crash during its creation, or a segment a close kept, and is deleted. Any other
/// segment whose header is damaged, or names another store, another number or another encryption, is refused.
///
/// The commits recovery replays may be in the cache of a process that stopped before forcing them: the last segment
/// that holds frames is forced before any commit follows them ([Result#lastWritten()]).
///
/// A store with a checkpoint replays the log from the segment its superblock names, skipping the commits its trees
/// hold; earlier segments hold nothing later, and become spares.
///
/// A commit whose units of [BlobFile] the file does not hold whole ends its segment like a frame that fails
/// authentication: its group was forced beside that file, and acknowledged only once both forces completed, so without a
/// later group it is a commit no one was told of. The commits of its run before it are replayed, those after it are
/// not. A run whose frame authenticates but whose entries are malformed is damage, not a tear, and is refused.
///
/// Segments are read through one buffer, a part at a time, which passes from each segment to the next. A frame is
/// authenticated where the buffer holds it, and only its opened body is a new array: replay copies no sealed bytes, and
/// a frame longer than the buffer is read into an array of its own, which no later frame keeps.
final class LogRecovery {
    private static final int BUFFER = 1 << 20;
    /// The longest sealed body a frame can declare: a body of [FrameBody#MAX_BYTES] with its tag or CRC. A longer one
    /// is a damaged header, and its length would overflow once the frame header is added.
    private static final int MAX_SEALED = FrameBody.MAX_BYTES + Math.max(LogWriter.TAG_BYTES, Integer.BYTES);

    private LogRecovery() {
    }

    /// Receives what the log holds, in order.
    interface Sink {
        void catalog(int keyspace, String name);

        /// Replays the commit `sequence`, unless the file of large values lacks one of its units.
        ///
        /// @return whether it replayed the commit; it changed nothing if not
        boolean commit(long sequence, byte[] body);

        void epoch(int epoch);
    }

    /// What recovery found.
    ///
    /// @param lastSequence the sequence of the last commit replayed, or the checkpoint's
    /// @param lastSegment the number of the last segment, or 0 if there was none
    /// @param empty the last segment if it was prepared and never written and has the current version, which the next
    ///              writer writes into, with
    ///              [LogFiles.Prepared#nameDurable()] telling whether a close marked it; or `null`
    /// @param lastWritten the last segment that holds a frame, or `null` if none does
    record Result(long lastSequence, long lastSegment, LogFiles.Prepared empty, Path lastWritten) {
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
        LogFiles.Prepared empty = null;
        Path lastWritten = null;
        byte[] buffer = null;
        for (var index = 0; index < segments.size(); index++) {
            var number = segments.get(index);
            var path = files.segment(number);
            try (var reader = new SegmentReader(path, buffer)) {
                buffer = reader.buffer();
                var header = new byte[LogWriter.SEGMENT_HEADER];
                if (!reader.read(0, header, 0, header.length) || !intactSegmentHeader(header, crc)) {
                    // The header is forced before the segment takes its name, so only a file holding nothing past its
                    // header can be a crash during its creation; the next open writes a segment in its place
                    if (index == segments.size() - 1 && (reader.size() <= LogWriter.SEGMENT_HEADER
                            || reader.isZero(LogWriter.SEGMENT_HEADER, reader.size() - LogWriter.SEGMENT_HEADER))) {
                        reader.close();
                        Files.delete(path);
                        continue;
                    }
                    throw new LedgerCorruptedException(path + " has a damaged header", null);
                }
                var version = ByteBuffer.wrap(header).getInt(8);
                if (version < LogWriter.OLDEST_SEGMENT_VERSION || version > LogWriter.SEGMENT_VERSION) {
                    throw new LedgerCorruptedException(path + " has segment version " + version + ", and this build reads "
                            + LogWriter.OLDEST_SEGMENT_VERSION + " to " + LogWriter.SEGMENT_VERSION, null);
                }
                if (!ownSegmentHeader(header, number, storeId, keys)) {
                    throw new LedgerCorruptedException(path + " is not a segment of this store", null);
                }
                lastSegment = number;
                var salt = Arrays.copyOfRange(header, LogWriter.SALT_OFFSET, LogWriter.SALT_OFFSET + LogWriter.SALT_BYTES);
                var tag = ByteBuffer.wrap(salt).getLong();
                var frame = new byte[LogWriter.FRAME_HEADER];
                long position = LogWriter.SEGMENT_HEADER;
                if (!reader.read(position, frame, 0, LogWriter.FRAME_HEADER) || isZero(frame, 0, LogWriter.FRAME_HEADER)) {
                    // Prepared and never written, unless damage zeroed the first frame before groups a later force made
                    // durable, whose first frames may carry the sequence of the commit before them
                    checkNoLaterGroup(reader, position, Long.MIN_VALUE, tag, crc, path);
                    var mark = new byte[1];
                    var kept = reader.read(LogWriter.KEPT_MARK, mark, 0, 1) && mark[0] == LogWriter.KEPT_MARK_VALUE;
                    // An older version's stays empty: its header promises frames the writer may not keep to
                    empty = version != LogWriter.SEGMENT_VERSION ? null
                            : new LogFiles.Prepared(number, path, salt, reader.size() / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT, kept);
                    continue;
                }
                empty = null;
                if (!wholeFrameHeader(frame, tag, crc) || (frame[17] & LogWriter.GROUP_START) == 0) {
                    // The torn first group of a use, which the next open follows with a segment of its own
                    checkNoLaterGroup(reader, position, Long.MIN_VALUE, tag, crc, path);
                    continue;
                }
                var useTag = ByteBuffer.wrap(frame).getLong(LogWriter.USE_TAG_OFFSET);
                var segmentKey = keys.encrypted() ? keys.segmentKey(storeId, number, salt, useTag) : null;
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
                    if (!validFrameHeader(frame, tag, useTag, crc)
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
                    if (sealedLength >= 0 && sealedLength <= MAX_SEALED && end <= reader.size()) {
                        byte[] sealed;
                        int at;
                        if (sealedLength <= reader.buffer().length) {
                            sealed = reader.buffer();
                            at = reader.view(position + LogWriter.FRAME_HEADER, sealedLength);
                        } else {
                            sealed = new byte[sealedLength];
                            at = reader.read(position + LogWriter.FRAME_HEADER, sealed, 0, sealedLength) ? 0 : -1;
                        }
                        if (at >= 0) {
                            body = open(frame, sealed, at, position, sealedLength, keys, segmentKey, cipher, crc);
                        }
                    }
                    if (body == null) {
                        checkNoLaterGroup(reader, position, last, tag, crc, path);
                        break;
                    }
                    lastWritten = path;
                    var replayed = true;
                    switch (type) {
                        case FrameBody.COMMIT -> {
                            if (sequence > checkpoint) {
                                if (sequence != last + 1) {
                                    throw new LedgerCorruptedException(path + " holds commit " + sequence + " after commit " + last, null);
                                }
                                replayed = sink.commit(sequence, body);
                                if (replayed) {
                                    last = sequence;
                                }
                            }
                            // Otherwise held by the checkpoint's trees
                        }
                        case FrameBody.COMMITS -> {
                            var commits = new FrameBody.RunReader(body);
                            for (var next = sequence; replayed && commits.hasNext(); next++) {
                                if (next <= checkpoint) {
                                    // Held by the checkpoint's trees
                                    commits.skip();
                                    continue;
                                }
                                if (next != last + 1) {
                                    throw new LedgerCorruptedException(path + " holds commit " + next + " after commit " + last, null);
                                }
                                replayed = sink.commit(next, commits.next());
                                if (replayed) {
                                    last = next;
                                }
                            }
                        }
                        case FrameBody.CATALOG -> {
                            if (sequence > last) {
                                throw new LedgerCorruptedException(path + " holds a catalog at " + sequence + " after commit " + last, null);
                            }
                            FrameBody.readCatalog(body, sink::catalog);
                        }
                        case FrameBody.EPOCH -> {
                            if (sequence > last) {
                                throw new LedgerCorruptedException(path + " holds an epoch at " + sequence + " after commit " + last, null);
                            }
                            sink.epoch(FrameBody.readEpoch(body));
                        }
                        default -> throw new LedgerCorruptedException(path + " holds a frame of unknown type " + type, null);
                    }
                    if (!replayed) {
                        checkNoLaterGroup(reader, position, last, tag, crc, path);
                        break;
                    }
                    position = end;
                }
            }
        }
        return new Result(last, lastSegment, empty, lastWritten);
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

    /// {@return whether `header` is a whole segment header, of any version}
    private static boolean intactSegmentHeader(byte[] header, CRC32C crc) {
        var buffer = ByteBuffer.wrap(header);
        crc.reset();
        crc.update(header, 0, LogWriter.HEADER_CRC_OFFSET);
        return buffer.getLong(0) == LogWriter.SEGMENT_MAGIC
                && buffer.getInt(LogWriter.HEADER_CRC_OFFSET) == (int) crc.getValue();
    }

    /// {@return whether the intact segment header `header` names this store, its encryption and `number`}
    private static boolean ownSegmentHeader(byte[] header, long number, byte[] storeId, StoreKeys keys) {
        var buffer = ByteBuffer.wrap(header);
        return header[12] == (keys.encrypted() ? 1 : 0)
                && buffer.getLong(16) == number
                && Arrays.equals(header, 24, 40, storeId, 0, 16);
    }

    /// {@return whether `frame` starts with a whole frame header of the segment whose salt has `tag`, of any use}
    private static boolean wholeFrameHeader(byte[] frame, long tag, CRC32C crc) {
        var header = ByteBuffer.wrap(frame);
        if (header.getInt(0) != LogWriter.MAGIC || header.getLong(LogWriter.SEGMENT_TAG_OFFSET) != tag) {
            return false;
        }
        crc.reset();
        crc.update(frame, 0, LogWriter.FRAME_HEADER - Integer.BYTES);
        return header.getInt(LogWriter.FRAME_HEADER - Integer.BYTES) == (int) crc.getValue();
    }

    /// {@return whether `frame` starts with a frame header of the use `useTag` of the segment whose salt has `tag`}
    private static boolean validFrameHeader(byte[] frame, long tag, long useTag, CRC32C crc) {
        return wholeFrameHeader(frame, tag, crc) && ByteBuffer.wrap(frame).getLong(LogWriter.USE_TAG_OFFSET) == useTag;
    }

    /// {@return a new array holding the body of the frame at `position` of its segment, whose header is `header` and
    /// whose `sealedLength` sealed bytes start at `at` of `sealed`, or `null` if it fails authentication}
    ///
    /// The header authenticated is the one replay parsed. The result never shares `sealed`, which may be the buffer the
    /// next frame is read into.
    private static byte[] open(byte[] header, byte[] sealed, int at, long position, int sealedLength, StoreKeys keys,
                               SecretKeySpec segmentKey, Cipher cipher, CRC32C crc) {
        if (keys.encrypted()) {
            if (sealedLength < LogWriter.TAG_BYTES) {
                return null;
            }
            try {
                cipher.init(Cipher.DECRYPT_MODE, segmentKey, new GCMParameterSpec(LogWriter.TAG_BYTES * 8, LogWriter.nonce(position)));
                cipher.updateAAD(header, 0, LogWriter.FRAME_HEADER);
                // Into an array the cipher allocates: decrypting in place would leave the body in the buffer
                return cipher.doFinal(sealed, at, sealedLength);
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
        crc.update(header, 0, LogWriter.FRAME_HEADER);
        crc.update(sealed, at, bodyLength);
        if (ByteBuffer.wrap(sealed).getInt(at + bodyLength) != (int) crc.getValue()) {
            return null;
        }
        return Arrays.copyOfRange(sealed, at, at + bodyLength);
    }

    /// Throws if a group written after a later force, by any use of the segment whose salt has `tag`, follows the
    /// invalid frame at `position`.
    private static void checkNoLaterGroup(SegmentReader reader, long position, long last, long tag, CRC32C crc, Path path) throws IOException {
        var frame = new byte[LogWriter.FRAME_HEADER];
        for (var candidate = (position / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
             reader.read(candidate, frame, 0, LogWriter.FRAME_HEADER);
             candidate += LogWriter.ALIGNMENT) {
            if (wholeFrameHeader(frame, tag, crc)
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

    /// A segment read through a buffer of at most [#BUFFER] bytes, refilled forward as recovery moves through it.
    ///
    /// Only bytes the reader itself filled are read from the buffer, so the buffer of another reader can be handed to it.
    static final class SegmentReader implements AutoCloseable {
        private final FileChannel channel;
        private final long size;
        private final byte[] buffer;
        private long start;
        private int length;

        /// Opens the segment at `path`, read through `buffer` if that is as long as the reader needs, otherwise through a
        /// buffer of its own.
        ///
        /// @param buffer the buffer of the reader of an earlier segment, or `null`
        SegmentReader(Path path, byte[] buffer) throws IOException {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            this.size = channel.size();
            var wanted = (int) Math.min(BUFFER, Math.max(size, 1));
            this.buffer = buffer != null && buffer.length >= wanted ? buffer : new byte[wanted];
        }

        long size() {
            return size;
        }

        /// {@return the buffer the segment is read through, to hand to the reader of the next segment}
        byte[] buffer() {
            return buffer;
        }

        /// {@return the offset in [#buffer()] of the `count` bytes at `position`, read into it if it did not hold them,
        /// or -1 if the segment does not hold them all}
        ///
        /// The bytes stay there until the next call to the reader; `count` is at most the buffer's length.
        int view(long position, int count) throws IOException {
            if (position < 0 || position + count > size) {
                return -1;
            }
            if (position < start || position + count > start + length) {
                fill(position);
                if (count > length) {
                    // The file ended before the size it had when the reader opened it
                    return -1;
                }
            }
            return (int) (position - start);
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
                if (count > length) {
                    // The file ended before the size it had when the reader opened it
                    return false;
                }
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

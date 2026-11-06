package com.github.auties00.ledger.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/// The files of a store's log directory.
///
/// ```text
/// %016x.log    a segment, by number; recovery reads these only
/// %016x.spare  an obsolete segment kept to be prepared again, named after the number it had
/// %016x.prep   a new segment being prepared, which a crash may leave behind
/// ```
///
/// A segment is prepared before the log needs it ([#prepare(long, long)]): a spare, or a new file, gets the segment's
/// header with a fresh salt and a zeroed first frame block, is forced, and only then takes its name, so recovery never
/// meets a segment half prepared, and finds one never written empty at once. A spare has its blocks already, and a new
/// file can be written with zeros to its full length first, except on Windows: writing into either leaves a force no
/// file size and no block allocation to record, which on ext4 halved the time of a durable commit and spared one force
/// in a thousand a wait of tens of milliseconds for a journal commit. Once a checkpoint makes segments obsolete, they become spares ([#retireBefore(long, int)]) instead of
/// being deleted.
///
/// It is safe for concurrent use: the log writer prepares while a checkpoint retires.
final class LogFiles {
    private static final String SEGMENT = ".log";
    private static final String SPARE = ".spare";
    private static final String PREPARING = ".prep";
    private static final SecureRandom SALTS = new SecureRandom();
    // Not on Windows, where forcing a growing file cost a durable commit no more than forcing one written with zeros
    // first, and the zeros made an ingest of a history a fifth slower
    private static final boolean ZERO_FILL = !System.getProperty("os.name", "").startsWith("Windows");

    private final Path directory;
    private final StoreKeys keys;
    private final byte[] storeId;
    private final Object lock;

    /// Creates the files of the log in `directory`.
    LogFiles(Path directory, StoreKeys keys, byte[] storeId) {
        this.directory = directory;
        this.keys = keys;
        this.storeId = storeId;
        this.lock = new Object();
    }

    static String segmentName(long number) {
        return "%016x".formatted(number) + SEGMENT;
    }

    /// {@return the number of the segment named `name`, or -1 if `name` names no segment}
    static long segmentNumber(String name) {
        if (name.length() != 16 + SEGMENT.length() || !name.endsWith(SEGMENT)) {
            return -1;
        }
        try {
            return Long.parseUnsignedLong(name, 0, 16, 16);
        } catch (NumberFormatException _) {
            return -1;
        }
    }

    Path segment(long number) {
        return directory.resolve(segmentName(number));
    }

    /// {@return the numbers of the segments, ascending}
    List<Long> segments() throws IOException {
        var numbers = new ArrayList<Long>();
        if (!Files.isDirectory(directory)) {
            return numbers;
        }
        try (var files = Files.list(directory)) {
            for (var file : (Iterable<Path>) files::iterator) {
                var number = segmentNumber(file.getFileName().toString());
                if (number >= 0) {
                    numbers.add(number);
                }
            }
        }
        numbers.sort(Long::compare);
        return numbers;
    }

    private List<Path> spares() throws IOException {
        var spares = new ArrayList<Path>();
        try (var files = Files.list(directory)) {
            for (var file : (Iterable<Path>) files::iterator) {
                if (file.getFileName().toString().endsWith(SPARE)) {
                    spares.add(file);
                }
            }
        }
        return spares;
    }

    /// A segment ready to be written.
    ///
    /// @param number its number
    /// @param path its file
    /// @param salt the salt of its key, which its frame headers carry
    record Prepared(long number, Path path, byte[] salt) {
    }

    /// {@return the directory of the segments}
    Path directory() {
        return directory;
    }

    /// Prepares segment `number`, which must not exist, from a spare or a new file of `length` bytes.
    ///
    /// @param zeros whether a new file is first written with zeros to its full length
    /// @return the segment, durable under its name
    Prepared prepare(long number, long length, boolean zeros) throws IOException {
        synchronized (lock) {
            var target = segment(number);
            var spares = spares();
            // A new file grows as its first use writes it: setting its length would allocate nothing on most
            // file systems and cost a slow allocation on NTFS, while a spare has its blocks already
            Path source;
            var fresh = spares.isEmpty();
            if (fresh) {
                source = directory.resolve("%016x".formatted(number) + PREPARING);
                Files.deleteIfExists(source);
                Files.createFile(source);
            } else {
                source = spares.getFirst();
            }
            var salt = new byte[LogWriter.SALT_BYTES];
            SALTS.nextBytes(salt);
            var start = ByteBuffer.allocate(LogWriter.SEGMENT_HEADER + LogWriter.ALIGNMENT);
            start.putLong(0, LogWriter.SEGMENT_MAGIC);
            start.putInt(8, LogWriter.SEGMENT_VERSION);
            start.put(12, (byte) (keys.encrypted() ? 1 : 0));
            start.putLong(16, number);
            start.put(24, storeId);
            start.put(LogWriter.SALT_OFFSET, salt);
            var crc = new CRC32C();
            crc.update(start.array(), 0, LogWriter.HEADER_CRC_OFFSET);
            start.putInt(LogWriter.HEADER_CRC_OFFSET, (int) crc.getValue());
            try (var channel = FileChannel.open(source, StandardOpenOption.WRITE)) {
                if (fresh && zeros && ZERO_FILL) {
                    fill(channel, start.capacity(), length);
                }
                // The header, and a first frame block of zeros that marks the segment as never written
                while (start.hasRemaining()) {
                    channel.write(start, start.position());
                }
                FullSync.force(channel, source, true);
            }
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            Durability.forceDirectory(directory);
            return new Prepared(number, target, salt);
        }
    }

    /// Writes zeros to `channel` from `from` to `to`, so the file has the blocks of its full length.
    private static void fill(FileChannel channel, long from, long to) throws IOException {
        var zeros = ByteBuffer.allocate(1 << 20);
        var position = from;
        while (position < to) {
            zeros.clear().limit((int) Math.min(zeros.capacity(), to - position));
            while (zeros.hasRemaining()) {
                position += channel.write(zeros, position);
            }
        }
    }

    /// Turns segment `number`, which holds no frame, into a spare.
    void spare(long number) throws IOException {
        synchronized (lock) {
            var path = segment(number);
            if (Files.exists(path)) {
                Files.move(path, directory.resolve("%016x".formatted(number) + SPARE), StandardCopyOption.ATOMIC_MOVE);
            }
        }
    }

    /// Turns the segments numbered below `first`, which a durable checkpoint made obsolete, into spares until there
    /// are `keep`, and deletes the others and any spare past `keep`.
    void retireBefore(long first, int keep) throws IOException {
        synchronized (lock) {
            var spares = spares();
            for (var number : segments()) {
                if (number >= first) {
                    break;
                }
                var path = segment(number);
                if (spares.size() < keep) {
                    var spare = directory.resolve("%016x".formatted(number) + SPARE);
                    Files.move(path, spare, StandardCopyOption.ATOMIC_MOVE);
                    spares.add(spare);
                } else {
                    Files.deleteIfExists(path);
                }
            }
            for (var i = keep; i < spares.size(); i++) {
                Files.deleteIfExists(spares.get(i));
            }
        }
    }

    /// Deletes the files a preparation left when a crash cut it short.
    void deleteStale() throws IOException {
        synchronized (lock) {
            if (!Files.isDirectory(directory)) {
                return;
            }
            try (var files = Files.list(directory)) {
                for (var file : (Iterable<Path>) files::iterator) {
                    if (file.getFileName().toString().endsWith(PREPARING)) {
                        Files.deleteIfExists(file);
                    }
                }
            }
        }
    }
}

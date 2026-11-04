package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerKeyException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.zip.CRC32C;

/// The header of a store: what every open needs before it reads the log.
///
/// The file `ledger.header` is 128 bytes, big-endian:
///
/// ```text
///   0  magic "LEDGER01"
///   8  format version, 4 bytes
///  12  encryption: 0 none, 1 raw key, 2 passphrase
///  16  PBKDF2 iterations, 4 bytes
///  24  store id, 16 random bytes
///  40  salt, 32 random bytes
///  72  key check, 32 bytes (zero without encryption)
/// 104  CRC-32C of bytes 0 to 103
/// ```
///
/// It is written once, when the store is created, through a temporary file moved into place.
///
/// @param encryption the kind of encryption
/// @param iterations the PBKDF2 iterations of a passphrase
/// @param storeId the random identity of the store, which binds its segments to it
/// @param salt the random salt of the key derivation
/// @param check the key check
record StoreHeader(EncryptionSpec.Kind encryption, int iterations, byte[] storeId, byte[] salt, byte[] check) {
    static final String FILE = "ledger.header";
    static final int VERSION = 1;
    private static final byte[] MAGIC = "LEDGER01".getBytes(StandardCharsets.US_ASCII);
    private static final int SIZE = 128;
    private static final int CRC_OFFSET = 104;

    /// Reads the header of the store in `directory`, or creates it if the store is new, and derives the store's keys.
    ///
    /// @throws IOException if the header cannot be read or written
    /// @throws LedgerKeyException if `encryption` is not the encryption the store was created with
    /// @throws LedgerCorruptedException if the header is damaged
    static Opened openOrCreate(Path directory, EncryptionSpec encryption) throws IOException {
        var header = read(directory);
        if (header != null) {
            return new Opened(header, header.open(encryption));
        }
        return create(directory, encryption);
    }

    /// A header and the keys derived from it.
    ///
    /// @param header the header
    /// @param keys the store's keys
    record Opened(StoreHeader header, StoreKeys keys) {
    }

    private static Opened create(Path directory, EncryptionSpec encryption) throws IOException {
        SecureRandom random;
        try {
            random = SecureRandom.getInstanceStrong();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("No strong random source", exception);
        }
        var storeId = new byte[16];
        var salt = new byte[32];
        random.nextBytes(storeId);
        random.nextBytes(salt);
        var iterations = encryption.kind() == EncryptionSpec.Kind.PASSPHRASE ? StoreKeys.PASSPHRASE_ITERATIONS : 0;
        var keys = StoreKeys.derive(encryption, salt, iterations);
        var header = new StoreHeader(encryption.kind(), iterations, storeId, salt, keys.check());
        header.write(directory);
        return new Opened(header, keys);
    }

    /// {@return the header of the store in `directory`, or `null` if the store has none yet}
    ///
    /// @throws IOException if the header cannot be read
    /// @throws LedgerCorruptedException if the header is damaged or of an unknown format
    static StoreHeader read(Path directory) throws IOException {
        var file = directory.resolve(FILE);
        if (!Files.exists(file)) {
            return null;
        }
        var bytes = Files.readAllBytes(file);
        if (bytes.length != SIZE) {
            throw new LedgerCorruptedException(file + " is " + bytes.length + " bytes, not " + SIZE, null);
        }
        var buffer = ByteBuffer.wrap(bytes);
        var crc = new CRC32C();
        crc.update(bytes, 0, CRC_OFFSET);
        if ((int) crc.getValue() != buffer.getInt(CRC_OFFSET) || !ByteBuffer.wrap(bytes, 0, 8).equals(ByteBuffer.wrap(MAGIC))) {
            throw new LedgerCorruptedException(file + " is damaged", null);
        }
        var version = buffer.getInt(8);
        if (version != VERSION) {
            throw new LedgerCorruptedException(file + " has format version " + version + ", this Ledger reads " + VERSION, null);
        }
        var kind = switch (buffer.get(12)) {
            case 0 -> EncryptionSpec.Kind.NONE;
            case 1 -> EncryptionSpec.Kind.KEY;
            case 2 -> EncryptionSpec.Kind.PASSPHRASE;
            default -> throw new LedgerCorruptedException(file + " names an unknown encryption " + buffer.get(12), null);
        };
        var storeId = new byte[16];
        buffer.get(24, storeId);
        var salt = new byte[32];
        buffer.get(40, salt);
        var check = new byte[32];
        buffer.get(72, check);
        return new StoreHeader(kind, buffer.getInt(16), storeId, salt, check);
    }

    /// {@return the keys of this store under `encryption`}
    ///
    /// @throws LedgerKeyException if `encryption` is not the encryption the store was created with
    StoreKeys open(EncryptionSpec encryption) {
        if (encryption.kind() != this.encryption) {
            throw new LedgerKeyException("The store was created with " + describe(this.encryption) + ", not " + describe(encryption.kind()));
        }
        var keys = StoreKeys.derive(encryption, salt, iterations);
        keys.verify(check);
        return keys;
    }

    private static String describe(EncryptionSpec.Kind kind) {
        return switch (kind) {
            case NONE -> "no encryption";
            case KEY -> "a raw key";
            case PASSPHRASE -> "a passphrase";
        };
    }

    private void write(Path directory) throws IOException {
        var buffer = ByteBuffer.allocate(SIZE);
        buffer.put(0, MAGIC);
        buffer.putInt(8, VERSION);
        buffer.put(12, (byte) switch (encryption) {
            case NONE -> 0;
            case KEY -> 1;
            case PASSPHRASE -> 2;
        });
        buffer.putInt(16, iterations);
        buffer.put(24, storeId);
        buffer.put(40, salt);
        buffer.put(72, check);
        var crc = new CRC32C();
        crc.update(buffer.array(), 0, CRC_OFFSET);
        buffer.putInt(CRC_OFFSET, (int) crc.getValue());
        var temporary = directory.resolve(FILE + ".tmp");
        try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            FullSync.force(channel, temporary, true);
        }
        Files.move(temporary, directory.resolve(FILE), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Durability.forceDirectory(directory);
    }
}

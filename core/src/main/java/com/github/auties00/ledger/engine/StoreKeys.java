package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerKeyException;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Arrays;

/// The keys of an encrypting store, derived from its master key with HKDF-SHA256 (RFC 5869).
///
/// The master key is the caller's raw key, or the output of PBKDF2-HMAC-SHA256 over a passphrase. HKDF extracts a
/// pseudorandom key from it with the store's salt, and expands one key per use: the check the header keeps to tell a
/// wrong key from a damaged file, one AES-256 key per log segment (from its number and a random salt of its own), the
/// AES-256 key of the data file's units, and that of the units of the file of large values. A store without encryption
/// has no keys.
final class StoreKeys {
    /// The PBKDF2 iterations of a passphrase.
    static final int PASSPHRASE_ITERATIONS = 600_000;
    private static final byte[] CHECK_INFO = "ledger key check".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SEGMENT_INFO = "ledger log segment".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PAGE_INFO = "ledger data file".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] BLOB_INFO = "ledger blob file".getBytes(StandardCharsets.US_ASCII);

    private final byte[] pseudorandomKey;

    private StoreKeys(byte[] pseudorandomKey) {
        this.pseudorandomKey = pseudorandomKey;
    }

    /// {@return the keys of `encryption` with `salt`, or keys that encrypt nothing for no encryption}
    ///
    /// @param iterations the PBKDF2 iterations of a passphrase
    static StoreKeys derive(EncryptionSpec encryption, byte[] salt, int iterations) {
        var master = switch (encryption.kind()) {
            case NONE -> null;
            case KEY -> encryption.key();
            case PASSPHRASE -> pbkdf2(encryption.passphrase(), salt, iterations);
        };
        if (master == null) {
            return new StoreKeys(null);
        }
        try {
            return new StoreKeys(hmac(salt, master));
        } finally {
            if (encryption.kind() == EncryptionSpec.Kind.PASSPHRASE) {
                Arrays.fill(master, (byte) 0);
            }
        }
    }

    private static byte[] pbkdf2(char[] passphrase, byte[] salt, int iterations) {
        var spec = new PBEKeySpec(passphrase, salt, iterations, 256);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("PBKDF2-HMAC-SHA256 is unavailable", exception);
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] hmac(byte[] key, byte[]... parts) {
        try {
            var mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            for (var part : parts) {
                mac.update(part);
            }
            return mac.doFinal();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", exception);
        }
    }

    /// {@return the first 32 bytes HKDF-Expand produces from `info`}
    private byte[] expand(byte[]... info) {
        var parts = Arrays.copyOf(info, info.length + 1);
        parts[info.length] = new byte[]{1};
        return hmac(pseudorandomKey, parts);
    }

    /// {@return whether the store encrypts}
    boolean encrypted() {
        return pseudorandomKey != null;
    }

    /// {@return the value the header keeps to recognize these keys, or 32 zero bytes without encryption}
    byte[] check() {
        return encrypted() ? expand(CHECK_INFO) : new byte[32];
    }

    /// Throws unless `expected`, a header's check, is the check of these keys.
    ///
    /// @throws LedgerKeyException if the check differs
    void verify(byte[] expected) {
        if (!MessageDigest.isEqual(check(), expected)) {
            throw new LedgerKeyException("The key or passphrase is not the one the store was created with");
        }
    }

    /// {@return the AES-256 key of the units of the data file, or `null` without encryption}
    SecretKeySpec pageKey() {
        return encrypted() ? new SecretKeySpec(expand(PAGE_INFO), "AES") : null;
    }

    /// {@return the AES-256 key of the units of the file of large values, or `null` without encryption}
    SecretKeySpec blobKey() {
        return encrypted() ? new SecretKeySpec(expand(BLOB_INFO), "AES") : null;
    }

    /// {@return the AES-256 key of log segment `number` of the store `storeId`, whose header holds `salt`}
    SecretKeySpec segmentKey(byte[] storeId, long number, byte[] salt) {
        return new SecretKeySpec(expand(SEGMENT_INFO, storeId, ByteBuffer.allocate(Long.BYTES).putLong(number).array(), salt), "AES");
    }
}

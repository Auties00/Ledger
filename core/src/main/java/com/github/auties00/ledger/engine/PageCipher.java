package com.github.auties00.ledger.engine;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.CRC32C;

/// Seals and opens the units of the data file, as [PageFormat] lays them out: AES-256-GCM under the store's page key,
/// or a CRC-32C for a store without encryption.
///
/// It is safe for concurrent use: each call takes a cipher from a pool, since a cipher is not.
final class PageCipher {
    private final SecretKeySpec key;
    private final ConcurrentLinkedQueue<Cipher> ciphers;

    /// Creates the cipher of a store that encrypts under `key`, or of one that does not if `key` is `null`.
    PageCipher(SecretKeySpec key) {
        this.key = key;
        this.ciphers = new ConcurrentLinkedQueue<>();
    }

    /// Seals the unit of `pages` pages in `plain`, whose header and content are filled in, into `out` at `offset`.
    void seal(byte[] plain, int pages, byte[] out, int offset) {
        var length = pages * PageFormat.SIZE;
        var tag = length - PageFormat.TAG;
        System.arraycopy(plain, 0, out, offset, PageFormat.HEADER);
        if (key == null) {
            System.arraycopy(plain, PageFormat.HEADER, out, offset + PageFormat.HEADER, tag - PageFormat.HEADER);
            var crc = new CRC32C();
            crc.update(out, offset, tag);
            Bytes.i32(out, offset + tag, (int) crc.getValue());
            Arrays.fill(out, offset + tag + Integer.BYTES, offset + length, (byte) 0);
            return;
        }
        var cipher = take();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(PageFormat.TAG * 8, plain, PageFormat.NONCE, 12));
            cipher.updateAAD(plain, 0, PageFormat.HEADER);
            cipher.doFinal(plain, PageFormat.HEADER, tag - PageFormat.HEADER, out, offset + PageFormat.HEADER);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("A unit could not be sealed", exception);
        } finally {
            ciphers.offer(cipher);
        }
    }

    /// Opens the unit of `pages` pages sealed in `sealed` at `offset` into `plain`, header included.
    ///
    /// @return whether it authenticates
    boolean open(byte[] sealed, int offset, int pages, byte[] plain) {
        var length = pages * PageFormat.SIZE;
        var tag = length - PageFormat.TAG;
        System.arraycopy(sealed, offset, plain, 0, PageFormat.HEADER);
        if (key == null) {
            var crc = new CRC32C();
            crc.update(sealed, offset, tag);
            if (Bytes.i32(sealed, offset + tag) != (int) crc.getValue()) {
                return false;
            }
            System.arraycopy(sealed, offset + PageFormat.HEADER, plain, PageFormat.HEADER, tag - PageFormat.HEADER);
            return true;
        }
        var cipher = take();
        try {
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(PageFormat.TAG * 8, sealed, offset + PageFormat.NONCE, 12));
            cipher.updateAAD(sealed, offset, PageFormat.HEADER);
            cipher.doFinal(sealed, offset + PageFormat.HEADER, length - PageFormat.HEADER, plain, PageFormat.HEADER);
            return true;
        } catch (AEADBadTagException _) {
            return false;
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("A unit could not be opened", exception);
        } finally {
            ciphers.offer(cipher);
        }
    }

    private Cipher take() {
        var cipher = ciphers.poll();
        if (cipher != null) {
            return cipher;
        }
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("AES-GCM is unavailable", exception);
        }
    }
}

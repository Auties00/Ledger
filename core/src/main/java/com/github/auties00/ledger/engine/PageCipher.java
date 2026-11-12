package com.github.auties00.ledger.engine;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.CRC32C;

/// Seals and opens the units of the store's files, as [PageFormat] lays them out: AES-256-GCM under the key of the
/// file, or a CRC-32C for a store without encryption.
///
/// It is safe for concurrent use: each call takes a cipher from a pool, since a cipher is not. It is left open to
/// subclasses only for the tests that watch which threads seal.
class PageCipher {
    /// The content a streamed unit passes to the cipher in one call: small enough to stay out of the way of the
    /// cipher's intrinsic, which a cold JVM reaches only through calls that are not too large.
    private static final int SLICE = 64 << 10;

    private final SecretKeySpec key;
    private final ConcurrentLinkedQueue<Cipher> ciphers;

    /// Creates the cipher of a store that encrypts under `key`, or of one that does not if `key` is `null`.
    PageCipher(SecretKeySpec key) {
        this.key = key;
        this.ciphers = new ConcurrentLinkedQueue<>();
    }

    /// Seals the unit of `pages` pages in `plain`, whose header and content are filled in, into `out` at `offset`.
    ///
    /// `plain` may end anywhere from the end of its content to the end of the unit: the bytes it lacks before the tag
    /// seal as zeros, so the unit is the one the whole pages, zero past the content, make.
    ///
    /// @throws IllegalArgumentException if `plain` ends before its content or past the unit, or the content does not
    ///                                  fit in the unit
    void seal(byte[] plain, int pages, byte[] out, int offset) {
        var length = pages * PageFormat.SIZE;
        var tag = length - PageFormat.TAG;
        var content = Bytes.i32(plain, PageFormat.LENGTH);
        if (!PageFormat.fits(pages, content) || plain.length < PageFormat.HEADER + content || plain.length > length) {
            throw new IllegalArgumentException("A unit of " + pages + " pages cannot hold " + content + " bytes of content in "
                    + plain.length + " bytes");
        }
        // The bytes of plain sealed as they are; zeros follow up to the tag
        var filled = Math.min(plain.length, tag);
        System.arraycopy(plain, 0, out, offset, PageFormat.HEADER);
        if (key == null) {
            System.arraycopy(plain, PageFormat.HEADER, out, offset + PageFormat.HEADER, filled - PageFormat.HEADER);
            // The arrays of the pool come back with the bytes of their last use
            Arrays.fill(out, offset + filled, offset + tag, (byte) 0);
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
            Sealing.encrypt(cipher, plain, PageFormat.HEADER, filled - PageFormat.HEADER, tag - filled, out, offset + PageFormat.HEADER);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("A unit could not be sealed", exception);
        } finally {
            ciphers.offer(cipher);
        }
    }

    /// Seals in place the unit of `pages` pages at the start of `unit`, whose header, content and zero padding are
    /// filled in up to its tag.
    ///
    /// The unit is the one [#seal(byte\[\], int, byte\[\], int)] makes of the same header and content: the cipher
    /// encrypts each slice over itself, which AES-GCM does without a copy when the output starts where the input does.
    /// The bytes of `unit` past the unit are left as they are.
    ///
    /// @throws IllegalArgumentException if `unit` is shorter than the unit, or the content its header names does not
    ///                                  fit in the unit
    void seal(byte[] unit, int pages) {
        var length = pages * PageFormat.SIZE;
        var tag = length - PageFormat.TAG;
        var content = Bytes.i32(unit, PageFormat.LENGTH);
        if (!PageFormat.fits(pages, content) || unit.length < length) {
            throw new IllegalArgumentException("A unit of " + pages + " pages cannot hold " + content + " bytes of content in "
                    + unit.length + " bytes");
        }
        if (key == null) {
            var crc = new CRC32C();
            crc.update(unit, 0, tag);
            Bytes.i32(unit, tag, (int) crc.getValue());
            Arrays.fill(unit, tag + Integer.BYTES, length, (byte) 0);
            return;
        }
        var cipher = take();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(PageFormat.TAG * 8, unit, PageFormat.NONCE, 12));
            cipher.updateAAD(unit, 0, PageFormat.HEADER);
            // Slices of whole blocks, each written over itself; were the cipher to hold some back, its output would
            // only trail its input, which it encrypts in place as well
            var consumed = PageFormat.HEADER;
            var written = PageFormat.HEADER;
            while (tag - consumed > SLICE) {
                written += cipher.update(unit, consumed, SLICE, unit, written);
                consumed += SLICE;
            }
            cipher.doFinal(unit, consumed, tag - consumed, unit, written);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("A unit could not be sealed", exception);
        } finally {
            ciphers.offer(cipher);
        }
    }

    /// Receives the bytes of a unit sealed a part at a time.
    @FunctionalInterface
    interface Output {
        /// Takes the first `length` bytes of `bytes`, which the cipher fills again once it returns.
        void write(byte[] bytes, int length) throws IOException;
    }

    /// Seals the unit of `pages` pages whose header is `header` and whose content is the `contentLength` bytes of
    /// `content` at `contentOffset`, passing it to `out` a part at a time through `buffer`.
    ///
    /// The unit is the one [#seal(byte\[\], int, byte\[\], int)] makes of the same header and content, but neither is
    /// whole in memory: a value of megabytes is sealed through a buffer of its own size. The cipher reads each whole
    /// slice of the content where it is; only the last part of the content and the padding after it are copied.
    ///
    /// @param header the unit's header, filled in, of [PageFormat#HEADER] bytes
    /// @param buffer the buffer the sealed bytes pass through, of at least 64 KiB
    /// @throws IOException if `out` fails
    void seal(byte[] header, byte[] content, int contentOffset, int contentLength, int pages, byte[] buffer, Output out) throws IOException {
        var tag = pages * PageFormat.SIZE - PageFormat.TAG;
        var padding = tag - PageFormat.HEADER - (long) contentLength;
        if (padding < 0) {
            throw new IllegalArgumentException("A unit of " + pages + " pages holds at most " + PageFormat.capacity(pages) + " bytes");
        }
        System.arraycopy(header, 0, buffer, 0, PageFormat.HEADER);
        var used = PageFormat.HEADER;
        var total = contentLength + padding;
        if (key == null) {
            var crc = new CRC32C();
            crc.update(header, 0, PageFormat.HEADER);
            for (var offset = 0L; offset < total; ) {
                if (used == buffer.length) {
                    out.write(buffer, used);
                    used = 0;
                }
                var count = (int) Math.min(buffer.length - used, total - offset);
                var fromContent = (int) Math.max(0, Math.min(count, contentLength - offset));
                if (fromContent > 0) {
                    System.arraycopy(content, contentOffset + (int) offset, buffer, used, fromContent);
                }
                Arrays.fill(buffer, used + fromContent, used + count, (byte) 0);
                crc.update(buffer, used, count);
                used += count;
                offset += count;
            }
            if (buffer.length - used < PageFormat.TAG) {
                out.write(buffer, used);
                used = 0;
            }
            Bytes.i32(buffer, used, (int) crc.getValue());
            Arrays.fill(buffer, used + Integer.BYTES, used + PageFormat.TAG, (byte) 0);
            out.write(buffer, used + PageFormat.TAG);
            return;
        }
        var cipher = take();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(PageFormat.TAG * 8, header, PageFormat.NONCE, 12));
            cipher.updateAAD(header, 0, PageFormat.HEADER);
            var sliceLength = Math.min(SLICE, buffer.length / 2);
            byte[] tail = null;
            for (var offset = 0L; offset < total; ) {
                var count = (int) Math.min(sliceLength, total - offset);
                // The cipher may hold back a partial block, and gives it out with the next call
                if (buffer.length - used < count + PageFormat.TAG) {
                    out.write(buffer, used);
                    used = 0;
                }
                if (offset + count <= contentLength) {
                    used += cipher.update(content, contentOffset + (int) offset, count, buffer, used);
                } else {
                    if (tail == null) {
                        tail = new byte[count];
                    }
                    var fromContent = (int) Math.max(0, contentLength - offset);
                    if (fromContent > 0) {
                        System.arraycopy(content, contentOffset + (int) offset, tail, 0, fromContent);
                    }
                    Arrays.fill(tail, fromContent, count, (byte) 0);
                    used += cipher.update(tail, 0, count, buffer, used);
                }
                offset += count;
            }
            if (buffer.length - used < 2 * PageFormat.TAG) {
                out.write(buffer, used);
                used = 0;
            }
            used += cipher.doFinal(buffer, used);
            out.write(buffer, used);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("A unit could not be sealed", exception);
        } finally {
            ciphers.offer(cipher);
        }
    }

    /// Opens the unit of `pages` pages sealed in `sealed` at `offset` into `plain`, header included.
    ///
    /// @param plain the array the plaintext goes to, of at least the unit's bytes before its tag
    /// @return whether it authenticates
    boolean open(byte[] sealed, int offset, int pages, byte[] plain) {
        var length = pages * PageFormat.SIZE;
        var tag = length - PageFormat.TAG;
        System.arraycopy(sealed, offset, plain, 0, PageFormat.HEADER);
        if (key == null) {
            if (!checksumMatches(sealed, offset, tag)) {
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

    /// {@return the first `keep` bytes of the plaintext of the unit of `pages` pages sealed in `sealed` at `offset`,
    /// header included, in an array of their own, or `null` if the unit fails authentication}
    ///
    /// AES-GCM gives out no plaintext before it has authenticated the whole unit, so a unit kept short of its pages is
    /// opened into an array of `pool` first, which goes back to it, and copied out of it: the copy fills an array the
    /// JVM need not zero first, where a whole unit's array is zeroed and then opened into. A whole unit is opened
    /// straight into its array. Without encryption the bytes before the tag are the plaintext, and are copied out of
    /// `sealed` once their checksum matches.
    ///
    /// @param keep the bytes to keep: the whole unit, or from its header to its tag
    /// @throws IllegalArgumentException if `keep` is out of that range
    byte[] open(byte[] sealed, int offset, int pages, int keep, HeapPool pool) {
        var length = pages * PageFormat.SIZE;
        if (keep == length) {
            var plain = new byte[length];
            return open(sealed, offset, pages, plain) ? plain : null;
        }
        var tag = length - PageFormat.TAG;
        if (keep < PageFormat.HEADER || keep > tag) {
            throw new IllegalArgumentException("A unit of " + pages + " pages cannot be kept to " + keep + " bytes");
        }
        if (key == null) {
            return checksumMatches(sealed, offset, tag) ? Arrays.copyOfRange(sealed, offset, offset + keep) : null;
        }
        var scratch = pool.acquire(tag);
        try {
            return open(sealed, offset, pages, scratch) ? Arrays.copyOf(scratch, keep) : null;
        } finally {
            pool.release(scratch);
        }
    }

    /// {@return whether the CRC-32C after the first `tag` bytes of the unit sealed in `sealed` at `offset`, which does
    /// not encrypt, matches them}
    private static boolean checksumMatches(byte[] sealed, int offset, int tag) {
        var crc = new CRC32C();
        crc.update(sealed, offset, tag);
        return Bytes.i32(sealed, offset + tag) == (int) crc.getValue();
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

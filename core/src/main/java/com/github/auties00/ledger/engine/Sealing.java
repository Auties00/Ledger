package com.github.auties00.ledger.engine;

import javax.crypto.Cipher;
import java.security.GeneralSecurityException;

/// Encryption with AES-GCM in slices.
final class Sealing {
    /// The bytes one call to the cipher encrypts: a multiple of the AES block, so no slice leaves a partial block.
    private static final int SLICE = 64 << 10;
    /// The zeros a unit's padding is encrypted from, which nothing writes.
    private static final byte[] ZEROS = new byte[PageFormat.SIZE];

    private Sealing() {
    }

    /// Encrypts the `length` bytes of `input` at `inputOffset`, then `padding` zero bytes, into `output` at
    /// `outputOffset`, then appends the tag, through `cipher`, which is initialized for encryption and has its
    /// additional data.
    ///
    /// The result is the one GCM message a single `doFinal` would give of the input followed by the zeros. It is fed
    /// to the cipher a slice at a time because the JDK reaches its AES-GCM intrinsics only from code the JIT compiled
    /// with the second compiler, which counts calls: sealing a few megabytes in one call runs the slow path every time
    /// until many calls made the JIT compile it, 40 times slower on AVX2. The zeros come from an array of this class, so
    /// a unit built only as long as its content seals as the whole unit would.
    ///
    /// @return the bytes written, ciphertext and tag
    static int encrypt(Cipher cipher, byte[] input, int inputOffset, int length, int padding, byte[] output, int outputOffset)
            throws GeneralSecurityException {
        var written = 0;
        var consumed = 0;
        while (length - consumed > SLICE) {
            written += cipher.update(input, inputOffset + consumed, SLICE, output, outputOffset + written);
            consumed += SLICE;
        }
        if (padding == 0) {
            return written + cipher.doFinal(input, inputOffset + consumed, length - consumed, output, outputOffset + written);
        }
        // The cipher may hold back a partial block, and gives it out with the next call
        written += cipher.update(input, inputOffset + consumed, length - consumed, output, outputOffset + written);
        var left = padding;
        while (left > ZEROS.length) {
            written += cipher.update(ZEROS, 0, ZEROS.length, output, outputOffset + written);
            left -= ZEROS.length;
        }
        return written + cipher.doFinal(ZEROS, 0, left, output, outputOffset + written);
    }
}

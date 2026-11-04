package com.github.auties00.ledger.engine;

import javax.crypto.Cipher;
import java.security.GeneralSecurityException;

/// Encryption with AES-GCM in slices.
final class Sealing {
    /// The bytes one call to the cipher encrypts: a multiple of the AES block, so no call leaves a partial block.
    private static final int SLICE = 64 << 10;

    private Sealing() {
    }

    /// Encrypts the `length` bytes of `input` at `inputOffset` into `output` at `outputOffset`, then appends the tag,
    /// through `cipher`, which is initialized for encryption and has its additional data.
    ///
    /// The result is the one GCM message a single `doFinal` would give. It is fed to the cipher a slice at a time
    /// because the JDK reaches its AES-GCM intrinsics only from code the JIT compiled with the second compiler, which
    /// counts calls: sealing a few megabytes in one call runs the slow path every time until many calls made the JIT
    /// compile it, 40 times slower on AVX2.
    ///
    /// @return the bytes written, ciphertext and tag
    static int encrypt(Cipher cipher, byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
            throws GeneralSecurityException {
        var written = 0;
        var consumed = 0;
        while (length - consumed > SLICE) {
            written += cipher.update(input, inputOffset + consumed, SLICE, output, outputOffset + written);
            consumed += SLICE;
        }
        return written + cipher.doFinal(input, inputOffset + consumed, length - consumed, output, outputOffset + written);
    }
}

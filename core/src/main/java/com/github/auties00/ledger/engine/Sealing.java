package com.github.auties00.ledger.engine;

import javax.crypto.Cipher;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicLong;

/// Encryption with AES-GCM in slices.
///
/// The JDK reaches its AES-GCM intrinsics only from code the second compiler of the JIT compiled, which it compiles
/// once a method was called about 5,000 times: its GHASH, its counter mode and the methods that call them. Slices of
/// 64 KiB call each about six times, so a process whose log seals large frames from its start sealed the first 50 to
/// 100 MiB of them in the JDK's Java code, a few hundred megabytes a second, where the intrinsics take gigabytes. The
/// first [#COLD_BYTES] of the log's frames the process seals therefore go to the cipher in slices of [#COLD_SLICE]
/// ([#slice(int)]), one call each, so those methods reach the second compiler within them; the slices after are the
/// log's own.
///
/// Only the log takes them: its frames are many messages, each initialized, given its header and finished, so the JIT
/// compiles those methods from profiles that saw messages start and end. A unit of a file is one message of up to
/// hundreds of megabytes, so the short slices of the first would have the JIT compile the cipher from calls that never
/// finished a message, and the first message to end, and every later one, would deoptimize it, which left its methods
/// in code of the first compiler for the rest of a process that wrote values of 16 MiB.
final class Sealing {
    /// The bytes one call to the cipher encrypts: a multiple of the AES block, so no slice leaves a partial block.
    private static final int SLICE = 64 << 10;
    /// The bytes of a slice while the process has sealed fewer than [#COLD_BYTES] in such slices: a multiple of the
    /// AES block, at least the 512 bytes below which the JDK's combined GCM intrinsic does not run, and short enough
    /// that 16,384 calls pass through the cipher's methods in the budget, where its methods are compiled after about
    /// 5,000, more while the compiler's queue is long, as it is as a JVM starts.
    static final int COLD_SLICE = 1 << 10;
    /// The bytes of frames the process seals in slices of [#COLD_SLICE]: at a few hundred megabytes a second before the
    /// compilation, a call costs less than a tenth of the bytes it seals, and after it, the 16 MiB cost a few
    /// milliseconds more than slices of 64 KiB.
    static final long COLD_BYTES = 16L << 20;
    // The bytes of the budget no slice took yet; below zero once it is spent
    private static final AtomicLong COLD_LEFT = new AtomicLong(COLD_BYTES);
    /// The zeros a unit's padding is encrypted from, which nothing writes.
    private static final byte[] ZEROS = new byte[PageFormat.SIZE];

    private Sealing() {
    }

    /// {@return the bytes the next call to the cipher that seals a frame of the log should take, at most `slice`:
    /// [#COLD_SLICE] while the process has sealed fewer than [#COLD_BYTES] bytes in such slices, which it then counts,
    /// else `slice`}
    ///
    /// Both are multiples of the AES block if `slice` is, so a caller that seals in place, whole blocks per call, may
    /// take either. A caller that seals less than the slice in the call takes only part of the budget's bytes, which
    /// only ends the budget sooner.
    static int slice(int slice) {
        if (COLD_LEFT.getPlain() <= 0 || slice <= COLD_SLICE) {
            return slice;
        }
        // Several threads may take the last slices at once, which ends the budget a few slices past it
        COLD_LEFT.getAndAdd(-COLD_SLICE);
        return COLD_SLICE;
    }

    /// {@return whether the process has sealed [#COLD_BYTES] bytes in slices of [#COLD_SLICE]; for tests}
    static boolean coldSpentForTests() {
        return COLD_LEFT.get() <= 0;
    }

    /// Encrypts the `length` bytes of `input` at `inputOffset`, then `padding` zero bytes, into `output` at
    /// `outputOffset`, then appends the tag, through `cipher`, which is initialized for encryption and has its
    /// additional data.
    ///
    /// The result is the one GCM message a single `doFinal` would give of the input followed by the zeros. It is fed
    /// to the cipher a slice at a time because the JDK reaches its AES-GCM intrinsics only from code the JIT compiled
    /// with the second compiler, which counts calls: sealing a few megabytes in one call runs the slow path every time
    /// until many calls made the JIT compile it, 40 times slower on AVX2. The zeros come from an array
    /// of this class, so a unit built only as long as its content seals as the whole unit would.
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

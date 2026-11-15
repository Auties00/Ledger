package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The slices a process seals its first megabytes in, against one call to the cipher.
class SealingTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");

    @Test
    @DisplayName("a message sealed in the short slices of a cold process, and in the slices after, is the one a single call seals")
    void sealsTheSameMessage() throws Exception {
        var random = new Random(7);
        var lengths = new int[]{0, 1, 15, 16, 1_023, 1_024, 1_025, 70_000, 300_013};
        var nonce = 0L;
        // Past the budget, whatever the tests before took of it, so both kinds of slices are compared
        var sealed = 0L;
        while (sealed <= Sealing.COLD_BYTES + (1 << 20)) {
            for (var length : lengths) {
                var input = new byte[length + 5];
                random.nextBytes(input);
                var padding = length % 3 == 0 ? 0 : 4_096 + length % 7;
                var whole = Arrays.copyOf(Arrays.copyOfRange(input, 5, 5 + length), length + padding);
                var expected = cipher(nonce).doFinal(whole);
                var output = new byte[length + padding + 16 + 3];
                var written = Sealing.encrypt(cipher(nonce), input, 5, length, padding, output, 3);
                assertEquals(expected.length, written);
                assertArrayEquals(expected, Arrays.copyOfRange(output, 3, 3 + written));
                nonce++;
                sealed += length + padding;
            }
        }
        assertTrue(Sealing.coldSpentForTests());
        assertEquals(64 << 10, Sealing.slice(64 << 10));
        assertEquals(512, Sealing.slice(512));
    }

    private static Cipher cipher(long nonce) throws Exception {
        var cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, KEY, new GCMParameterSpec(128, ByteBuffer.allocate(12).putLong(4, nonce).array()));
        cipher.updateAAD(new byte[]{1, 2, 3});
        return cipher;
    }
}

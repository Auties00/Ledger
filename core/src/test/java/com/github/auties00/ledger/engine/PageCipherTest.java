package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A unit sealed a part at a time, the cipher reading its content in place, against the same unit sealed whole.
class PageCipherTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");

    static Stream<Arguments> units() {
        var slice = 64 << 10;
        var lengths = new int[]{0, 1, 15, 16, 17, 4_000, PageFormat.capacity(1), PageFormat.capacity(1) + 1, slice - 1, slice, slice + 1,
                2 * slice, 2 * slice + 13, 1 << 20, (1 << 20) + 13, (3 << 20) + 5};
        return Arrays.stream(lengths).boxed().flatMap(length -> Stream.of(true, false).flatMap(encrypted -> Stream.of(1 << 20, 64 << 10)
                .map(buffer -> Arguments.of(length, encrypted, buffer))));
    }

    @ParameterizedTest(name = "{0} bytes, encrypted: {1}, buffer of {2}")
    @MethodSource("units")
    @DisplayName("sealing a part at a time gives the bytes of the whole unit, which opens to its content and fails once changed")
    void sealsTheSameUnit(int length, boolean encrypted, int bufferLength) throws IOException {
        assertSealsTheSameUnit(length, encrypted, bufferLength);
    }

    static Stream<Arguments> unitsAtTheColdEnd() {
        // The content ends in the last short slice before 2 MiB, and the slice after it starts at 2 MiB, past the short
        // slices, with the padding left of the unit
        var lengths = new int[]{(2 << 20) - 47, (2 << 20) - 32, (2 << 20) - 10, (2 << 20) - 1, 2 << 20, (2 << 20) + 1};
        return Arrays.stream(lengths).boxed().flatMap(length -> Stream.of(1 << 20, 64 << 10).map(buffer -> Arguments.of(length, buffer)));
    }

    @ParameterizedTest(name = "{0} bytes, buffer of {1}")
    @MethodSource("unitsAtTheColdEnd")
    @DisplayName("a cold process seals a unit whose content ends in its last short slice as the whole unit")
    void sealsAtTheColdEnd(int length, int bufferLength) throws IOException {
        var before = Sealing.coldLeftForTests(Sealing.COLD_BYTES);
        try {
            assertSealsTheSameUnit(length, true, bufferLength);
        } finally {
            Sealing.coldLeftForTests(before);
        }
    }

    static Stream<Arguments> unitsAtTheBudgetEnd() {
        // Each ends a little past a page, so the padding after the slice that crosses the content's end is longer
        // than a short slice
        var lengths = new int[]{1_500, 4_200, 41_060, (1 << 20) + 700};
        return Arrays.stream(lengths).boxed().flatMap(length -> Stream.of(1 << 20, 64 << 10).map(buffer -> Arguments.of(length, buffer)));
    }

    @ParameterizedTest(name = "{0} bytes, buffer of {1}")
    @MethodSource("unitsAtTheBudgetEnd")
    @DisplayName("a unit whose last short slice, which spends the process's budget, crosses the end of its content seals as the whole unit")
    void sealsAtTheBudgetEnd(int length, int bufferLength) throws IOException {
        // The unit's first slice is 640 KiB when its content and the buffer hold as much, and takes a short slice of
        // the budget all the same
        var first = 640 << 10;
        var slices = length >= first && bufferLength >= first + 2 * PageFormat.TAG
                ? 1 + (length - first) / Sealing.COLD_SLICE + 1
                : length / Sealing.COLD_SLICE + 1;
        var before = Sealing.coldLeftForTests((long) slices * Sealing.COLD_SLICE);
        try {
            assertSealsTheSameUnit(length, true, bufferLength);
            assertTrue(Sealing.coldSpentForTests(), "the unit did not spend the budget");
        } finally {
            Sealing.coldLeftForTests(before);
        }
    }

    private static void assertSealsTheSameUnit(int length, boolean encrypted, int bufferLength) throws IOException {
        var cipher = new PageCipher(encrypted ? KEY : null);
        var pages = PageFormat.pagesFor(length);
        var content = new byte[length + 7];
        new Random(length).nextBytes(content);
        var header = new byte[PageFormat.HEADER];
        PageFile.header(header, PageFormat.EXTERNAL, 0, 5, pages, length, 77L, length);

        var plain = new byte[pages * PageFormat.SIZE];
        System.arraycopy(header, 0, plain, 0, PageFormat.HEADER);
        System.arraycopy(content, 7, plain, PageFormat.HEADER, length);
        var whole = new byte[plain.length];
        // A cipher object refuses to seal twice under one nonce
        new PageCipher(encrypted ? KEY : null).seal(plain, pages, whole, 0);

        var parts = new ByteArrayOutputStream();
        var buffer = new byte[bufferLength];
        // What the buffer held before must not reach the unit
        Arrays.fill(buffer, (byte) 0x5A);
        cipher.seal(header, content, 7, length, pages, buffer, (bytes, count) -> parts.write(bytes, 0, count));
        assertArrayEquals(whole, parts.toByteArray());

        var opened = new byte[plain.length];
        assertTrue(cipher.open(parts.toByteArray(), 0, pages, opened));
        var tag = plain.length - PageFormat.TAG;
        assertArrayEquals(Arrays.copyOf(plain, tag), Arrays.copyOf(opened, tag));
        assertEquals(length, Bytes.i32(opened, PageFormat.LENGTH));

        var tampered = parts.toByteArray();
        tampered[PageFormat.HEADER + length / 2] ^= 1;
        assertFalse(cipher.open(tampered, 0, pages, opened));
    }
}

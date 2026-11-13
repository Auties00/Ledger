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
import java.util.stream.IntStream;
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

    static Stream<Arguments> splitUnits() {
        var slice = 64 << 10;
        var lengths = new int[]{17, 4_000, slice - 1, slice + 1, 2 * slice + 13, 1 << 20, (1 << 20) + 13, (3 << 20) + 5};
        return Arrays.stream(lengths).boxed().flatMap(length -> IntStream.of(0, 1, 15, slice - 3, slice, length / 2 + 7, length - 1, length)
                        .filter(head -> head <= length).distinct().boxed()
                        .flatMap(head -> Stream.of(true, false).flatMap(encrypted -> Stream.of(1 << 20, 64 << 10)
                                .map(buffer -> Arguments.of(length, head, encrypted, buffer)))));
    }

    @ParameterizedTest(name = "{0} bytes split after {1}, encrypted: {2}, buffer of {3}")
    @MethodSource("splitUnits")
    @DisplayName("sealing content on two arrays gives the bytes of the unit its content on one array makes")
    void sealsSplitContent(int length, int head, boolean encrypted, int bufferLength) throws IOException {
        var pages = PageFormat.pagesFor(length);
        var content = new byte[length];
        new Random(length * 31L + head).nextBytes(content);
        var header = new byte[PageFormat.HEADER];
        PageFile.header(header, PageFormat.EXTERNAL, 0, 5, pages, length, 77L, length);

        var whole = new ByteArrayOutputStream();
        new PageCipher(encrypted ? KEY : null).seal(header, content, 0, length, pages, new byte[bufferLength],
                (bytes, count) -> whole.write(bytes, 0, count));

        // The head behind 3 bytes it leaves out, as a value's own array never has them
        var headArray = new byte[head + 3];
        System.arraycopy(content, 0, headArray, 3, head);
        var tail = Arrays.copyOfRange(content, head, length);
        var parts = new ByteArrayOutputStream();
        var buffer = new byte[bufferLength];
        Arrays.fill(buffer, (byte) 0x5A);
        var cipher = new PageCipher(encrypted ? KEY : null);
        cipher.seal(header, headArray, 3, head, tail, pages, buffer, (bytes, count) -> parts.write(bytes, 0, count));
        assertArrayEquals(whole.toByteArray(), parts.toByteArray());

        var opened = new byte[pages * PageFormat.SIZE];
        assertTrue(cipher.open(parts.toByteArray(), 0, pages, opened));
        assertArrayEquals(content, Arrays.copyOfRange(opened, PageFormat.HEADER, PageFormat.HEADER + length));
    }
}

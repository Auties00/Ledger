package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A unit sealed a part at a time, the cipher reading its content in place, against the same unit sealed whole, and the
/// units of the file of large values whose tag follows their content or that are sealed as records, which only the
/// streamed seal makes.
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

    static Stream<Arguments> closeTagUnits() {
        var slice = 64 << 10;
        var first = 640 << 10;
        // capacity(1) leaves no padding, capacity(1) + 1 a page less one byte of it
        var lengths = new int[]{0, 1, 15, 16, 17, 4_000, PageFormat.capacity(1), PageFormat.capacity(1) + 1, slice - 1, slice,
                slice + 1, first - 1, first, first + 13, 1 << 20, (1 << 20) + 13, (3 << 20) + 5};
        return Arrays.stream(lengths).boxed().flatMap(length -> Stream.of(true, false).flatMap(cold -> Stream.of(1 << 20, 64 << 10)
                .map(buffer -> Arguments.of(length, cold, buffer))));
    }

    @ParameterizedTest(name = "{0} bytes, cold: {1}, buffer of {2}")
    @MethodSource("closeTagUnits")
    @DisplayName("a unit whose tag follows its content seals the content alone, the tag after it and zeros to the end of its pages, opens to its content alone and fails once its header, content or tag changes")
    void sealsTheTagAfterTheContent(int length, boolean cold, int bufferLength) throws IOException {
        var before = cold ? Sealing.coldLeftForTests(Sealing.COLD_BYTES) : -1;
        try {
            var cipher = new PageCipher(KEY);
            var pages = PageFormat.pagesFor(length);
            var content = new byte[length + 7];
            new Random(length).nextBytes(content);
            var header = new byte[PageFormat.HEADER];
            PageFile.header(header, PageFormat.EXTERNAL, 0, 5, pages, length, 77L, length);
            header[PageFormat.FORMAT] = PageFormat.CLOSE_TAG;

            var parts = new ByteArrayOutputStream();
            var buffer = new byte[bufferLength];
            // What the buffer held before must not reach the unit
            Arrays.fill(buffer, (byte) 0x5A);
            cipher.seal(header, content, 7, length, pages, buffer, (bytes, count) -> parts.write(bytes, 0, count));
            var unit = parts.toByteArray();
            assertEquals(pages * PageFormat.SIZE, unit.length);
            assertArrayEquals(header, Arrays.copyOf(unit, PageFormat.HEADER));
            var end = PageFormat.HEADER + length + PageFormat.TAG;
            assertArrayEquals(new byte[unit.length - end], Arrays.copyOfRange(unit, end, unit.length), "a byte after the tag");

            var value = new byte[length];
            assertTrue(cipher.openContent(unit, 0, length, value, 0));
            assertArrayEquals(Arrays.copyOfRange(content, 7, 7 + length), value);
            // At an offset, and into an array at an offset, as the readers of the file open it
            var shifted = new byte[unit.length + 100];
            System.arraycopy(unit, 0, shifted, 100, unit.length);
            var plain = new byte[PageFormat.HEADER + length];
            assertTrue(cipher.openContent(shifted, 100, length, plain, PageFormat.HEADER));
            assertArrayEquals(value, Arrays.copyOfRange(plain, PageFormat.HEADER, plain.length));
            // The zeros after the tag are not authenticated
            if (end < unit.length) {
                var trailing = unit.clone();
                trailing[unit.length - 1] ^= 1;
                trailing[end] ^= 1;
                assertTrue(cipher.openContent(trailing, 0, length, new byte[length], 0));
            }
            // Neither the whole-unit open nor the seals take a unit of this format
            assertFalse(cipher.open(unit, 0, pages, new byte[unit.length]));
            assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).seal(unit.clone(), pages));
            assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).seal(Arrays.copyOf(unit, end - PageFormat.TAG), pages, new byte[unit.length], 0));

            var damaged = new ArrayList<Integer>(List.of(3, PageFormat.LENGTH + 3, PageFormat.HEADER + length, PageFormat.HEADER + length + PageFormat.TAG - 1));
            if (length > 0) {
                damaged.add(PageFormat.HEADER + length / 2);
            }
            for (var at : damaged) {
                var tampered = unit.clone();
                tampered[at] ^= 1;
                assertFalse(cipher.openContent(tampered, 0, length, new byte[length], 0), "a change at " + at);
            }
            // The other format's byte: its readers look for the tag at the end of the pages, where zeros are
            var otherFormat = unit.clone();
            otherFormat[PageFormat.FORMAT] = PageFormat.VERSION;
            assertFalse(cipher.open(otherFormat, 0, pages, new byte[unit.length]));
            assertFalse(cipher.openContent(otherFormat, 0, length, new byte[length], 0));
            // The tag moved to the end of the pages, under either format byte
            if (end < unit.length) {
                var moved = unit.clone();
                System.arraycopy(unit, PageFormat.HEADER + length, moved, unit.length - PageFormat.TAG, PageFormat.TAG);
                Arrays.fill(moved, PageFormat.HEADER + length, PageFormat.HEADER + length + PageFormat.TAG, (byte) 0);
                assertFalse(cipher.openContent(moved, 0, length, new byte[length], 0));
                moved[PageFormat.FORMAT] = PageFormat.VERSION;
                assertFalse(cipher.open(moved, 0, pages, new byte[unit.length]));
            }
            // A unit of the first format sealed of the same header does not open as one whose tag follows its content
            var firstHeader = header.clone();
            firstHeader[PageFormat.FORMAT] = PageFormat.VERSION;
            var firstParts = new ByteArrayOutputStream();
            new PageCipher(KEY).seal(firstHeader, content, 7, length, pages, buffer, (bytes, count) -> firstParts.write(bytes, 0, count));
            var firstUnit = firstParts.toByteArray();
            assertTrue(cipher.open(firstUnit, 0, pages, new byte[firstUnit.length]));
            firstUnit[PageFormat.FORMAT] = PageFormat.CLOSE_TAG;
            assertFalse(cipher.openContent(firstUnit, 0, length, new byte[length], 0));
        } finally {
            if (cold) {
                Sealing.coldLeftForTests(before);
            }
        }
    }

    @Test
    @DisplayName("a store without encryption refuses to seal a unit whose tag follows its content, and a unit never opens over itself")
    void closeTagNeedsEncryption() {
        var header = new byte[PageFormat.HEADER];
        PageFile.header(header, PageFormat.EXTERNAL, 0, 5, 1, 10, 77L, 10);
        header[PageFormat.FORMAT] = PageFormat.CLOSE_TAG;
        assertThrows(IllegalArgumentException.class, () -> new PageCipher(null).seal(header, new byte[10], 0, 10, 1, new byte[64 << 10], (_, _) -> {
        }));
        var unit = new byte[PageFormat.SIZE];
        assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).openContent(unit, 0, 10, unit, PageFormat.HEADER));
        System.arraycopy(header, 0, unit, 0, PageFormat.HEADER);
        assertFalse(new PageCipher(null).openContent(unit, 0, 10, new byte[10], 0));
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

    static Stream<Arguments> recordUnits() {
        var first = PageFormat.FIRST_RECORD_CONTENT;
        var record = PageFormat.RECORD_CONTENT;
        // One record, one record and a byte, two whole records, a last record that ends on a page, four records
        var lengths = new int[]{0, 1, first - 1, first, first + 1, record, record + 1, first + record, first + record + 1,
                first + record + PageFormat.SIZE - PageFormat.TAG, first + 2 * record, first + 2 * record + 1_000, (3 << 20) + 5};
        return Arrays.stream(lengths).boxed().flatMap(length -> Stream.of(true, false).map(cold -> Arguments.of(length, cold)));
    }

    @ParameterizedTest(name = "{0} bytes, cold: {1}")
    @MethodSource("recordUnits")
    @DisplayName("a unit of records seals each record as its own message under the unit's nonce with its index in the top bits, one write per record with zeros after the last, opens record by record and fails once a record, a tag, the header or the order changes")
    void sealsRecords(int length, boolean cold) throws Exception {
        var before = cold ? Sealing.coldLeftForTests(Sealing.COLD_BYTES) : -1;
        try {
            var cipher = new PageCipher(KEY);
            var pages = PageFormat.pagesFor(PageFormat.RECORDS, length);
            var content = new byte[length + 7];
            new Random(length).nextBytes(content);
            var header = new byte[PageFormat.HEADER];
            PageFile.header(header, PageFormat.EXTERNAL, 0, 5, pages, length, 77L, 9);
            header[PageFormat.FORMAT] = PageFormat.RECORDS;

            var parts = new ByteArrayOutputStream();
            var writes = new ArrayList<Integer>();
            var buffer = new byte[PageFormat.RECORD];
            // What the buffer held before must not reach the unit
            Arrays.fill(buffer, (byte) 0x5A);
            cipher.seal(header, content, 7, length, pages, buffer, (bytes, count) -> {
                writes.add(count);
                parts.write(bytes, 0, count);
            });
            var unit = parts.toByteArray();
            var records = PageFormat.records(length);
            assertEquals(pages * PageFormat.SIZE, unit.length);
            assertEquals(records, writes.size(), "one write per record");
            assertArrayEquals(header, Arrays.copyOf(unit, PageFormat.HEADER));
            var end = (int) PageFormat.sealedLength(PageFormat.RECORDS, length);
            assertArrayEquals(new byte[unit.length - end], Arrays.copyOfRange(unit, end, unit.length), "a byte after the last tag");
            for (var i = 1; i < records; i++) {
                assertEquals(i * PageFormat.RECORD, (int) PageFormat.recordPosition(i));
                assertEquals(0, PageFormat.recordPosition(i) % PageFormat.SIZE, "record " + i + " starts off a page");
            }

            // Each record is the message AES-GCM makes of its content under the unit's nonce with the record's index
            // in the top 16 bits of its first 8 bytes, with the header as additional data
            var plain = new byte[PageFormat.HEADER + length];
            System.arraycopy(header, 0, plain, 0, PageFormat.HEADER);
            try (var opener = cipher.records()) {
                for (var i = 0; i < records; i++) {
                    var at = (int) PageFormat.recordPosition(i);
                    var recordLength = PageFormat.recordLength(i, length);
                    var nonce = Arrays.copyOfRange(header, PageFormat.NONCE, PageFormat.NONCE + 12);
                    Bytes.i64(nonce, 0, 77L | (long) i << 48);
                    var reference = Cipher.getInstance("AES/GCM/NoPadding");
                    reference.init(Cipher.ENCRYPT_MODE, KEY, new GCMParameterSpec(128, nonce));
                    reference.updateAAD(header);
                    var expected = reference.doFinal(content, 7 + PageFormat.recordContent(i), recordLength);
                    assertArrayEquals(expected, Arrays.copyOfRange(unit, at, at + recordLength + PageFormat.TAG), "record " + i);
                    assertTrue(opener.open(header, i, unit, at, recordLength, plain, PageFormat.HEADER + PageFormat.recordContent(i)));
                    if (records > 1) {
                        assertFalse(opener.open(header, (i + 1) % records, unit, at, recordLength, new byte[recordLength], 0),
                                "record " + i + " opened at another index");
                    }
                    var tampered = unit.clone();
                    tampered[at + recordLength + PageFormat.TAG / 2] ^= 1;
                    assertFalse(opener.open(header, i, tampered, at, recordLength, new byte[recordLength], 0), "the tag of record " + i);
                    if (recordLength > 0) {
                        tampered = unit.clone();
                        tampered[at + recordLength / 2] ^= 1;
                        assertFalse(opener.open(header, i, tampered, at, recordLength, new byte[recordLength], 0), "the content of record " + i);
                    }
                    // A record cut short of its last byte
                    if (recordLength > 0) {
                        assertFalse(opener.open(header, i, unit, at, recordLength - 1, new byte[recordLength], 0), "record " + i + " cut short");
                    }
                }
                assertArrayEquals(Arrays.copyOfRange(content, 7, 7 + length), Arrays.copyOfRange(plain, PageFormat.HEADER, plain.length));
                // Two whole records swapped
                if (records >= 4) {
                    var swapped = unit.clone();
                    System.arraycopy(unit, PageFormat.RECORD, swapped, 2 * PageFormat.RECORD, PageFormat.RECORD);
                    System.arraycopy(unit, 2 * PageFormat.RECORD, swapped, PageFormat.RECORD, PageFormat.RECORD);
                    assertFalse(opener.open(header, 1, swapped, PageFormat.RECORD, PageFormat.RECORD_CONTENT, new byte[PageFormat.RECORD], 0));
                    assertFalse(opener.open(header, 2, swapped, 2 * PageFormat.RECORD, PageFormat.RECORD_CONTENT, new byte[PageFormat.RECORD], 0));
                }
                // Another header: a changed byte, the length, another format, a nonce with bits in the index's place
                var changed = new ArrayList<byte[]>();
                for (var at : new int[]{3, PageFormat.LENGTH + 3, PageFormat.PAGES + 3, PageFormat.NONCE + 11}) {
                    var other = header.clone();
                    other[at] ^= 1;
                    changed.add(other);
                }
                for (var format : new byte[]{PageFormat.VERSION, PageFormat.CLOSE_TAG}) {
                    var other = header.clone();
                    other[PageFormat.FORMAT] = format;
                    changed.add(other);
                }
                var high = header.clone();
                Bytes.i64(high, PageFormat.NONCE, 77L | 1L << 48);
                changed.add(high);
                var first = PageFormat.recordLength(0, length);
                for (var other : changed) {
                    assertFalse(opener.open(other, 0, unit, PageFormat.HEADER, first, new byte[first], 0));
                }
            }
            // Neither the whole-unit opens nor the seals of whole units take a unit of this format
            assertFalse(cipher.open(unit, 0, pages, new byte[unit.length]));
            assertFalse(cipher.openContent(unit, 0, length, new byte[length], 0));
            assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).seal(unit.clone(), pages));
            var otherFormat = unit.clone();
            otherFormat[PageFormat.FORMAT] = PageFormat.CLOSE_TAG;
            assertFalse(cipher.openContent(otherFormat, 0, length, new byte[length], 0));
            otherFormat[PageFormat.FORMAT] = PageFormat.VERSION;
            assertFalse(cipher.open(otherFormat, 0, pages, new byte[unit.length]));
        } finally {
            if (cold) {
                Sealing.coldLeftForTests(before);
            }
        }
    }

    @Test
    @DisplayName("a unit of records needs a store that encrypts, a nonce below 2^48, a buffer of a record and pages that hold its records")
    void recordsRefused() {
        var length = PageFormat.FIRST_RECORD_CONTENT + 100;
        var pages = PageFormat.pagesFor(PageFormat.RECORDS, length);
        var header = new byte[PageFormat.HEADER];
        PageFile.header(header, PageFormat.EXTERNAL, 0, 5, pages, length, 77L, 9);
        header[PageFormat.FORMAT] = PageFormat.RECORDS;
        var content = new byte[length];
        PageCipher.Output discard = (_, _) -> {
        };
        assertThrows(IllegalArgumentException.class, () -> new PageCipher(null).seal(header, content, 0, length, pages, new byte[PageFormat.RECORD], discard));
        assertThrows(IllegalStateException.class, () -> new PageCipher(null).records());
        assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).seal(header, content, 0, length, pages, new byte[PageFormat.RECORD - 1], discard));
        assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).seal(header, content, 0, length, pages - 1, new byte[PageFormat.RECORD], discard));
        var high = header.clone();
        Bytes.i64(high, PageFormat.NONCE, (1L << 48) | 77L);
        assertThrows(IllegalArgumentException.class, () -> new PageCipher(KEY).seal(high, content, 0, length, pages, new byte[PageFormat.RECORD], discard));
        try (var opener = new PageCipher(KEY).records()) {
            var sealed = new byte[PageFormat.RECORD];
            assertThrows(IllegalArgumentException.class, () -> opener.open(header, 0, sealed, 0, 100, sealed, 200));
        }
        // The pages of a unit of records hold its tags too
        assertEquals(PageFormat.pagesFor(PageFormat.FIRST_RECORD_CONTENT), PageFormat.pagesFor(PageFormat.RECORDS, PageFormat.FIRST_RECORD_CONTENT));
        assertTrue(PageFormat.fits(PageFormat.RECORDS, 128, PageFormat.FIRST_RECORD_CONTENT));
        assertFalse(PageFormat.fits(PageFormat.RECORDS, 128, PageFormat.FIRST_RECORD_CONTENT + 1));
        assertTrue(PageFormat.fits(PageFormat.RECORDS, 129, PageFormat.FIRST_RECORD_CONTENT + 1));
        assertEquals(4_097, PageFormat.pagesFor(PageFormat.RECORDS, 16 << 20));
        assertEquals(33, PageFormat.records(16 << 20));
        assertTrue(PageFormat.records(Integer.MAX_VALUE) < 1 << 16);
    }
}

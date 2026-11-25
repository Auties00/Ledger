package com.github.auties00.ledger;

import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The code examples of the root README, each run as it is printed there: the code between a `// readme:<name>` line
/// and the next `// readme:end` line must appear verbatim in README.md as a Java code block, which `inReadme` checks.
class ReadmeExamplesTest {
    private static final Path SOURCE = Path.of("src/test/java/com/github/auties00/ledger/ReadmeExamplesTest.java");
    private static final Path README = Path.of("../README.md");

    @TempDir
    Path directory;

    @Test
    @DisplayName("the quick start stores two entries durably and reads one back")
    void quickStart() throws InterruptedException {
        // readme:quick-start
        var settings = Keyspace.resident("settings", KeyCodec.utf8(), ValueCodec.utf8());
        var messages = Keyspace.paged("messages", KeyCodec.int64(), ValueCodec.utf8());
        try (var ledger = Ledger.builder()
                .directory(directory)
                .encryption(Encryption.passphrase("correct horse battery staple".toCharArray()))
                .keyspace(settings)
                .keyspace(messages)
                .build()) {
            var commit = ledger.commit(ledger.newBatch()
                    .put(messages, 1L, "Hello")
                    .put(settings, "theme", "dark"));
            commit.awaitDurable(); // on stable storage from here on
            System.out.println(ledger.get(messages, 1L).orElseThrow()); // Hello
        }
        // readme:end
        try (var ledger = Ledger.builder().directory(directory)
                .encryption(Encryption.passphrase("correct horse battery staple".toCharArray()))
                .keyspace(settings).keyspace(messages).build()) {
            assertEquals(Optional.of("dark"), ledger.get(settings, "theme"));
        }
    }

    @Test
    @DisplayName("a store opens with its options and reopens with the same key")
    void openStore() {
        var masterKey = new byte[32];
        var notes = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
        // readme:open
        var ledger = Ledger.builder()
                .directory(directory)                   // created if missing; one process holds it at a time
                .encryption(Encryption.key(masterKey))  // 32 bytes; Encryption.none() for plaintext files
                .keyspace(notes)
                .pageCache(64L << 20)                   // the cache of paged key spaces, on the heap
                .checkpointEvery(128L << 20)            // log growth between checkpoints
                .onBackgroundFailure(failure -> System.err.println("Checkpoint failed: " + failure))
                .build();
        // readme:end
        ledger.commit(ledger.newBatch().put(notes, "a", "1"));
        ledger.close();
        try (var reopened = Ledger.builder().directory(directory).encryption(Encryption.key(masterKey)).keyspace(notes).build()) {
            assertEquals(Optional.of("1"), reopened.get(notes, "a"));
        }
    }

    // readme:keyspaces
    record MessageKey(long chat, long sequence) {
        static final ValueLayout.OfLong BIG_ENDIAN = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

        // Big-endian with the sign bit flipped, so the bytes sort as the numbers do: by chat, then by sequence
        static final KeyCodec<MessageKey> CODEC = KeyCodec.of(
                key -> 16,
                (key, target) -> {
                    target.set(BIG_ENDIAN, 0, key.chat() ^ Long.MIN_VALUE);
                    target.set(BIG_ENDIAN, 8, key.sequence() ^ Long.MIN_VALUE);
                },
                source -> new MessageKey(
                        source.get(BIG_ENDIAN, 0) ^ Long.MIN_VALUE,
                        source.get(BIG_ENDIAN, 8) ^ Long.MIN_VALUE));

        static KeyRange<MessageKey> chat(long chat) {
            return KeyRange.between(new MessageKey(chat, Long.MIN_VALUE), new MessageKey(chat + 1, Long.MIN_VALUE));
        }
    }

    // Small and read often: every entry held in memory, decoded
    static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // Larger than memory should hold: read through the page cache
    static final Keyspace<MessageKey, String> MESSAGES = Keyspace.paged("messages", MessageKey.CODEC, ValueCodec.utf8());
    // readme:end

    private Ledger openMessages() {
        return Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(CHATS).keyspace(MESSAGES).build();
    }

    @Test
    @DisplayName("reads and writes go through batches, gets, ranges, firsts and counts")
    void readAndWrite() {
        try (var ledger = openMessages()) {
            // readme:read-write
            ledger.commit(ledger.newBatch()
                    .put(CHATS, "family", "Family")
                    .put(MESSAGES, new MessageKey(7, 1), "Hi")
                    .put(MESSAGES, new MessageKey(7, 2), "Dinner at eight?")
                    .put(MESSAGES, new MessageKey(7, 3), "Yes"));
            ledger.commit(ledger.newBatch().delete(MESSAGES, new MessageKey(7, 1)));

            Optional<String> chat = ledger.get(CHATS, "family");                          // Optional[Family]
            List<Entry<MessageKey, String>> page = ledger.range(MESSAGES, MessageKey.chat(7).reversed(), 50);
            Optional<Entry<MessageKey, String>> newest = ledger.first(MESSAGES, MessageKey.chat(7).reversed());
            long count = ledger.size(MESSAGES);                                           // 2, exact
            // readme:end
            assertEquals(Optional.of("Family"), chat);
            assertEquals(List.of("Yes", "Dinner at eight?"), page.stream().map(Entry::value).toList());
            assertEquals("Yes", newest.orElseThrow().value());
            assertEquals(2, count);
        }
    }

    @Test
    @DisplayName("a transaction reads and writes alone, and its writes apply together")
    void transactions() {
        var counters = Keyspace.resident("counters", KeyCodec.utf8(), ValueCodec.utf8());
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(counters).build()) {
            // readme:transaction
            ledger.update(transaction -> {
                var unread = transaction.get(counters, "unread").map(Integer::parseInt).orElse(0);
                transaction.put(counters, "unread", Integer.toString(unread + 1));
                transaction.put(counters, "last-sender", "Ada");
            });
            // readme:end
            ledger.update(transaction -> {
                var unread = transaction.get(counters, "unread").map(Integer::parseInt).orElse(0);
                transaction.put(counters, "unread", Integer.toString(unread + 1));
            });
            assertEquals(Optional.of("2"), ledger.get(counters, "unread"));
            assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                transaction.put(counters, "unread", "100");
                throw new IllegalStateException("rolled back");
            }));
            assertEquals(Optional.of("2"), ledger.get(counters, "unread"));
        }
    }

    @Test
    @DisplayName("a commit is visible at once and durable once awaited")
    void durability() throws InterruptedException {
        try (var ledger = openMessages()) {
            // readme:durability
            // Deferred: atomic and visible when commit returns; survives a crash of the process, not a loss of power
            var commit = ledger.commit(ledger.newBatch().put(MESSAGES, new MessageKey(7, 4), "Received"));

            // Durable: wait before acting on it, for example before acknowledging the message to its sender
            commit.awaitDurable();
            boolean durable = commit.isDurable();                     // true
            boolean inTime = commit.awaitDurable(Duration.ofMillis(5)); // a bounded wait

            // Or wait for every commit made so far at once
            ledger.awaitDurable();
            // readme:end
            assertTrue(durable);
            assertTrue(inTime);
        }
    }

    @Test
    @DisplayName("a snapshot keeps reading one state while range removals and scans run")
    void snapshotsAndScans() {
        try (var ledger = openMessages()) {
            var fill = ledger.newBatch();
            for (var sequence = 1; sequence <= 3; sequence++) {
                fill.put(MESSAGES, new MessageKey(7, sequence), "message " + sequence);
                fill.put(MESSAGES, new MessageKey(8, sequence), "message " + sequence);
            }
            ledger.commit(fill);
            // readme:snapshot
            try (var snapshot = ledger.snapshot()) {
                // Clear a chat: the cost does not grow with the number of entries removed
                ledger.commit(ledger.newBatch().deleteRange(MESSAGES, MessageKey.chat(7)));

                // The snapshot still reads the state before the removal
                long visited = snapshot.forEach(MESSAGES, MessageKey.chat(7), (key, text) -> {
                    System.out.println(key.sequence() + ": " + text);
                    return true; // false stops the scan
                });
                System.out.println(visited + " " + snapshot.size(MESSAGES) + " " + ledger.size(MESSAGES)); // 3 6 3
            }
            // readme:end
            assertEquals(3, ledger.size(MESSAGES));
            assertTrue(ledger.range(MESSAGES, MessageKey.chat(7), 1).isEmpty());
        }
    }

    @Test
    @DisplayName("a store refuses a wrong passphrase, and no encryption, with LedgerKeyException")
    void encryption() {
        var notes = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
        // readme:encryption
        var withKey = Encryption.key(masterKey());                       // a 256-bit key the application keeps
        var withPassphrase = Encryption.passphrase(passphrase());        // PBKDF2-HMAC-SHA256, 600,000 iterations
        // readme:end
        Ledger.builder().directory(directory.resolve("key")).encryption(withKey).keyspace(notes).build().close();
        Ledger.builder().directory(directory).encryption(withPassphrase).keyspace(notes).build().close();
        // readme:wrong-key
        try (var ledger = Ledger.builder()
                .directory(directory)
                .encryption(Encryption.passphrase("not the passphrase".toCharArray()))
                .keyspace(notes)
                .build()) {
            ledger.get(notes, "a");
        } catch (LedgerKeyException wrongKey) {
            System.err.println(wrongKey.getMessage()); // a wrong secret, never reported as damage
        }
        // readme:end
        assertThrows(LedgerKeyException.class, () -> Ledger.builder().directory(directory)
                .encryption(Encryption.none()).keyspace(notes).build());
    }

    private static byte[] masterKey() {
        return new byte[32];
    }

    private static char[] passphrase() {
        return "correct horse battery staple".toCharArray();
    }

    @Test
    @DisplayName("a store in memory keeps the same contract and forgets everything on close")
    void memory() throws InterruptedException {
        var notes = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
        // readme:memory
        try (var ledger = Ledger.builder().memory().keyspace(notes).build()) {
            ledger.commit(ledger.newBatch().put(notes, "draft", "Remember the milk")).awaitDurable(); // returns at once
        }
        // readme:end
        try (var ledger = Ledger.builder().memory().keyspace(notes).build()) {
            assertEquals(0, ledger.size(notes));
        }
    }

    @Test
    @DisplayName("a second store of a directory fails with LedgerLockedException")
    void locked() {
        var notes = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(notes).build()) {
            // readme:locked
            try (var second = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(notes).build()) {
                second.size(notes);
            } catch (LedgerLockedException locked) {
                System.err.println(locked.directory() + " is open in another store");
            }
            // readme:end
            assertEquals(0, ledger.size(notes));
        }
    }

    @Test
    @DisplayName("a failed force stops the store, and the next open recovers every durable commit")
    void stopAndRecover() throws InterruptedException {
        // readme:crash
        var disk = CrashFileSystem.create();
        var notes = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
        var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.none()).keyspace(notes).build();
        ledger.commit(ledger.newBatch().put(notes, "a", "durable")).awaitDurable();

        disk.stopAtForce(0); // the next force fails, and the simulated process stops there
        var commit = ledger.commit(ledger.newBatch().put(notes, "b", "visible"));
        try {
            commit.awaitDurable();
        } catch (LedgerIoException stopped) {
            // The store has stopped: every later commit and wait throws this too, reads keep working
        }

        // What a power cut at that moment could leave; the same seed draws the same state
        var afterCut = disk.afterPowerLoss(new Random(42));
        try (var recovered = Ledger.builder().directory(afterCut.getPath("/store")).encryption(Encryption.none()).keyspace(notes).build()) {
            System.out.println(recovered.get(notes, "a")); // Optional[durable]: every durable commit is back
            System.out.println(recovered.get(notes, "b")); // never durable: a power cut may keep the batch or drop it, never half of it
        }
        // readme:end
        assertTrue(disk.stopped());
        assertThrows(LedgerIoException.class, commit::awaitDurable);
        assertThrows(LedgerIoException.class, () -> ledger.commit(ledger.newBatch().put(notes, "c", "refused")));
        assertEquals(Optional.of("visible"), ledger.get(notes, "b"));
        try (var recovered = Ledger.builder().directory(afterCut.getPath("/store")).encryption(Encryption.none()).keyspace(notes).build()) {
            assertEquals(Optional.of("durable"), recovered.get(notes, "a"));
            var b = recovered.get(notes, "b");
            assertTrue(b.isEmpty() || b.get().equals("visible"));
        }
        try {
            ledger.close();
        } catch (LedgerIoException expected) {
            // A stopped store cannot make its commits durable on close
        }
    }

    @Test
    @DisplayName("every example of the README is printed there verbatim")
    void inReadme() throws IOException {
        var source = Files.readAllLines(SOURCE, StandardCharsets.UTF_8);
        var readme = Files.readString(README, StandardCharsets.UTF_8).replace("\r\n", "\n");
        var begin = Pattern.compile("^\\s*// readme:([a-z-]+)$");
        List<String> missing = new ArrayList<>();
        var examples = 0;
        for (var i = 0; i < source.size(); i++) {
            var match = begin.matcher(source.get(i));
            if (!match.matches() || match.group(1).equals("end")) {
                continue;
            }
            var lines = new ArrayList<String>();
            var j = i + 1;
            while (!source.get(j).strip().equals("// readme:end")) {
                lines.add(source.get(j++));
            }
            var indent = lines.stream().filter(line -> !line.isBlank())
                    .mapToInt(line -> line.length() - line.stripLeading().length()).min().orElse(0);
            var code = String.join("\n", lines.stream().map(line -> line.isBlank() ? "" : line.substring(indent)).toList());
            examples++;
            if (!readme.contains("```java\n" + code + "\n```")) {
                missing.add(match.group(1));
            }
        }
        assertTrue(examples > 0, "no example found");
        if (!missing.isEmpty()) {
            fail("README.md does not print these examples as they are in " + SOURCE + ": " + missing);
        }
    }
}

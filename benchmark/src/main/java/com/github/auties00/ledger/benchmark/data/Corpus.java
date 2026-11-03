package com.github.auties00.ledger.benchmark.data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.SplittableRandom;

/// A deterministic message history shaped like a WhatsApp account's.
///
/// Chats follow a Zipf distribution: a few hold most of the messages. Message bodies mirror what the store keeps
/// for each kind:
///
/// | Kind | Share | Body |
/// |---|---|---|
/// | Text | 70% | 90 to 450 bytes |
/// | Media without thumbnail | 15% | 600 to 1,300 bytes |
/// | Media with thumbnail | 15% | 2.5 to 12 KB |
///
/// Bodies are random bytes, as they are once sealed. Messages come in the order a history sync delivers them: by
/// 30-day window, newest window first, and within a window chat by chat, newest message first.
///
/// The same `messages` always yield the same corpus.
public final class Corpus {
    /// The latest message timestamp, in milliseconds since the epoch.
    public static final long LATEST = 1_793_000_000_000L;
    private static final long SPAN = 5L * 365 * 24 * 3600 * 1000;
    private static final long WINDOW = 30L * 24 * 3600 * 1000;
    private static final int POOL_SIZE = 16 << 20;
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    /// The length of a message id, which WhatsApp Web ids share.
    public static final int ID_LENGTH = 20;
    /// The length of a key of [com.github.auties00.ledger.benchmark.engine.Keyspace#MESSAGES].
    public static final int MESSAGE_KEY_LENGTH = 4 + 8 + ID_LENGTH;

    private final int messages;
    private final int[] chatSizes;
    private final int[] chatOf;
    private final int[] sequenceOf;
    private final byte[] pool;

    /// Generates a corpus of `messages` messages.
    ///
    /// @param messages the number of messages, at least 1 and below 16,777,216
    public Corpus(int messages) {
        if (messages < 1 || messages >= 1 << 24) {
            throw new IllegalArgumentException("messages must be in [1, 16777216): " + messages);
        }
        this.messages = messages;
        this.chatSizes = chatSizes(messages);
        this.chatOf = new int[messages];
        this.sequenceOf = new int[messages];
        var order = new long[messages];
        var next = 0;
        for (var chat = 0; chat < chatSizes.length; chat++) {
            for (var sequence = 0; sequence < chatSizes[chat]; sequence++) {
                var window = (LATEST - timestamp(chat, sequence)) / WINDOW;
                order[next++] = (window << 44) | ((long) chat << 24) | (0xFFFFFF - sequence);
            }
        }
        Arrays.sort(order);
        for (var i = 0; i < messages; i++) {
            chatOf[i] = (int) ((order[i] >>> 24) & 0xFFFFF);
            sequenceOf[i] = 0xFFFFFF - (int) (order[i] & 0xFFFFFF);
        }
        this.pool = new byte[POOL_SIZE];
        new SplittableRandom(0x1ED6E5L).nextBytes(pool);
    }

    private static int[] chatSizes(int messages) {
        var chats = Math.clamp(messages / 400, Math.min(64, messages), 4096);
        var weights = new double[chats];
        var total = 0.0;
        for (var chat = 0; chat < chats; chat++) {
            weights[chat] = 1.0 / (chat + 1);
            total += weights[chat];
        }
        var sizes = new int[chats];
        var assigned = 0;
        for (var chat = 0; chat < chats; chat++) {
            sizes[chat] = Math.max(1, (int) (messages * weights[chat] / total));
            assigned += sizes[chat];
        }
        // Rounding leaves a remainder, which goes to the largest chat; overshoot comes out of the largest too
        sizes[0] += messages - assigned;
        if (sizes[0] < 1) {
            throw new IllegalStateException("Cannot spread " + messages + " messages over " + chats + " chats");
        }
        return sizes;
    }

    /// {@return the number of messages}
    public int messages() {
        return messages;
    }

    /// {@return the number of chats}
    public int chats() {
        return chatSizes.length;
    }

    /// {@return the number of messages in `chat`}
    ///
    /// @param chat the chat number
    public int chatSize(int chat) {
        return chatSizes[chat];
    }

    /// {@return the chat of the message at position `index` of the ingest order}
    ///
    /// @param index the position
    public int chat(int index) {
        return chatOf[index];
    }

    /// {@return the position in its chat of the message at position `index` of the ingest order; 0 is the oldest}
    ///
    /// @param index the position
    public int sequence(int index) {
        return sequenceOf[index];
    }

    /// {@return the timestamp of message `sequence` of `chat`, in milliseconds since the epoch}
    ///
    /// @param chat the chat number
    /// @param sequence the position in the chat
    public long timestamp(int chat, int sequence) {
        var size = chatSizes[chat];
        var slot = SPAN / size;
        return LATEST - SPAN + sequence * slot + Math.floorMod(mix(chat, sequence, 1), Math.max(1, slot));
    }

    /// {@return the id of message `sequence` of `chat`: `3EB0` and 16 hex digits}
    ///
    /// @param chat the chat number
    /// @param sequence the position in the chat
    public byte[] id(int chat, int sequence) {
        var id = new byte[ID_LENGTH];
        id[0] = '3';
        id[1] = 'E';
        id[2] = 'B';
        id[3] = '0';
        var bits = mix(chat, sequence, 2);
        for (var i = 0; i < 16; i++) {
            id[4 + i] = HEX[(int) (bits >>> (60 - 4 * i)) & 0xF];
        }
        return id;
    }

    /// {@return the key of message `sequence` of `chat`: chat, timestamp and id, so a chat's messages sort by time}
    ///
    /// @param chat the chat number
    /// @param sequence the position in the chat
    public byte[] messageKey(int chat, int sequence) {
        return ByteBuffer.allocate(MESSAGE_KEY_LENGTH)
                .putInt(chat)
                .putLong(timestamp(chat, sequence))
                .put(id(chat, sequence))
                .array();
    }

    /// {@return the key under which the message key of message `sequence` of `chat` is found by id}
    ///
    /// @param chat the chat number
    /// @param sequence the position in the chat
    public byte[] idKey(int chat, int sequence) {
        return ByteBuffer.allocate(4 + ID_LENGTH)
                .putInt(chat)
                .put(id(chat, sequence))
                .array();
    }

    /// {@return the first key of `chat` in [com.github.auties00.ledger.benchmark.engine.Keyspace#MESSAGES]}
    ///
    /// @param chat the chat number
    public static byte[] chatStart(int chat) {
        return ByteBuffer.allocate(4).putInt(chat).array();
    }

    /// {@return the first key after `chat` in [com.github.auties00.ledger.benchmark.engine.Keyspace#MESSAGES]}
    ///
    /// @param chat the chat number
    public static byte[] chatEnd(int chat) {
        return ByteBuffer.allocate(4).putInt(chat + 1).array();
    }

    /// {@return the body of message `sequence` of `chat`}
    ///
    /// Its first byte holds flags: bit 0 marks a starred message (1%), bit 1 a message sent by the account (35%).
    ///
    /// @param chat the chat number
    /// @param sequence the position in the chat
    public byte[] body(int chat, int sequence) {
        var bits = mix(chat, sequence, 3);
        var kind = (int) Math.floorMod(bits, 100);
        var spread = (int) Math.floorMod(bits >>> 8, 1 << 20);
        var size = kind < 70 ? 90 + spread % 361
                : kind < 85 ? 600 + spread % 701
                : 2_500 + spread % 9_501;
        var body = slice(bits >>> 28, size);
        var flags = (int) Math.floorMod(bits >>> 40, 100);
        body[0] = (byte) ((flags == 0 ? 1 : 0) | (flags < 35 ? 2 : 0));
        return body;
    }

    /// {@return whether a body marks its message as starred}
    ///
    /// @param body a body [#body(int, int)] returned
    public static boolean starred(byte[] body) {
        return (body[0] & 1) != 0;
    }

    /// {@return `size` bytes from the random pool, starting at a position derived from `seed`}
    ///
    /// @param seed selects the position
    /// @param size the number of bytes, at most 16 MiB
    public byte[] slice(long seed, int size) {
        if (size > POOL_SIZE) {
            throw new IllegalArgumentException("size exceeds the pool: " + size);
        }
        var offset = (int) Math.floorMod(seed, POOL_SIZE - size + 1);
        return Arrays.copyOfRange(pool, offset, offset + size);
    }

    /// {@return a key of `length` bytes derived from `seed`, spread uniformly over the key space}
    ///
    /// @param seed selects the key
    /// @param length the key length, at least 8
    public static byte[] hashedKey(long seed, int length) {
        var key = new byte[length];
        for (var i = 0; i < length; i += 8) {
            var bits = mix(seed, i, 4);
            for (var j = 0; j < 8 && i + j < length; j++) {
                key[i + j] = (byte) (bits >>> (56 - 8 * j));
            }
        }
        return key;
    }

    /// {@return an 8-byte big-endian key, so sequence numbers sort numerically}
    ///
    /// @param value the number
    public static byte[] longKey(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    /// {@return the address of Signal session `index`, as the store keys sessions}
    ///
    /// @param index the session number
    public static byte[] sessionKey(int index) {
        return ("39" + (3_000_000_000L + index * 7919L) + ".0:" + (index % 4)).getBytes(StandardCharsets.US_ASCII);
    }

    private static long mix(long a, long b, long salt) {
        var z = a * 0x9E3779B97F4A7C15L + b * 0xC2B2AE3D27D4EB4FL + salt * 0x165667B19E3779F9L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}

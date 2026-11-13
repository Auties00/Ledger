package com.github.auties00.ledger.benchmark.support;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;

/// The messages a client receives after the history a store holds, as the workloads that write to a loaded store add
/// them.
///
/// Each message is what `cobalt.OfflineDrainBenchmark` stores for one: its body, its id index entry, the sender's
/// Signal session and the chat's metadata. Its chat and body follow those of a message of the history picked at
/// random, so new messages go where the history's activity is; its timestamp is past [Corpus#LATEST], so it sorts after
/// every message of its chat. Messages are numbered from a first number, and two instances whose numbers never meet
/// write different keys, so concurrent writers can each own one. Not thread-safe.
public final class Arrivals {
    private final Corpus corpus;
    private final SplittableRandom random;
    private long next;

    /// Creates the messages that follow `corpus`'s history.
    ///
    /// @param corpus the history
    /// @param first the number of the first message
    /// @param seed selects the chats and senders
    public Arrivals(Corpus corpus, long first, long seed) {
        this.corpus = corpus;
        this.random = new SplittableRandom(seed);
        this.next = first;
    }

    /// Adds the next message to `batch`.
    ///
    /// @param batch receives the message's four writes
    /// @return `batch`
    public WriteBatch receive(WriteBatch batch) {
        var template = random.nextInt(corpus.messages());
        var chat = corpus.chat(template);
        var number = next++;
        var id = corpus.id(chat, (int) (number % 0xFFFFFF));
        var key = ByteBuffer.allocate(Corpus.MESSAGE_KEY_LENGTH)
                .putInt(chat)
                .putLong(Corpus.LATEST + number)
                .put(id)
                .array();
        var idKey = ByteBuffer.allocate(4 + Corpus.ID_LENGTH)
                .putInt(chat)
                .put(id)
                .array();
        return batch.put(Keyspace.MESSAGES, key, corpus.body(chat, corpus.sequence(template)))
                .put(Keyspace.MESSAGE_IDS, idKey, key)
                .put(Keyspace.SESSIONS, Corpus.sessionKey(random.nextInt(Stores.SESSIONS)), corpus.slice(number, Stores.SESSION_BYTES))
                .put(Keyspace.CHATS, Corpus.chatStart(chat), corpus.slice(~number, Stores.CHAT_BYTES));
    }
}

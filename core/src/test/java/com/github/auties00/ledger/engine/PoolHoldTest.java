package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The work that holds the overflow of a pool while it runs, so collections in the middle of it keep the arrays it
/// takes again: the body of a frame on chunks, a batch on a rope, a checkpoint; and the overflow a store's large
/// commit leaves, which the first collection after it frees. The tests that look for collections run `System.gc()`, a
/// full one.
class PoolHoldTest {
    private static final byte[] KEY = new byte[32];
    private static final int CHUNK = FrameBody.CHUNK_BYTES;
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());

    /// Fills `body` with writes of a few kilobytes until it holds `size` bytes.
    private static void fillTo(FrameBody body, int size) {
        var key = new byte[16];
        var value = new byte[3_000];
        var i = 0;
        while (body.size() < size) {
            ByteBuffer.wrap(key).putInt(0, i++);
            body.put(1, MemorySegment.ofArray(key), MemorySegment.ofArray(value));
        }
    }

    /// {@return the arrays the body's bytes are in, by identity}
    private static List<byte[]> chunks(FrameBody body) {
        var arrays = new ArrayList<byte[]>();
        for (var i = 0; i < body.chunkCount(); i++) {
            arrays.add(body.chunk(i));
        }
        return arrays;
    }

    /// Runs collections until `pool` holds no overflow, at most 100, waiting a little between them for work that
    /// still holds it to end.
    private static void collectOverflow(HeapPool pool) {
        for (var i = 0; i < 100 && pool.overflowBytesForTests() != 0; i++) {
            System.gc();
            LockSupport.parkNanos(10_000_000);
        }
    }

    private static Ledger.Options options(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES);
    }

    @Test
    @DisplayName("a body holds the overflow from its first chunk, a later one or the one it starts on, until it releases its chunks")
    void bodyHolds() {
        var pool = new HeapPool(64L << 20, false);
        var grown = new FrameBody(256, FrameBody.NOT_REFERABLE, pool);
        fillTo(grown, CHUNK / 2);
        assertFalse(grown.holdsPoolForTests(), "a body on an array of its own");
        fillTo(grown, CHUNK + 100);
        assertTrue(grown.holdsPoolForTests(), "a body past its first chunk");
        grown.handOff();
        grown.release();
        assertFalse(grown.holdsPoolForTests(), "a released body");
        var started = new FrameBody(CHUNK, FrameBody.NOT_REFERABLE, pool);
        assertTrue(started.holdsPoolForTests(), "a body started on a chunk");
        started.discard();
        assertFalse(started.holdsPoolForTests(), "a discarded body");
        var plain = new FrameBody(16);
        fillTo(plain, 2 * CHUNK);
        assertFalse(plain.holdsPoolForTests(), "a body made without a pool");
    }

    @Test
    @DisplayName("the next body on chunks takes the chunks of the frame sealed before it, through collections while it fills, and the first collection after its release frees what is past the reserve")
    void bodyKeepsChunks() {
        var pool = new HeapPool(64L << 20, false);
        var first = new FrameBody(CHUNK, FrameBody.NOT_REFERABLE, pool);
        fillTo(first, 12 * CHUNK - 4_000);
        var arrays = chunks(first);
        assertEquals(12, arrays.size());
        var next = new FrameBody(CHUNK, FrameBody.NOT_REFERABLE, pool);
        // The log seals the first frame while the next commit fills its body
        first.handOff();
        first.release();
        assertEquals(8L * CHUNK, pool.overflowBytesForTests(), "the chunks past the reserve");
        System.gc();
        System.gc();
        assertEquals(8L * CHUNK, pool.overflowBytesForTests(), "a collection freed the chunks while a body held them");
        fillTo(next, 12 * CHUNK - 4_000);
        // The first chunk of the next body is a new one, taken before the first released its own
        var taken = chunks(next);
        var reused = taken.stream().filter(chunk -> arrays.stream().anyMatch(array -> array == chunk)).count();
        assertEquals(11, reused, "the chunks of the first body the next one took");
        next.handOff();
        next.release();
        arrays.clear();
        taken.clear();
        collectOverflow(pool);
        assertEquals(0, pool.overflowBytesForTests(), "a collection with no body open left the overflow");
        assertEquals(HeapPool.RESERVE_PER_SIZE, pool.reservedBytesForTests());
    }

    @Test
    @DisplayName("an abandoned body lets go of its hold and keeps its chunks, which never go back to the pool")
    void abandoned() {
        var pool = new HeapPool(64L << 20, false);
        pool.holdOverflowForTests(true);
        var body = new FrameBody(256, FrameBody.NOT_REFERABLE, pool);
        fillTo(body, 3 * CHUNK);
        var arrays = chunks(body);
        body.handOff();
        body.abandon();
        body.abandon();
        assertFalse(body.holdsPoolForTests());
        assertEquals(List.of(), pool.freeForTests(CHUNK));
        assertEquals(arrays, chunks(body), "the chunks the body still holds, which a seal may read");
        var unpooled = new FrameBody(16);
        unpooled.abandon();
        assertFalse(unpooled.holdsPoolForTests());
    }

    @Test
    @DisplayName("a batch holds the overflow from its move to a rope until it leaves it, at its end or its clear, and a batch dropped on a rope holds nothing")
    void batchHolds() {
        var pool = new HeapPool(64L << 20, false);
        var batch = new ArrayWriteBatch(new Object(), pool, Map.of(MESSAGES.name(), MESSAGES));
        var key = new byte[8];
        batch.put(MESSAGES, key.clone(), new byte[10_000]);
        assertFalse(batch.holdsPoolForTests(), "a batch on its array");
        for (var i = 1; batch.chunksForTests() == 0; i++) {
            ByteBuffer.wrap(key).putInt(0, i);
            batch.put(MESSAGES, key.clone(), new byte[10_000]);
        }
        assertTrue(batch.holdsPoolForTests(), "a batch on a rope");
        // The chunks of the frame before it, sealed while the batch fills
        for (var i = 0; i < 12; i++) {
            pool.release(new byte[CHUNK]);
        }
        var overflow = pool.overflowBytesForTests();
        assertTrue(overflow > 0, "the chunks past the reserve");
        System.gc();
        System.gc();
        assertEquals(overflow, pool.overflowBytesForTests(), "a collection freed the chunks while a batch held them");
        batch.clear();
        assertFalse(batch.holdsPoolForTests(), "a cleared batch");
        collectOverflow(pool);
        assertEquals(0, pool.overflowBytesForTests(), "a collection with no batch on a rope left the overflow");

        var dropped = new ArrayWriteBatch(new Object(), pool, Map.of(MESSAGES.name(), MESSAGES));
        for (var i = 0; dropped.chunksForTests() == 0; i++) {
            ByteBuffer.wrap(key).putInt(0, i);
            dropped.put(MESSAGES, key.clone(), new byte[10_000]);
        }
        var probe = new WeakReference<>(dropped);
        dropped = null;
        for (var i = 0; i < 12; i++) {
            pool.release(new byte[CHUNK]);
        }
        assertTrue(pool.overflowBytesForTests() > 0, "the chunks past the reserve");
        collectOverflow(pool);
        assertEquals(0, pool.overflowBytesForTests(), "a batch dropped on a rope still held the overflow");
        assertNull(probe.get());
    }

    @Test
    @DisplayName("a checkpoint holds the overflow of the store's pool while it writes its pages, and lets go of it once it ends")
    void checkpointHolds() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var armed = new AtomicBoolean();
        var probes = new AtomicInteger();
        var held = new AtomicInteger();
        disk.intercept((path, kind) -> {
            // The first writes of the checkpoint: a collection at each of them takes a while
            if (armed.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE) && probes.get() < 3) {
                probes.incrementAndGet();
                // Reached from here only through the probe
                var probe = new WeakReference<>(HeapPool.process().hold());
                System.gc();
                System.gc();
                if (probe.get() != null) {
                    held.incrementAndGet();
                }
            }
        });
        try (var ledger = options(directory).build()) {
            var key = new byte[8];
            for (var commit = 0; commit < 4; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 500; i++) {
                    ByteBuffer.wrap(key).putInt(0, commit * 1_000 + i);
                    batch.put(MESSAGES, key.clone(), new byte[2_000]);
                }
                ledger.commit(batch).awaitDurable();
            }
            armed.set(true);
            ledger.checkpoint();
            armed.set(false);
            assertTrue(probes.get() > 0, "the checkpoint wrote no page");
            assertEquals(probes.get(), held.get(), "the writes of the checkpoint the overflow was free at");
            var after = new WeakReference<>(HeapPool.process().hold());
            for (var i = 0; i < 100 && after.get() != null; i++) {
                System.gc();
                LockSupport.parkNanos(10_000_000);
            }
            assertNull(after.get(), "the overflow held once the checkpoint ended");
        }
    }

    @Test
    @DisplayName("once a store's large commit is durable, the first collection leaves the process's pool no more than its reserve")
    void largeCommitLeavesReserve() throws Exception {
        var disk = CrashFileSystem.create();
        var pool = HeapPool.process();
        try (var ledger = options(disk.getPath("/store")).build()) {
            var batch = ledger.newBatch();
            var key = new byte[8];
            for (var i = 0; i < 2_000; i++) {
                ByteBuffer.wrap(key).putInt(0, i);
                batch.put(MESSAGES, key.clone(), new byte[4_000]);
            }
            ledger.commit(batch).awaitDurable();
            assertTrue(pool.overflowBytesForTests() > 0 || pool.reservedBytesForTests() > 0, "the chunks of the commit's frame and rope");
            collectOverflow(pool);
            assertEquals(0, pool.overflowBytesForTests(), "the overflow after the commit");
            assertTrue(pool.reservedBytesForTests() <= HeapPool.RESERVE, "the reserve after the commit");
        }
    }
}

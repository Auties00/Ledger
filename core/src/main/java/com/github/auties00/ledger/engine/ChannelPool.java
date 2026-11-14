package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/// The channels of one file that callers take turns on, each used by one thread at a time: Windows serializes
/// positional reads and writes on one channel, and an interrupt during a call closes the channel it ran on.
///
/// The pool opens one channel at once and the others, up to its limit, as callers find every open one in use; a store
/// that never reads from several threads at once keeps one descriptor. A caller that finds none free and cannot open
/// another, because the limit is reached or the system refuses one, waits for one to be given back.
///
/// It is safe for concurrent use.
final class ChannelPool implements AutoCloseable {
    /// How long a caller waiting for a channel waits before it looks again whether the pool closed.
    private static final long CLOSED_CHECK_MILLIS = 100;

    private final Path path;
    private final OpenOption[] options;
    private final int limit;
    // The channels no caller holds
    private final ArrayBlockingQueue<FileChannel> idle;
    // The channels in circulation, held, idle or being opened: at most limit, so every one fits back in the queue
    private final AtomicInteger opened;
    private volatile boolean closed;

    /// Opens the first channel of a pool of at most `limit` channels, each opened with `options`.
    ///
    /// @throws IOException if the first channel cannot be opened
    ChannelPool(Path path, int limit, OpenOption... options) throws IOException {
        if (limit < 1) {
            throw new IllegalArgumentException("A pool needs at least one channel: " + limit);
        }
        this.path = path;
        this.options = options;
        this.limit = limit;
        this.idle = new ArrayBlockingQueue<>(limit);
        this.opened = new AtomicInteger(1);
        idle.add(FileChannel.open(path, options));
    }

    /// {@return an open channel for the caller alone, until it gives it back with [#give(FileChannel)]}
    ///
    /// It waits while every channel is in use and no other can be opened, and keeps an interrupt for the caller.
    ///
    /// @throws IllegalStateException if the pool is closed
    /// @throws LedgerIoException if the channel taken was closed by an interrupt and none can be opened in its place
    FileChannel take() {
        if (closed) {
            throw new IllegalStateException("The store is closed");
        }
        var channel = idle.poll();
        if (channel == null) {
            channel = openExtra();
            if (channel == null) {
                channel = await();
            }
        }
        if (channel.isOpen()) {
            return channel;
        }
        // An interrupt closed it after its last use, and it could not be replaced then
        try {
            return reopen();
        } catch (IOException exception) {
            give(channel);
            throw new LedgerIoException(path + " could not be opened", exception);
        }
    }

    /// {@return a new channel in place of one an interrupt closed, which the caller gives back instead}
    ///
    /// @throws IOException if it cannot be opened
    FileChannel reopen() throws IOException {
        return FileChannel.open(path, options);
    }

    /// Gives back a channel [#take()] returned or [#reopen()] opened in its place; one given back after the pool closed
    /// is closed.
    void give(FileChannel channel) {
        // The count never passes the queue's capacity, so the offer only fails if a caller gave a channel twice
        if (!idle.offer(channel)) {
            closeQuietly(channel);
            return;
        }
        // After the offer: either close() drains the channel, or the closed flag it set first is visible here
        if (closed && idle.remove(channel)) {
            opened.decrementAndGet();
            closeQuietly(channel);
        }
    }

    /// {@return the channels in circulation, held or idle, for tests}
    int openedForTests() {
        return opened.get();
    }

    /// Closes the channels no caller holds; those held are closed as they are given back.
    @Override
    public void close() {
        closed = true;
        FileChannel channel;
        while ((channel = idle.poll()) != null) {
            opened.decrementAndGet();
            closeQuietly(channel);
        }
    }

    /// {@return another channel, if the limit and the system allow one, or `null`}
    private FileChannel openExtra() {
        while (true) {
            var count = opened.get();
            if (count >= limit) {
                return null;
            }
            if (opened.compareAndSet(count, count + 1)) {
                break;
            }
        }
        try {
            return FileChannel.open(path, options);
        } catch (IOException _) {
            // Out of descriptors, say: the channels already open serve, one at a time
            opened.decrementAndGet();
            return null;
        }
    }

    /// {@return a channel another caller gives back, waited for}
    ///
    /// @throws IllegalStateException if the pool closes meanwhile, as no channel may come back then
    private FileChannel await() {
        var interrupted = false;
        try {
            while (true) {
                try {
                    var channel = idle.poll(CLOSED_CHECK_MILLIS, TimeUnit.MILLISECONDS);
                    if (channel != null) {
                        return channel;
                    }
                    if (closed) {
                        throw new IllegalStateException("The store is closed");
                    }
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void closeQuietly(FileChannel channel) {
        try {
            channel.close();
        } catch (IOException _) {
            // Nothing is left unforced through a caller's channel: the files are forced through channels of their own
        }
    }
}

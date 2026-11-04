package com.github.auties00.ledger.testkit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/// A channel on a file or a directory of a [CrashFileSystem]; a channel on a directory reads nothing, and its
/// [#force(boolean)] makes the namespace changes that touched the directory durable.
final class CrashFileChannel extends FileChannel {
    private final CrashFileSystem fileSystem;
    private final Disk disk;
    private final String path;
    private final Disk.FileNode file;
    private final boolean readable;
    private final boolean writable;
    private final boolean append;
    private long position;

    /// Creates a channel on `file`, or on the directory at `path` if `file` is `null`.
    CrashFileChannel(CrashFileSystem fileSystem, String path, Disk.FileNode file, boolean readable, boolean writable, boolean append) {
        this.fileSystem = fileSystem;
        this.disk = fileSystem.disk();
        this.path = path;
        this.file = file;
        this.readable = readable;
        this.writable = writable;
        this.append = append;
    }

    private Disk.FileNode file() throws IOException {
        if (file == null) {
            throw new IOException(path + " is a directory");
        }
        return file;
    }

    private void checkOpen() throws IOException {
        if (!isOpen()) {
            throw new java.nio.channels.ClosedChannelException();
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        synchronized (disk.lock) {
            var read = read(dst, position);
            if (read > 0) {
                position += read;
            }
            return read;
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        var total = 0L;
        for (var i = offset; i < offset + length; i++) {
            var read = read(dsts[i]);
            if (read < 0) {
                return total == 0 ? -1 : total;
            }
            total += read;
            if (dsts[i].hasRemaining()) {
                break;
            }
        }
        return total;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        fileSystem.beforeChange(path, ChangeInterceptor.ChangeKind.WRITE);
        synchronized (disk.lock) {
            if (append) {
                position = file().size;
            }
            var written = writeAt(src, position);
            position += written;
            return written;
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        var total = 0L;
        for (var i = offset; i < offset + length; i++) {
            total += write(srcs[i]);
        }
        return total;
    }

    @Override
    public long position() throws IOException {
        checkOpen();
        return position;
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        checkOpen();
        if (newPosition < 0) {
            throw new IllegalArgumentException("Negative position: " + newPosition);
        }
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        synchronized (disk.lock) {
            checkOpen();
            disk.alive();
            return file == null ? 0 : file.size;
        }
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        fileSystem.beforeChange(path, ChangeInterceptor.ChangeKind.TRUNCATE);
        synchronized (disk.lock) {
            checkOpen();
            if (!writable) {
                throw new NonWritableChannelException();
            }
            disk.truncate(file(), size);
            position = Math.min(position, size);
            return this;
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        fileSystem.beforeChange(path, ChangeInterceptor.ChangeKind.FORCE);
        synchronized (disk.lock) {
            checkOpen();
            if (file == null) {
                disk.forceDirectory(path);
            } else {
                disk.force(file);
            }
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException("Transfers are not simulated");
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) {
        throw new UnsupportedOperationException("Transfers are not simulated");
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        synchronized (disk.lock) {
            checkOpen();
            if (!readable) {
                throw new NonReadableChannelException();
            }
            disk.alive();
            var source = file();
            if (position >= source.size) {
                return -1;
            }
            var count = (int) Math.min(dst.remaining(), source.size - position);
            dst.put(source.data, (int) position, count);
            return count;
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        fileSystem.beforeChange(path, ChangeInterceptor.ChangeKind.WRITE);
        synchronized (disk.lock) {
            return writeAt(src, position);
        }
    }

    private int writeAt(ByteBuffer src, long position) throws IOException {
        synchronized (disk.lock) {
            checkOpen();
            if (!writable) {
                throw new NonWritableChannelException();
            }
            var bytes = new byte[src.remaining()];
            var start = src.position();
            src.get(start, bytes);
            disk.write(file(), position, bytes);
            // Consumed only once the write succeeded
            src.position(start + bytes.length);
            return bytes.length;
        }
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
        throw new UnsupportedOperationException("Mapping is not simulated");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        var lock = tryLock(position, size, shared);
        if (lock == null) {
            throw new OverlappingFileLockException();
        }
        return lock;
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        synchronized (disk.lock) {
            checkOpen();
            if (!disk.lock(file(), this)) {
                // The lock is held by this machine, as another channel of the simulated process holds it
                throw new OverlappingFileLockException();
            }
            return new FileLock(this, position, size, shared) {
                private boolean valid = true;

                @Override
                public boolean isValid() {
                    synchronized (disk.lock) {
                        return valid && CrashFileChannel.this.isOpen();
                    }
                }

                @Override
                public void release() {
                    synchronized (disk.lock) {
                        if (valid) {
                            valid = false;
                            disk.unlock(file, CrashFileChannel.this);
                        }
                    }
                }
            };
        }
    }

    @Override
    protected void implCloseChannel() {
        synchronized (disk.lock) {
            if (file != null) {
                disk.unlock(file, this);
            }
        }
    }
}

package com.github.auties00.ledger.testkit;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/// The provider of one [CrashFileSystem], which applies every operation to its [Disk].
final class CrashFileSystemProvider extends FileSystemProvider {
    private final CrashFileSystem fileSystem;

    CrashFileSystemProvider(CrashFileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    private CrashPath cast(Path path) {
        if (!(path instanceof CrashPath crash) || crash.getFileSystem() != fileSystem) {
            throw new ProviderMismatchException();
        }
        return crash;
    }

    private Disk disk() {
        return fileSystem.disk();
    }

    @Override
    public String getScheme() {
        return CrashFileSystem.SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
        throw new UnsupportedOperationException("Create crash file systems with CrashFileSystem.create()");
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        throw new UnsupportedOperationException("Crash file systems have no URIs");
    }

    @Override
    public Path getPath(URI uri) {
        throw new UnsupportedOperationException("Crash file systems have no URIs");
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return newFileChannel(path, options, attrs);
    }

    @Override
    public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        var key = cast(path).key();
        var write = options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.APPEND);
        var read = options.contains(StandardOpenOption.READ) || !write;
        var disk = disk();
        boolean creates;
        synchronized (disk.lock) {
            creates = write && disk.node(key) == null
                    && (options.contains(StandardOpenOption.CREATE) || options.contains(StandardOpenOption.CREATE_NEW));
        }
        if (creates) {
            fileSystem.beforeChange(key, ChangeInterceptor.ChangeKind.CREATE);
        }
        synchronized (disk.lock) {
            disk.alive();
            var node = disk.node(key);
            if (node instanceof Disk.DirectoryNode) {
                if (write) {
                    throw new IOException(key + " is a directory");
                }
                return new CrashFileChannel(fileSystem, key, null, true, false, false, false);
            }
            if (node == null) {
                if (!write || !(options.contains(StandardOpenOption.CREATE) || options.contains(StandardOpenOption.CREATE_NEW))) {
                    throw new NoSuchFileException(key);
                }
                var file = new Disk.FileNode();
                disk.create(key, file);
                node = file;
            } else if (write && options.contains(StandardOpenOption.CREATE_NEW)) {
                throw new FileAlreadyExistsException(key);
            }
            var file = (Disk.FileNode) node;
            if (write && options.contains(StandardOpenOption.TRUNCATE_EXISTING) && file.size > 0) {
                disk.truncate(file, 0);
            }
            // Other options, such as the JDK's DIRECT, change nothing a loss of power keeps
            var sync = options.contains(StandardOpenOption.DSYNC) || options.contains(StandardOpenOption.SYNC);
            return new CrashFileChannel(fileSystem, key, file, read, write, options.contains(StandardOpenOption.APPEND), sync);
        }
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        var directory = cast(dir);
        var key = directory.key();
        var disk = disk();
        var entries = new ArrayList<Path>();
        synchronized (disk.lock) {
            for (var name : disk.list(key)) {
                var entry = directory.resolve(name);
                if (filter.accept(entry)) {
                    entries.add(entry);
                }
            }
        }
        return new DirectoryStream<>() {
            @Override
            public Iterator<Path> iterator() {
                return entries.iterator();
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        fileSystem.beforeChange(cast(dir).key(), ChangeInterceptor.ChangeKind.CREATE);
        var disk = disk();
        synchronized (disk.lock) {
            disk.create(cast(dir).key(), new Disk.DirectoryNode());
        }
    }

    @Override
    public void delete(Path path) throws IOException {
        fileSystem.beforeChange(cast(path).key(), ChangeInterceptor.ChangeKind.DELETE);
        var disk = disk();
        synchronized (disk.lock) {
            disk.delete(cast(path).key());
        }
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        var from = cast(source).key();
        var to = cast(target).key();
        var replace = Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING);
        fileSystem.beforeChange(from, ChangeInterceptor.ChangeKind.RENAME);
        var disk = disk();
        synchronized (disk.lock) {
            if (!(disk.node(from) instanceof Disk.FileNode file)) {
                throw disk.node(from) == null ? new NoSuchFileException(from) : new IOException("Copying a directory is not simulated: " + from);
            }
            if (disk.node(to) != null) {
                if (!replace) {
                    throw new FileAlreadyExistsException(to);
                }
                disk.delete(to);
            }
            var copy = new Disk.FileNode();
            disk.create(to, copy);
            disk.write(copy, 0, Arrays.copyOf(file.data, (int) file.size));
        }
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        var from = cast(source).key();
        var to = cast(target).key();
        var replace = Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)
                || Arrays.asList(options).contains(StandardCopyOption.ATOMIC_MOVE);
        fileSystem.beforeChange(from, ChangeInterceptor.ChangeKind.RENAME);
        var disk = disk();
        synchronized (disk.lock) {
            if (!replace && disk.node(to) != null) {
                throw new FileAlreadyExistsException(to);
            }
            disk.rename(from, to);
        }
    }

    @Override
    public boolean isSameFile(Path path, Path path2) {
        return cast(path).key().equals(cast(path2).key());
    }

    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) {
        throw new UnsupportedOperationException("A crash file system has no file stores");
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        var disk = disk();
        synchronized (disk.lock) {
            disk.alive();
            if (disk.node(cast(path).key()) == null) {
                throw new NoSuchFileException(path.toString());
            }
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if (type != BasicFileAttributes.class) {
            throw new UnsupportedOperationException("A crash file system has basic attributes only");
        }
        var key = cast(path).key();
        var disk = disk();
        synchronized (disk.lock) {
            disk.alive();
            var node = disk.node(key);
            if (node == null) {
                throw new NoSuchFileException(key);
            }
            var size = node instanceof Disk.FileNode file ? file.size : 0;
            return (A) new Attributes(node instanceof Disk.DirectoryNode, size, node);
        }
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        var basic = readAttributes(path, BasicFileAttributes.class, options);
        var values = new HashMap<String, Object>();
        values.put("size", basic.size());
        values.put("isDirectory", basic.isDirectory());
        values.put("isRegularFile", basic.isRegularFile());
        values.put("isSymbolicLink", false);
        values.put("isOther", false);
        values.put("lastModifiedTime", basic.lastModifiedTime());
        values.put("lastAccessTime", basic.lastAccessTime());
        values.put("creationTime", basic.creationTime());
        values.put("fileKey", basic.fileKey());
        return values;
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {
        throw new UnsupportedOperationException("A crash file system has no settable attributes");
    }

    /// The basic attributes of a node.
    private record Attributes(boolean isDirectory, long size, Object fileKey) implements BasicFileAttributes {
        private static final FileTime EPOCH = FileTime.fromMillis(0);

        @Override
        public FileTime lastModifiedTime() {
            return EPOCH;
        }

        @Override
        public FileTime lastAccessTime() {
            return EPOCH;
        }

        @Override
        public FileTime creationTime() {
            return EPOCH;
        }

        @Override
        public boolean isRegularFile() {
            return !isDirectory;
        }

        @Override
        public boolean isSymbolicLink() {
            return false;
        }

        @Override
        public boolean isOther() {
            return false;
        }
    }
}

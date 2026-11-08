package com.github.auties00.ledger.testkit;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;

/// The state of a [CrashFileSystem]: every file's contents as written and as made durable, and the namespace as it is
/// and as made durable.
///
/// It models a file system that journals its metadata in order and caches file data, as ext4, XFS, APFS and NTFS do,
/// and assumes no more than POSIX promises:
///
/// - a write is durable once a force of its file returns, or once it returns itself if its channel was opened with
///   `DSYNC`; until then a loss of power may keep it, drop it, or keep any of its 512-byte sectors, in any combination
///   with the other writes since the force, and may extend the file over it while dropping its data;
/// - a truncation is durable once a force of its file returns;
/// - creating, renaming and deleting are durable once a force of a directory they touch returns; a loss of power keeps
///   an unforced prefix of them, in the order they ran, and a file whose entry it does not keep is gone with its data;
/// - a crash of the process alone keeps everything.
///
/// Every call holds [#lock].
final class Disk {
    static final int SECTOR = 512;

    final Object lock;
    // Absolute path to node, as the namespace is now
    private final Map<String, Node> names;
    // As the last forces left it
    private final Map<String, Node> durableNames;
    private final List<NamespaceChange> pending;
    // Locks held, by file, and the channel holding each
    private final Map<FileNode, Object> locks;
    private long remaining;
    private long remainingForces;
    private long operations;
    private long forces;
    private boolean stopped;

    Disk() {
        this.lock = new Object();
        this.names = new HashMap<>();
        this.durableNames = new HashMap<>();
        this.pending = new ArrayList<>();
        this.locks = new IdentityHashMap<>();
        this.remaining = Long.MAX_VALUE;
        this.remainingForces = Long.MAX_VALUE;
        var root = new DirectoryNode();
        names.put("/", root);
        durableNames.put("/", root);
    }

    /// A file or a directory.
    sealed interface Node permits FileNode, DirectoryNode {
    }

    /// A directory, whose entries are the names under it.
    static final class DirectoryNode implements Node {
    }

    /// A file: its contents now, as last forced, and the changes since.
    static final class FileNode implements Node {
        byte[] data;
        long size;
        byte[] durableData;
        long durableSize;
        final List<Change> changes;

        FileNode() {
            this.data = new byte[0];
            this.durableData = new byte[0];
            this.changes = new ArrayList<>();
        }

        FileNode(byte[] contents) {
            this.data = contents.clone();
            this.size = contents.length;
            this.durableData = contents.clone();
            this.durableSize = contents.length;
            this.changes = new ArrayList<>();
        }
    }

    /// A write, or with `truncation` at 0 or more, a truncation to that size.
    record Change(long offset, byte[] bytes, long truncation) {
    }

    /// A change of the namespace.
    sealed interface NamespaceChange permits Create, Delete, Rename {
        /// {@return whether the change touches the entries of `directory`}
        boolean touches(String directory);
    }

    record Create(String path, Node node) implements NamespaceChange {
        @Override
        public boolean touches(String directory) {
            return parent(path).equals(directory);
        }
    }

    record Delete(String path) implements NamespaceChange {
        @Override
        public boolean touches(String directory) {
            return parent(path).equals(directory);
        }
    }

    record Rename(String from, String to) implements NamespaceChange {
        @Override
        public boolean touches(String directory) {
            return parent(from).equals(directory) || parent(to).equals(directory);
        }
    }

    static String parent(String path) {
        var index = path.lastIndexOf('/');
        return index <= 0 ? "/" : path.substring(0, index);
    }

    private static String child(String directory, String name) {
        return directory.equals("/") ? "/" + name : directory + "/" + name;
    }

    /// Counts a change, and stops the process when it is the one [#stopAfter(long)] named.
    ///
    /// @throws IOException if the process has stopped
    void change() throws IOException {
        alive();
        if (remaining-- <= 0) {
            stopped = true;
            throw stoppedError();
        }
        operations++;
    }

    /// Throws if the process has stopped.
    void alive() throws IOException {
        if (stopped) {
            throw stoppedError();
        }
    }

    private static IOException stoppedError() {
        return new IOException("The simulated process has stopped");
    }

    void stopAfter(long changes) {
        remaining = changes;
    }

    void stopAtForce(long successfulForces) {
        remainingForces = successfulForces;
    }

    long operations() {
        return operations;
    }

    long forces() {
        return forces;
    }

    /// Counts a force, and stops the process when it is the one [#stopAtForce(long)] named.
    private void forced() throws IOException {
        if (remainingForces-- <= 0) {
            stopped = true;
            throw stoppedError();
        }
        change();
        forces++;
    }

    boolean stopped() {
        return stopped;
    }

    Node node(String path) {
        return names.get(path);
    }

    /// {@return the names of the entries of `directory`}
    List<String> list(String directory) throws IOException {
        alive();
        if (!(names.get(directory) instanceof DirectoryNode)) {
            throw names.containsKey(directory) ? new NotDirectoryException(directory) : new NoSuchFileException(directory);
        }
        var prefix = directory.equals("/") ? "/" : directory + "/";
        var children = new ArrayList<String>();
        for (var path : names.keySet()) {
            if (path.length() > prefix.length() && path.startsWith(prefix) && path.indexOf('/', prefix.length()) < 0) {
                children.add(path.substring(prefix.length()));
            }
        }
        children.sort(null);
        return children;
    }

    /// Creates `node` at `path`, whose parent must be a directory.
    void create(String path, Node node) throws IOException {
        change();
        if (names.containsKey(path)) {
            throw new FileAlreadyExistsException(path);
        }
        if (!(names.get(parent(path)) instanceof DirectoryNode)) {
            throw new NoSuchFileException(parent(path));
        }
        names.put(path, node);
        pending.add(new Create(path, node));
    }

    void delete(String path) throws IOException {
        change();
        var node = names.get(path);
        if (node == null) {
            throw new NoSuchFileException(path);
        }
        if (node instanceof DirectoryNode && !list(path).isEmpty()) {
            throw new DirectoryNotEmptyException(path);
        }
        names.remove(path);
        pending.add(new Delete(path));
    }

    /// Renames the file at `from` to `to`, replacing what is there.
    void rename(String from, String to) throws IOException {
        change();
        var node = names.get(from);
        if (node == null) {
            throw new NoSuchFileException(from);
        }
        if (node instanceof DirectoryNode) {
            throw new IOException("Renaming a directory is not simulated: " + from);
        }
        if (!(names.get(parent(to)) instanceof DirectoryNode)) {
            throw new NoSuchFileException(parent(to));
        }
        if (names.get(to) instanceof DirectoryNode) {
            throw new FileAlreadyExistsException(to);
        }
        names.remove(from);
        names.put(to, node);
        pending.add(new Rename(from, to));
    }

    /// Makes the changes of the namespace up to the last one touching `directory` durable, as a journal commit would.
    void forceDirectory(String directory) throws IOException {
        alive();
        forced();
        var last = -1;
        for (var i = 0; i < pending.size(); i++) {
            if (pending.get(i).touches(directory)) {
                last = i;
            }
        }
        for (var i = 0; i <= last; i++) {
            apply(durableNames, pending.getFirst());
            pending.removeFirst();
        }
    }

    private static void apply(Map<String, Node> names, NamespaceChange change) {
        switch (change) {
            case Create create -> names.put(create.path(), create.node());
            case Delete delete -> names.remove(delete.path());
            case Rename rename -> {
                var node = names.remove(rename.from());
                if (node != null) {
                    names.put(rename.to(), node);
                }
            }
        }
    }

    /// Writes `bytes` into `file` at `offset`.
    void write(FileNode file, long offset, byte[] bytes) throws IOException {
        change();
        var end = offset + bytes.length;
        if (end > file.data.length) {
            file.data = Arrays.copyOf(file.data, (int) Math.max(end, file.data.length * 2L));
        }
        System.arraycopy(bytes, 0, file.data, (int) offset, bytes.length);
        file.size = Math.max(file.size, end);
        file.changes.add(new Change(offset, bytes, -1));
    }

    void truncate(FileNode file, long size) throws IOException {
        change();
        if (size < file.size) {
            Arrays.fill(file.data, (int) size, (int) file.size, (byte) 0);
            file.size = size;
            file.changes.add(new Change(0, null, size));
        }
    }

    /// Makes the contents of `file` durable.
    void force(FileNode file) throws IOException {
        alive();
        forced();
        file.durableData = Arrays.copyOf(file.data, (int) file.size);
        file.durableSize = file.size;
        file.changes.clear();
    }

    /// Makes the `length` bytes of `file` at `offset` durable, and a size that holds them, as a write through a
    /// channel opened with `DSYNC` is once it returns; the other writes since the last force stay at risk outside
    /// those bytes.
    ///
    /// A truncation not yet durable makes the whole file durable instead, as the journal commit of the size that a
    /// file system makes for such a write writes the file's data before it.
    void forceRange(FileNode file, long offset, long length) throws IOException {
        for (var change : file.changes) {
            if (change.truncation() >= 0) {
                force(file);
                return;
            }
        }
        alive();
        forced();
        var end = offset + length;
        if (end > file.durableSize) {
            file.durableData = Arrays.copyOf(file.durableData, (int) end);
            file.durableSize = end;
        }
        System.arraycopy(file.data, (int) offset, file.durableData, (int) offset, (int) length);
        var kept = new ArrayList<Change>(file.changes.size());
        for (var change : file.changes) {
            var from = change.offset();
            var to = from + change.bytes().length;
            if (to <= offset || from >= end) {
                kept.add(change);
                continue;
            }
            if (from < offset) {
                kept.add(new Change(from, Arrays.copyOfRange(change.bytes(), 0, (int) (offset - from)), -1));
            }
            if (to > end) {
                kept.add(new Change(end, Arrays.copyOfRange(change.bytes(), (int) (end - from), (int) (to - from)), -1));
            }
        }
        file.changes.clear();
        file.changes.addAll(kept);
    }

    /// Takes the lock of `file` for `holder`.
    ///
    /// @return whether it was free
    boolean lock(FileNode file, Object holder) throws IOException {
        alive();
        return locks.putIfAbsent(file, holder) == null;
    }

    void unlock(FileNode file, Object holder) {
        locks.remove(file, holder);
    }

    /// {@return a new disk holding what a loss of power now could leave, drawn from `seed`}
    ///
    /// The draws come from a [SplittableRandom], whose outputs spread even for neighbouring seeds; the first outputs
    /// of [Random] for neighbouring seeds are alike.
    Disk afterPowerLoss(long seed) {
        var random = new SplittableRandom(seed);
        var image = new HashMap<>(durableNames);
        var kept = random.nextInt(pending.size() + 1);
        for (var i = 0; i < kept; i++) {
            apply(image, pending.get(i));
        }
        var disk = new Disk();
        var copies = new IdentityHashMap<Node, Node>();
        for (var entry : image.entrySet()) {
            var copy = copies.computeIfAbsent(entry.getValue(), node -> switch (node) {
                case DirectoryNode _ -> new DirectoryNode();
                case FileNode file -> new FileNode(survivingContents(file, random));
            });
            disk.place(entry.getKey(), copy);
        }
        return disk;
    }

    /// {@return a new disk holding everything written now, as a crash of the process alone leaves it}
    Disk afterProcessCrash() {
        var disk = new Disk();
        var copies = new IdentityHashMap<Node, Node>();
        for (var entry : names.entrySet()) {
            var copy = copies.computeIfAbsent(entry.getValue(), node -> switch (node) {
                case DirectoryNode _ -> new DirectoryNode();
                case FileNode file -> new FileNode(Arrays.copyOf(file.data, (int) file.size));
            });
            disk.place(entry.getKey(), copy);
        }
        return disk;
    }

    private void place(String path, Node node) {
        names.put(path, node);
        durableNames.put(path, node);
        // An entry whose directory a power loss dropped is dropped too
        for (var directory = parent(path); !directory.equals("/"); directory = parent(directory)) {
            names.putIfAbsent(directory, new DirectoryNode());
            durableNames.putIfAbsent(directory, names.get(directory));
        }
    }

    /// {@return the contents of `file` a loss of power may leave: what was forced, then each change since kept,
    /// dropped or torn at sectors}
    private static byte[] survivingContents(FileNode file, SplittableRandom random) {
        var contents = Arrays.copyOf(file.durableData, (int) file.durableSize);
        var size = file.durableSize;
        for (var change : file.changes) {
            if (change.truncation() >= 0) {
                if (random.nextBoolean() && change.truncation() < size) {
                    size = change.truncation();
                    contents = Arrays.copyOf(contents, (int) size);
                }
                continue;
            }
            var end = change.offset() + change.bytes().length;
            var mode = random.nextInt(4);
            if (mode == 3 && random.nextInt(3) != 0) {
                // Dropped, data and size alike
                continue;
            }
            if (end > size) {
                // A write whose data a loss of power dropped can still leave the file grown, with zeros
                size = end;
                contents = Arrays.copyOf(contents, (int) size);
            }
            if (mode == 3) {
                continue;
            }
            for (var sector = change.offset() / SECTOR * SECTOR; sector < end; sector += SECTOR) {
                if (mode == 2 && random.nextBoolean()) {
                    // Torn: this sector kept the bytes it had
                    continue;
                }
                var from = Math.max(sector, change.offset());
                var to = Math.min(sector + SECTOR, end);
                System.arraycopy(change.bytes(), (int) (from - change.offset()), contents, (int) from, (int) (to - from));
            }
        }
        return contents;
    }

    /// {@return the path of the entry `name` of `directory`}
    static String resolve(String directory, String name) {
        return child(directory, name);
    }
}

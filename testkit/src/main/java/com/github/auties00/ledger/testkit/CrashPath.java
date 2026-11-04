package com.github.auties00.ledger.testkit;

import java.net.URI;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/// A path of a [CrashFileSystem]: names separated by `/`, absolute when it starts with one.
final class CrashPath implements Path {
    private final CrashFileSystem fileSystem;
    private final boolean absolute;
    private final List<String> names;

    CrashPath(CrashFileSystem fileSystem, boolean absolute, List<String> names) {
        this.fileSystem = fileSystem;
        this.absolute = absolute;
        this.names = List.copyOf(names);
    }

    /// {@return the path that `text` names}
    static CrashPath parse(CrashFileSystem fileSystem, String text) {
        var names = new ArrayList<String>();
        for (var name : text.split("/")) {
            if (!name.isEmpty()) {
                names.add(name);
            }
        }
        return new CrashPath(fileSystem, text.startsWith("/"), names);
    }

    /// {@return the absolute, normalized form of this path, as the disk keys its nodes}
    String key() {
        var normal = (CrashPath) toAbsolutePath().normalize();
        return normal.names.isEmpty() ? "/" : "/" + String.join("/", normal.names);
    }

    private static CrashPath cast(Path path) {
        if (!(path instanceof CrashPath crash)) {
            throw new ProviderMismatchException();
        }
        return crash;
    }

    @Override
    public CrashFileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return absolute;
    }

    @Override
    public Path getRoot() {
        return absolute ? new CrashPath(fileSystem, true, List.of()) : null;
    }

    @Override
    public Path getFileName() {
        return names.isEmpty() ? null : new CrashPath(fileSystem, false, List.of(names.getLast()));
    }

    @Override
    public Path getParent() {
        if (names.isEmpty() || (!absolute && names.size() == 1)) {
            return absolute && !names.isEmpty() ? getRoot() : null;
        }
        return new CrashPath(fileSystem, absolute, names.subList(0, names.size() - 1));
    }

    @Override
    public int getNameCount() {
        return names.size();
    }

    @Override
    public Path getName(int index) {
        return new CrashPath(fileSystem, false, List.of(names.get(index)));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        return new CrashPath(fileSystem, false, names.subList(beginIndex, endIndex));
    }

    @Override
    public boolean startsWith(Path other) {
        var path = cast(other);
        return path.absolute == absolute && path.names.size() <= names.size()
                && names.subList(0, path.names.size()).equals(path.names);
    }

    @Override
    public boolean endsWith(Path other) {
        var path = cast(other);
        if (path.absolute) {
            return equals(path);
        }
        return path.names.size() <= names.size() && names.subList(names.size() - path.names.size(), names.size()).equals(path.names);
    }

    @Override
    public Path normalize() {
        var normal = new ArrayList<String>();
        for (var name : names) {
            if (name.equals(".")) {
                continue;
            }
            if (name.equals("..") && !normal.isEmpty() && !normal.getLast().equals("..")) {
                normal.removeLast();
            } else if (!(name.equals("..") && absolute)) {
                normal.add(name);
            }
        }
        return new CrashPath(fileSystem, absolute, normal);
    }

    @Override
    public Path resolve(Path other) {
        var path = cast(other);
        if (path.absolute) {
            return path;
        }
        var joined = new ArrayList<>(names);
        joined.addAll(path.names);
        return new CrashPath(fileSystem, absolute, joined);
    }

    @Override
    public Path resolve(String other) {
        return resolve(parse(fileSystem, other));
    }

    @Override
    public Path relativize(Path other) {
        var path = cast(other);
        if (path.absolute != absolute) {
            throw new IllegalArgumentException("Only paths of the same kind relativize: " + this + ", " + other);
        }
        var common = 0;
        while (common < names.size() && common < path.names.size() && names.get(common).equals(path.names.get(common))) {
            common++;
        }
        var relative = new ArrayList<String>();
        for (var i = common; i < names.size(); i++) {
            relative.add("..");
        }
        relative.addAll(path.names.subList(common, path.names.size()));
        return new CrashPath(fileSystem, false, relative);
    }

    @Override
    public URI toUri() {
        return URI.create(CrashFileSystem.SCHEME + "://" + key());
    }

    @Override
    public Path toAbsolutePath() {
        return absolute ? this : new CrashPath(fileSystem, true, names);
    }

    @Override
    public Path toRealPath(LinkOption... options) {
        return toAbsolutePath().normalize();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
        throw new UnsupportedOperationException("A crash file system has no watch service");
    }

    @Override
    public Iterator<Path> iterator() {
        var paths = new ArrayList<Path>(names.size());
        for (var i = 0; i < names.size(); i++) {
            paths.add(getName(i));
        }
        return paths.iterator();
    }

    @Override
    public int compareTo(Path other) {
        return toString().compareTo(cast(other).toString());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof CrashPath path && path.fileSystem == fileSystem && path.absolute == absolute
                && path.names.equals(names);
    }

    @Override
    public int hashCode() {
        return Objects.hash(absolute, names);
    }

    @Override
    public String toString() {
        var joined = String.join("/", names);
        return absolute ? "/" + joined : joined;
    }

    /// {@return the names of this path}
    List<String> names() {
        return names;
    }
}

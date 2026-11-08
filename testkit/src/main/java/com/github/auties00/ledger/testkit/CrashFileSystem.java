package com.github.auties00.ledger.testkit;

import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/// A file system in memory that keeps what a force made durable apart from what was written since, to test what a
/// store survives a loss of power or a crash of its process.
///
/// Run the store on a path of this file system, then take the state a failure would leave: [#afterPowerLoss(Random)]
/// keeps what was forced and a random draw of the rest, as a power cut may; [#afterProcessCrash()] keeps everything
/// written, as the operating system does when only the process dies. Either returns a new file system, on which the
/// store opens again and recovers. [#stopAfter(long)] makes the failure happen at a chosen point: the change it names,
/// and every operation after it, throws, as if the process had stopped there.
///
/// {@snippet lang = java :
/// var disk = CrashFileSystem.create();
/// disk.stopAfter(1_000);
/// runWorkload(disk.getPath("/store"));            // fails at the 1,000th change
/// var survivor = disk.afterPowerLoss(new Random(seed));
/// checkInvariants(survivor.getPath("/store"));    // what the store recovers
/// }
///
/// # What a loss of power keeps
/// The model journals metadata in order and caches file data, as ext4, XFS, APFS and NTFS do, and assumes no more
/// than POSIX promises. A write is durable once a force of its file returns, or once it returns if its channel was
/// opened with [java.nio.file.StandardOpenOption#DSYNC] or [java.nio.file.StandardOpenOption#SYNC]; until then it may
/// be kept, dropped, or kept in some of its 512-byte sectors only, in any combination with the other writes since that
/// force, and a dropped write may still leave the file grown, with zeros. A write of a `DSYNC` channel counts as a
/// force for [#stopAtForce(long)], and the interceptor sees it as a write and then a force. A truncation is durable
/// once a force of its file returns.
/// Creating, renaming and deleting are durable once a force of a directory they touch returns (a channel opened on the
/// directory for reading, then forced); a loss of power keeps a prefix, in order, of those not forced, and a file
/// whose entry it drops is gone, with its data.
///
/// # Limits
/// Only what an embedded store uses is simulated: files, directories, positional and gathering channels, file
/// locks, atomic renames of files. Symbolic links, attributes other than the basic ones, file stores, watch services,
/// memory mapping and renaming directories throw [UnsupportedOperationException] or [java.io.IOException].
///
/// # Threading
/// Every operation is safe from any thread; each runs alone.
public final class CrashFileSystem extends FileSystem {
    static final String SCHEME = "ledger-crash";

    private final Disk disk;
    private final CrashFileSystemProvider provider;
    private volatile boolean open;
    private volatile ChangeInterceptor interceptor;

    private CrashFileSystem(Disk disk) {
        this.disk = disk;
        this.provider = new CrashFileSystemProvider(this);
        this.open = true;
        this.interceptor = (_, _) -> {
        };
    }

    /// {@return a new, empty file system}
    public static CrashFileSystem create() {
        return new CrashFileSystem(new Disk());
    }

    Disk disk() {
        return disk;
    }

    /// Stops the simulated process at the `changes`-th change from now: that change and every operation after it
    /// throw [java.io.IOException], and nothing changes any more.
    ///
    /// A change is a write, a truncation, a force, or creating, renaming or deleting an entry.
    ///
    /// @param changes the changes that still succeed, at least 0
    /// @throws IllegalArgumentException if `changes` is negative
    public void stopAfter(long changes) {
        if (changes < 0) {
            throw new IllegalArgumentException("changes must not be negative: " + changes);
        }
        synchronized (disk.lock) {
            disk.stopAfter(changes);
        }
    }

    /// Stops the simulated process at the force after `forces` more forces: that force and every operation after it
    /// throw [java.io.IOException], so the writes it would have made durable are left to a loss of power.
    ///
    /// The forces are where a store's durability turns, so a failure there finds what ordering a store relies on.
    ///
    /// @param forces the forces that still succeed, at least 0
    /// @throws IllegalArgumentException if `forces` is negative
    public void stopAtForce(long forces) {
        if (forces < 0) {
            throw new IllegalArgumentException("forces must not be negative: " + forces);
        }
        synchronized (disk.lock) {
            disk.stopAtForce(forces);
        }
    }

    /// Shows every later change to `interceptor` before it is made, replacing the interceptor set before.
    ///
    /// @param interceptor sees each change, and may fail or delay it
    public void intercept(ChangeInterceptor interceptor) {
        this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
    }

    /// Runs the interceptor before a change of the entry at `key`.
    void beforeChange(String key, ChangeInterceptor.ChangeKind kind) throws java.io.IOException {
        interceptor.beforeChange(CrashPath.parse(this, key), kind);
    }

    /// {@return the forces made so far, of files and of directories}
    public long forces() {
        synchronized (disk.lock) {
            return disk.forces();
        }
    }

    /// {@return the changes made so far}
    public long changes() {
        synchronized (disk.lock) {
            return disk.operations();
        }
    }

    /// {@return whether the simulated process has stopped}
    public boolean stopped() {
        synchronized (disk.lock) {
            return disk.stopped();
        }
    }

    /// {@return a new file system holding what a loss of power at this moment could leave}
    ///
    /// Each call draws another possible state from `random`; the same seed draws the same state.
    ///
    /// @param random draws which writes and namespace changes not made durable survive
    public CrashFileSystem afterPowerLoss(Random random) {
        Objects.requireNonNull(random, "random");
        synchronized (disk.lock) {
            return new CrashFileSystem(disk.afterPowerLoss(random.nextLong()));
        }
    }

    /// {@return a new file system holding everything written so far, as a crash of the process alone leaves it}
    public CrashFileSystem afterProcessCrash() {
        synchronized (disk.lock) {
            return new CrashFileSystem(disk.afterProcessCrash());
        }
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return List.of(CrashPath.parse(this, "/"));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return List.of();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Set.of("basic");
    }

    @Override
    public Path getPath(String first, String... more) {
        Objects.requireNonNull(first, "first");
        return CrashPath.parse(this, more.length == 0 ? first : first + "/" + String.join("/", more));
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException("A crash file system matches no patterns");
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("A crash file system has no users");
    }

    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("A crash file system has no watch service");
    }
}

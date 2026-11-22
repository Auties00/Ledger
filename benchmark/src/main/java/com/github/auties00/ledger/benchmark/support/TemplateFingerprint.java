package com.github.auties00.ledger.benchmark.support;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/// The shape of the store templates this JVM used: the size and name of every file, which two builds of one template
/// share when they laid it out alike (the bytes differ anyway, as every checkpoint draws its own nonces).
///
/// Each template is printed once per JVM, as a line starting with `[template]`, the first time it is used, ending with
/// the mode it was built in (`mode=ordered` or `mode=timed`, which its `.complete` marker holds, so the marker's size
/// in the listing tells the modes apart too), and the line of a template this JVM built gives the build time. The memory counters report the shape of the last template
/// as `templateBytes`, `templateFiles` and `templateShape`, once per JVM, in its first measured iteration, so that
/// their score is the value of one JVM rather than a sum over iterations.
public final class TemplateFingerprint {
    /// The bits of [Shape#hash()] kept, so that the hash survives JMH's conversion of counters to `double`.
    private static final long HASH_MASK = (1L << 52) - 1;
    private static final Set<Path> PRINTED = ConcurrentHashMap.newKeySet();
    private static final AtomicBoolean TAKEN = new AtomicBoolean();
    private static volatile Shape last;

    private TemplateFingerprint() {
    }

    /// The shape of one template.
    ///
    /// @param bytes the total size of its files
    /// @param files the number of its files
    /// @param hash a 52-bit FNV-1a hash of [#listing()], with the files under `log/` hashed by their total size alone:
    ///             the number of the segment a closed Ledger store keeps for its next open counts the segments the
    ///             log made, which its background spares make depend on thread timing (16 or 17 for one build of
    ///             the 200,000-message template on two runners, whose data files were equal), and reading the copy
    ///             never depends on it
    /// @param listing every file's path relative to the template and its size, in path order
    public record Shape(long bytes, int files, long hash, String listing) {
    }

    /// Prints the build time of `template`, which this JVM just built.
    ///
    /// @param nanos the build time
    static void built(Path template, long nanos) {
        System.out.printf("[template] built %s in %d ms (%s)%n", template.getFileName(), nanos / 1_000_000,
                Stores.TIMED_TEMPLATES ? "timed" : "ordered");
    }

    /// Records `template` as the last one this JVM used, printing its shape if this JVM has not printed it yet.
    ///
    /// @throws IOException if the template's tree cannot be walked
    static void used(Path template) throws IOException {
        var shape = shapeOf(template);
        last = shape;
        if (PRINTED.add(template)) {
            System.out.printf("[template] %s bytes=%d files=%d shape=%013x %s mode=%s%n", template.getFileName(),
                    shape.bytes(), shape.files(), shape.hash(), shape.listing(), Stores.TEMPLATE_MODE);
        }
    }

    /// {@return the shape of the last template this JVM used, the first time it is called after one was used, then
    /// `null`}
    public static Shape take() {
        var shape = last;
        return shape != null && TAKEN.compareAndSet(false, true) ? shape : null;
    }

    /// {@return the shape of the tree under `template`}
    ///
    /// @throws IOException if the tree cannot be walked
    public static Shape shapeOf(Path template) throws IOException {
        var sizes = new TreeMap<String, Long>();
        try (var files = Files.walk(template)) {
            for (var file : (Iterable<Path>) files::iterator) {
                if (Files.isRegularFile(file)) {
                    sizes.put(template.relativize(file).toString().replace(File.separatorChar, '/'), Files.size(file));
                }
            }
        }
        var listing = new StringBuilder();
        var bytes = 0L;
        for (var entry : sizes.entrySet()) {
            if (!listing.isEmpty()) {
                listing.append(',');
            }
            listing.append(entry.getKey()).append('=').append(entry.getValue());
            bytes += entry.getValue();
        }
        var hashed = new StringBuilder();
        var logBytes = 0L;
        for (var entry : sizes.entrySet()) {
            if (entry.getKey().startsWith("log/")) {
                logBytes += entry.getValue();
            } else {
                hashed.append(entry.getKey()).append('=').append(entry.getValue()).append(',');
            }
        }
        hashed.append("log=").append(logBytes);
        var hash = 0xcbf29ce484222325L;
        for (var b : hashed.toString().getBytes(StandardCharsets.UTF_8)) {
            hash = (hash ^ (b & 0xff)) * 0x100000001b3L;
        }
        return new Shape(bytes, sizes.size(), hash & HASH_MASK, listing.toString());
    }
}

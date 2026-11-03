package com.github.auties00.ledger.benchmark.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

/// File tree helpers for the directories engines own.
public final class Directories {
    private Directories() {
    }

    /// {@return the total size of the regular files under `directory`, or 0 if it does not exist}
    ///
    /// @param directory the directory to measure
    /// @throws IOException if the tree cannot be walked
    public static long size(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return 0;
        }
        try (var files = Files.walk(directory)) {
            return files.filter(Files::isRegularFile).mapToLong(file -> {
                try {
                    return Files.size(file);
                } catch (NoSuchFileException _) {
                    // Engines delete files while they compact
                    return 0;
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            }).sum();
        } catch (UncheckedIOException exception) {
            throw exception.getCause();
        }
    }

    /// Copies the tree under `source` to `target`, which must not exist.
    ///
    /// @param source the directory to copy
    /// @param target the directory to create
    /// @throws IOException if a file cannot be copied
    public static void copy(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Files.createDirectories(target.resolve(source.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, target.resolve(source.relativize(file)), StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /// Deletes `directory` and everything under it; does nothing if it does not exist.
    ///
    /// @param directory the directory to delete
    /// @throws IOException if a file cannot be deleted
    public static void delete(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exception) throws IOException {
                if (exception != null) {
                    throw exception;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}

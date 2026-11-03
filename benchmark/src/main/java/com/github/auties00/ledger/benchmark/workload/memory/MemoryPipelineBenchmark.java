package com.github.auties00.ledger.benchmark.workload.memory;

import com.github.auties00.ledger.benchmark.support.Directories;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/// Where Ledger keeps the bytes it encrypts and writes: on the heap or in native memory.
///
/// In JDK 26, AES-GCM runs its intrinsic directly only on buffers backed by arrays, and copies native buffers
/// through 768-byte arrays; file I/O copies heap buffers through a temporary native buffer and passes native buffers
/// straight to the OS. Every page of an encrypted store goes through both, so one side copies whichever memory the
/// store uses. Each benchmark runs the same code with `memory` set to `heap` (array-backed buffers) or `native`
/// (buffers over segments of an arena), so the difference is the memory alone:
///
/// - [#seal()] and [#open()] encrypt or decrypt one page into another buffer;
/// - [#sealAndWrite()] seals a page and writes it at a cycling position of a file, as the log and the checkpoint do;
/// - [#readAndOpen()] reads a sealed page from the file and decrypts it, as a page cache miss does;
/// - [#copyToNative()] copies one page from the heap to native memory, the price of one copy.
///
/// Writes are not forced: a force costs the same with either memory and would hide the difference. The operating
/// system's page cache serves the reads, so they measure the copy and decryption, not the device.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class MemoryPipelineBenchmark {
    private static final int TAG_BYTES = 16;
    private static final int SLOTS = 1_024;

    /// Where the buffers live: `heap` or `native`.
    @Param({"heap", "native"})
    public String memory;

    /// The size of a page.
    @Param({"4096", "16384", "65536"})
    public int pageSize;

    private Arena arena;
    private Cipher cipher;
    private SecretKeySpec key;
    private byte[] nonce;
    private long counter;
    private ByteBuffer page;
    private ByteBuffer sealed;
    private ByteBuffer opened;
    private MemorySegment heapPage;
    private MemorySegment nativePage;
    private Path file;
    private FileChannel channel;
    private int slot;

    /// Allocates the buffers, seals one page and fills the file with sealed pages.
    ///
    /// @throws IOException if the file cannot be written
    /// @throws GeneralSecurityException if AES-GCM is unavailable
    @Setup(Level.Trial)
    public void setUp() throws IOException, GeneralSecurityException {
        arena = Arena.ofShared();
        var keyBytes = new byte[32];
        var random = new SplittableRandom(17);
        random.nextBytes(keyBytes);
        key = new SecretKeySpec(keyBytes, "AES");
        cipher = Cipher.getInstance("AES/GCM/NoPadding");
        nonce = new byte[12];
        page = allocate(pageSize);
        sealed = allocate(pageSize + TAG_BYTES);
        opened = allocate(pageSize);
        var content = new byte[pageSize];
        random.nextBytes(content);
        page.put(0, content);
        heapPage = MemorySegment.ofArray(content);
        nativePage = arena.allocate(pageSize);

        file = Stores.root().resolve("run").resolve("memory-" + UUID.randomUUID()).resolve("pages");
        Files.createDirectories(file.getParent());
        channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        for (var i = 0; i < SLOTS; i++) {
            seal();
            sealed.clear();
            while (sealed.hasRemaining()) {
                channel.write(sealed, (long) i * (pageSize + TAG_BYTES) + sealed.position());
            }
        }
    }

    private ByteBuffer allocate(int size) {
        return switch (memory) {
            case "heap" -> ByteBuffer.wrap(new byte[size]);
            case "native" -> arena.allocate(size).asByteBuffer();
            default -> throw new IllegalArgumentException("memory is heap or native: " + memory);
        };
    }

    /// Closes the file, deletes it and frees the native memory.
    ///
    /// @throws IOException if the file cannot be deleted
    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        channel.close();
        Directories.delete(file.getParent());
        arena.close();
    }

    private void nextNonce() {
        counter++;
        for (var i = 0; i < 8; i++) {
            nonce[4 + i] = (byte) (counter >>> (56 - 8 * i));
        }
    }

    /// Encrypts one page into the sealed buffer.
    ///
    /// @return the sealed bytes
    /// @throws GeneralSecurityException if encryption fails
    @Benchmark
    public int seal() throws GeneralSecurityException {
        nextNonce();
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BYTES * 8, nonce));
        page.clear();
        sealed.clear();
        return cipher.doFinal(page, sealed);
    }

    /// Decrypts the sealed buffer into a page.
    ///
    /// @return the opened bytes
    /// @throws GeneralSecurityException if decryption fails
    @Benchmark
    public int open() throws GeneralSecurityException {
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BYTES * 8, nonce));
        sealed.clear();
        opened.clear();
        return cipher.doFinal(sealed, opened);
    }

    /// Seals one page and writes it to the file without forcing it.
    ///
    /// @return the bytes written
    /// @throws GeneralSecurityException if encryption fails
    /// @throws IOException if the write fails
    @Benchmark
    public int sealAndWrite() throws GeneralSecurityException, IOException {
        seal();
        sealed.flip();
        var position = (long) (slot++ % SLOTS) * (pageSize + TAG_BYTES);
        var written = 0;
        while (sealed.hasRemaining()) {
            written += channel.write(sealed, position + written);
        }
        return written;
    }

    /// Reads one sealed page from the file and decrypts it.
    ///
    /// @return the opened bytes
    /// @throws GeneralSecurityException if decryption fails
    /// @throws IOException if the read fails
    @Benchmark
    public int readAndOpen() throws GeneralSecurityException, IOException {
        sealed.clear();
        var read = 0;
        while (sealed.hasRemaining()) {
            var n = channel.read(sealed, read);
            if (n < 0) {
                throw new IOException("The file ended early");
            }
            read += n;
        }
        // Every iteration reads slot 0, sealed with nonce 1 when the file was filled
        nonce[11] = 1;
        for (var i = 4; i < 11; i++) {
            nonce[i] = 0;
        }
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BYTES * 8, nonce));
        sealed.flip();
        opened.clear();
        return cipher.doFinal(sealed, opened);
    }

    /// Copies one page from the heap to native memory.
    ///
    /// @return the native page
    @Benchmark
    public MemorySegment copyToNative() {
        MemorySegment.copy(heapPage, 0, nativePage, 0, pageSize);
        return nativePage;
    }
}

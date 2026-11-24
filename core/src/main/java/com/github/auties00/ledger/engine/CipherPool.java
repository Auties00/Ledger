package com.github.auties00.ledger.engine;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/// The idle ciphers of the process, per transformation, which the stores take from when they need a cipher they have
/// none of, and give theirs back to as they close.
///
/// A cipher made by `Cipher.getInstance` looks the transformation up among the security providers, and its first
/// initialization chooses the provider by reflection, a cost an open would pay for each cipher of its pools, its log
/// writer and its recovery if each made its own. A store keeps its own pools, which every seal and open
/// takes from and gives back to, so a store never shares a cipher with another while it is open and never re-expands
/// the key of another's; this pool only seeds them and takes back what is idle when an owner is done with it, so a
/// store opened after another closed finds its ciphers made.
///
/// A cipher given back is first initialized to encrypt under a key of the process's own and a nonce never used before
/// ([#giveBack(Cipher)]): the JDK's AES-GCM keeps the key and nonce of the last encryption to refuse a second one under
/// both, and keeps the expanded key of the last initialization of either kind, so the store's key and its last nonce
/// leave the cipher before another store takes it. Two copies of a store, whose large values are sealed under one key
/// with nonces that start from the same counter at each open, would otherwise find a cipher refuse their first seal.
/// The pool keeps no other key of a store: a cipher holds that of the process until a store initializes it again.
///
/// An owner gives a cipher back only once nothing uses it, and only if no call on it failed other than by
/// authentication. It is bounded: [#IDLE_PER_PROCESSOR] ciphers per processor per transformation, the excess left to the
/// collector, so the heap of a process that opened many stores at once keeps no more than one with a few open does.
///
/// It is safe for concurrent use.
final class CipherPool {
    /// The idle ciphers a transformation keeps per processor: the ciphers of a store's pages, those its loader threads
    /// and readers take, its log writer's and its recovery's.
    static final int IDLE_PER_PROCESSOR = 2;
    /// The ciphers of AES-256-GCM, which seal and open the units and the log's frames.
    static final CipherPool GCM = new CipherPool("AES/GCM/NoPadding");
    /// The ciphers of AES-256-CTR, which read back values of the log the spill queue moved there.
    static final CipherPool CTR = new CipherPool("AES/CTR/NoPadding");

    private final String transformation;
    private final ConcurrentLinkedQueue<Cipher> idle;
    // The ciphers idle holds, about: counted after the offer and before the poll, so it may read one more than it holds
    private final AtomicInteger held;
    private final int bound;
    // The ciphers made because none was idle; for tests
    private final LongAdder made;

    private CipherPool(String transformation) {
        this.transformation = transformation;
        this.idle = new ConcurrentLinkedQueue<>();
        this.held = new AtomicInteger();
        this.bound = IDLE_PER_PROCESSOR * Runtime.getRuntime().availableProcessors();
        this.made = new LongAdder();
    }

    /// {@return a cipher of the transformation, idle or made now, which the caller initializes before each use}
    ///
    /// @throws IllegalStateException if the JDK has no cipher of the transformation
    Cipher take() {
        var cipher = idle.poll();
        if (cipher != null) {
            held.decrementAndGet();
            return cipher;
        }
        made.increment();
        try {
            return Cipher.getInstance(transformation);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException(transformation + " is unavailable", exception);
        }
    }

    /// Gives back `cipher`, a cipher of the transformation that nothing uses any more and whose last call did not fail
    /// other than by authentication, or leaves it to the collector if the pool holds as many as it keeps.
    ///
    /// The cipher is initialized to encrypt under the process's key and a nonce of its own first, which replaces the
    /// key and nonce it kept.
    void giveBack(Cipher cipher) {
        if (cipher == null || held.get() >= bound) {
            return;
        }
        try {
            Scrub.reset(cipher);
        } catch (GeneralSecurityException | RuntimeException _) {
            // A cipher that cannot be initialized is not handed out again
            return;
        }
        idle.offer(cipher);
        held.incrementAndGet();
    }

    /// {@return the ciphers made since the process started because the pool held none; for tests}
    long madeForTests() {
        return made.sum();
    }

    /// {@return the ciphers the pool keeps at most}
    int bound() {
        return bound;
    }

    /// {@return the ciphers the pool holds; for tests}
    int idleForTests() {
        return idle.size();
    }

    /// Takes every idle cipher out of the pool; for tests.
    void clearForTests() {
        while (idle.poll() != null) {
            held.decrementAndGet();
        }
    }

    /// The key and nonces a cipher given back is initialized with: drawn and counted for the process alone, never
    /// stored, so no store's key or nonce stays in an idle cipher and no initialization repeats an earlier one.
    private static final class Scrub {
        private static final SecretKeySpec KEY;
        private static final AtomicLong NONCES = new AtomicLong();

        static {
            var key = new byte[32];
            new SecureRandom().nextBytes(key);
            KEY = new SecretKeySpec(key, "AES");
        }

        /// Initializes `cipher` to encrypt under the process's key and a nonce no earlier reset used.
        static void reset(Cipher cipher) throws GeneralSecurityException {
            var nonce = NONCES.getAndIncrement();
            if (cipher.getAlgorithm().startsWith("AES/GCM")) {
                var iv = new byte[12];
                Bytes.i64(iv, 0, nonce);
                cipher.init(Cipher.ENCRYPT_MODE, KEY, new GCMParameterSpec(128, iv));
            } else {
                var iv = new byte[16];
                Bytes.i64(iv, 0, nonce);
                cipher.init(Cipher.ENCRYPT_MODE, KEY, new IvParameterSpec(iv));
            }
        }
    }
}

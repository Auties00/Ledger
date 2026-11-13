package com.github.auties00.ledger.benchmark.engine;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/// Seals every value with AES-256-GCM before it reaches the wrapped engine, as an application must to store data
/// authenticated and encrypted in an engine that cannot.
///
/// A sealed value is a 12-byte nonce followed by the ciphertext and its 16-byte tag; the key is the associated data,
/// so a value moved under another key fails to open. Keys stay in plaintext, which is as much as an application can
/// do without losing their order.
public final class SealedEngine implements Engine {
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_BITS = 128;

    private final Engine delegate;
    private final SecretKeySpec key;
    private final long noncePrefix;
    private final AtomicLong nonceCounter;
    private final ConcurrentLinkedQueue<Cipher> ciphers;

    /// Wraps `delegate`.
    ///
    /// Every instance seals under the same fixed key, so a store sealed by one opens in the next; each draws a random
    /// nonce prefix, which keeps nonces apart across instances.
    ///
    /// @param delegate the engine that stores the sealed values
    public SealedEngine(Engine delegate) {
        this.delegate = delegate;
        var keyBytes = new byte[32];
        for (var i = 0; i < keyBytes.length; i++) {
            keyBytes[i] = (byte) (i * 31 + 7);
        }
        this.key = new SecretKeySpec(keyBytes, "AES");
        this.noncePrefix = new SecureRandom().nextInt() & 0xFFFFFFFFL;
        this.nonceCounter = new AtomicLong();
        this.ciphers = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void write(WriteBatch batch, Durability durability) throws IOException {
        var sealed = new WriteBatch();
        for (var i = 0; i < batch.size(); i++) {
            var value = batch.value(i);
            if (value == null) {
                sealed.delete(batch.keyspace(i), batch.key(i));
            } else {
                sealed.put(batch.keyspace(i), batch.key(i), seal(batch.key(i), value));
            }
        }
        delegate.write(sealed, durability);
    }

    @Override
    public byte[] get(Keyspace keyspace, byte[] key) throws IOException {
        var value = delegate.get(keyspace, key);
        return value == null ? null : open(key, value);
    }

    @Override
    public int scan(Keyspace keyspace, byte[] from, byte[] to, boolean descending, int limit, EntryVisitor visitor) throws IOException {
        try {
            return delegate.scan(keyspace, from, to, descending, limit, (key, value) -> {
                try {
                    return visitor.visit(key, open(key, value));
                } catch (IOException exception) {
                    throw new SealException(exception);
                }
            });
        } catch (SealException exception) {
            throw exception.cause;
        }
    }

    @Override
    public void deleteRange(Keyspace keyspace, byte[] from, byte[] to, Durability durability) throws IOException {
        delegate.deleteRange(keyspace, from, to, durability);
    }

    @Override
    public ReadView openReadView() throws IOException {
        var view = delegate.openReadView();
        return new ReadView() {
            @Override
            public byte[] get(Keyspace keyspace, byte[] key) throws IOException {
                var value = view.get(keyspace, key);
                return value == null ? null : open(key, value);
            }

            @Override
            public void close() throws IOException {
                view.close();
            }
        };
    }

    @Override
    public void checkpoint() throws IOException {
        delegate.checkpoint();
    }

    @Override
    public long sizeOnDisk() throws IOException {
        return delegate.sizeOnDisk();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private byte[] seal(byte[] associatedData, byte[] plaintext) throws IOException {
        var cipher = acquire();
        try {
            var nonce = ByteBuffer.allocate(NONCE_LENGTH)
                    .putInt((int) noncePrefix)
                    .putLong(nonceCounter.getAndIncrement())
                    .array();
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(associatedData);
            var sealed = new byte[NONCE_LENGTH + cipher.getOutputSize(plaintext.length)];
            System.arraycopy(nonce, 0, sealed, 0, NONCE_LENGTH);
            cipher.doFinal(plaintext, 0, plaintext.length, sealed, NONCE_LENGTH);
            return sealed;
        } catch (GeneralSecurityException exception) {
            throw new IOException(exception);
        } finally {
            ciphers.add(cipher);
        }
    }

    private byte[] open(byte[] associatedData, byte[] sealed) throws IOException {
        var cipher = acquire();
        try {
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, sealed, 0, NONCE_LENGTH));
            cipher.updateAAD(associatedData);
            return cipher.doFinal(sealed, NONCE_LENGTH, sealed.length - NONCE_LENGTH);
        } catch (AEADBadTagException exception) {
            throw new IOException("Value under a key does not authenticate", exception);
        } catch (GeneralSecurityException exception) {
            throw new IOException(exception);
        } finally {
            ciphers.add(cipher);
        }
    }

    private Cipher acquire() throws IOException {
        var cipher = ciphers.poll();
        if (cipher != null) {
            return cipher;
        }
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (GeneralSecurityException exception) {
            throw new IOException(exception);
        }
    }

    /// Carries an [IOException] out of a scan visitor.
    private static final class SealException extends RuntimeException {
        private final IOException cause;

        SealException(IOException cause) {
            super(cause);
            this.cause = cause;
        }
    }
}

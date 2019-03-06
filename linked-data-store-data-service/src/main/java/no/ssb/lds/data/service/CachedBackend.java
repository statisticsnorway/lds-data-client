package no.ssb.lds.data.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import io.reactivex.Flowable;
import no.ssb.lds.data.common.BinaryBackend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * A backend that caches the blocks.
 */
public class CachedBackend implements BinaryBackend {

    private final BinaryBackend delegate;
    private final Cache<BlockKey, ByteBuffer> cache;
    private final Integer blockSize;
    private final IntFunction<ByteBuffer> bufferAllocator;

    public CachedBackend(BinaryBackend delegate, Caffeine<Object, Object> cache, Integer blockSize,
                         IntFunction<ByteBuffer> bufferAllocator) {
        this.delegate = Objects.requireNonNull(delegate);
        this.blockSize = Objects.requireNonNull(blockSize);
        this.bufferAllocator = Objects.requireNonNull(bufferAllocator);
        this.cache = cache.weigher((BlockKey key, ByteBuffer buffer) -> buffer.limit()).build();
    }

    public CachedBackend(BinaryBackend delegate, CaffeineSpec cacheSpec, Integer blockSize,
                         IntFunction<ByteBuffer> bufferAllocator) {
        this(delegate, Caffeine.from(cacheSpec), blockSize, bufferAllocator);
    }

    @Override
    public Flowable<String> list(String path) throws IOException {
        return delegate.list(path);
    }

    @Override
    public SeekableByteChannel read(String path) throws IOException {
        SeekableByteChannel channel = delegate.read(path);
        return new CachedSeekableByteChannel(path, blockSize, cache, channel, bufferAllocator);
    }

    @Override
    public SeekableByteChannel write(String path) throws IOException {
        return delegate.write(path);
    }

    /**
     * Block key.
     */
    private static class BlockKey {
        private final CharSequence path;
        private final Long pos;

        private BlockKey(String path, Long pos) {
            this.path = path;
            this.pos = pos;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BlockKey)) return false;
            BlockKey blockKey = (BlockKey) o;
            return Objects.equals(path, blockKey.path) &&
                    Objects.equals(pos, blockKey.pos);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, pos);
        }
    }

    private static class CachedSeekableByteChannel implements SeekableByteChannel {

        private final int blockSize;
        private final Cache<BlockKey, ByteBuffer> cache;
        private final String key;
        private final SeekableByteChannel delegate;
        private final IntFunction<ByteBuffer> bufferAllocator;
        private long position;

        private CachedSeekableByteChannel(String key, Integer blockSize, Cache<BlockKey, ByteBuffer> cache,
                                          SeekableByteChannel delegate, IntFunction<ByteBuffer> bufferAllocator) {
            this.key = key;
            this.blockSize = blockSize;
            this.cache = cache;
            this.delegate = delegate;
            this.bufferAllocator = bufferAllocator;
        }

        private BlockKey getKey(long pos) {
            return new BlockKey(key, (pos / blockSize) * blockSize);
        }

        private ByteBuffer getOrFetch() throws IOException {
            BlockKey key = getKey(position());
            ByteBuffer buffer = cache.getIfPresent(key);
            if (buffer == null) {
                buffer = bufferAllocator.apply(this.blockSize);
                delegate.position(key.pos);
                while (buffer.hasRemaining()) {
                    int read = delegate.read(buffer);
                    if (read < 0) {
                        break;
                    }
                }
                buffer.limit(buffer.position());
                cache.put(key, buffer);
            }
            ByteBuffer copy = buffer.duplicate();
            copy.position(Math.toIntExact(position - key.pos));
            return copy.slice();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            while (dst.hasRemaining()) {
                ByteBuffer buffer = getOrFetch();
                buffer.limit(Math.min(dst.remaining(), buffer.remaining()));
                dst.put(buffer);
                position += buffer.position();
                return position > size() ? -1 : buffer.position();
            }
            return delegate.read(dst);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return delegate.write(src);
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public SeekableByteChannel position(long newPosition) {
            position = newPosition;
            return this;
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            delegate.truncate(size);
            return this;
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}

package no.ssb.lds.data.common.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

public class SeekableByteChannelCounter implements SeekableByteChannel {

    private final SeekableByteChannel delegate;
    private final AtomicLong counter;

    public SeekableByteChannelCounter(SeekableByteChannel delegate, AtomicLong counter) {
        this.delegate = delegate;
        this.counter = counter;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = delegate.read(dst);
        if (read > -1) {
            counter.addAndGet(read);
        }
        return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = delegate.write(src);
        counter.addAndGet(written);
        return written;
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return delegate.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return delegate.truncate(size);
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

package no.ssb.lds.data.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

public class InputStreamCounter extends InputStream {

    private final InputStream delegate;
    private final AtomicLong counter;

    public InputStreamCounter(InputStream delegate, AtomicLong counter) {
        this.delegate = delegate;
        this.counter = counter;
    }

    @Override
    public int read() throws IOException {
        this.counter.incrementAndGet();
        return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        int read = delegate.read(b);
        counter.addAndGet(read);
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = delegate.read(b, off, len);
        counter.addAndGet(read);
        return read;
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return delegate.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        return delegate.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return delegate.readNBytes(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        return delegate.transferTo(out);
    }
}

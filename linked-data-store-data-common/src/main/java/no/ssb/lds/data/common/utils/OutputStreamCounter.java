package no.ssb.lds.data.common.utils;


import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

public class OutputStreamCounter extends OutputStream {

    private final OutputStream delegate;
    private final AtomicLong counter;


    public OutputStreamCounter(OutputStream delegate, AtomicLong counter) {
        this.delegate = delegate;
        this.counter = counter;
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        counter.incrementAndGet();
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
        counter.addAndGet(b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
        counter.addAndGet(len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}

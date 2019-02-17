package no.ssb.lds.data.common;

import no.ssb.lds.data.common.converter.FormatConverter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class InputStreamCounter extends InputStream {

    private final InputStream delegate;
    private final FormatConverter.Status status;

    public InputStreamCounter(InputStream delegate, FormatConverter.Status status) {
        this.delegate = delegate;
        this.status = status;
    }

    @Override
    public int read() throws IOException {
        this.status.addRead(1);
        return delegate.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        int read = delegate.read(b);
        status.addRead(read);
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = delegate.read(b, off, len);
        status.addRead(read);
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

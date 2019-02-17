package no.ssb.lds.data.common;

import no.ssb.lds.data.common.converter.FormatConverter;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamCounter extends OutputStream {

    private final OutputStream delegate;
    private final FormatConverter.Status status;


    public OutputStreamCounter(OutputStream delegate, FormatConverter.Status status) {
        this.delegate = delegate;
        this.status = status;
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        status.addWritten(1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
        status.addWritten(b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
        status.addWritten(len);
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

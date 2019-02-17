package no.ssb.lds.data.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static no.ssb.lds.data.common.converter.FormatConverter.Status;

public class SeekableByteChannelCounter implements SeekableByteChannel {

    private final SeekableByteChannel delegate;
    private final Status status;

    public SeekableByteChannelCounter(SeekableByteChannel delegate, Status status) {
        this.delegate = delegate;
        this.status = status;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = delegate.read(dst);
        if (read > -1) {
            status.addRead(read);
        }
        return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written = delegate.write(src);
        status.addWritten(written);
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

package no.ssb.lds.data.common.converter;

import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Wraps SeekableInputStream.
 */
public class SeekableInputStreamWrapper extends SeekableInputStream {

    private final SeekableByteChannel delegate;

    public SeekableInputStreamWrapper(SeekableByteChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegate.read(ByteBuffer.wrap(b));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public long skip(long n) throws IOException {
        delegate.position(delegate.position() + n);
        return delegate.position();
    }

    @Override
    public int available() throws IOException {
        return super.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public long getPos() throws IOException {
        return delegate.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
        delegate.position(newPos);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(ByteBuffer.wrap(bytes));
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        readFully(ByteBuffer.wrap(bytes, start, len));
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        return delegate.read(buf);
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (delegate.read(buf) < 0) {
                throw new EOFException();
            }
        }
    }

    @Override
    public int read() throws IOException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1);
            readFully(buffer);
            buffer.flip();
            return buffer.get() & 0xEF;
        } catch (EOFException eof) {
            return -1;
        }
    }
}

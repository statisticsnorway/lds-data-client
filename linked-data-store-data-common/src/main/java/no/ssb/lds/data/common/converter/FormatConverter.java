package no.ssb.lds.data.common.converter;

import io.reactivex.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Convert the data.
 */
public interface FormatConverter {

    /**
     * Convert the input to the output.
     *
     * @return an observable with read/write status.
     */
    Observable<Status> write(InputStream input, SeekableByteChannel output) throws IOException;

    /**
     * Convert back the input to the output.
     *
     * @return an observable with read/write status.
     */
    Observable<Status> read(SeekableByteChannel input, OutputStream ouput);

    class Status {

        private AtomicLong read = new AtomicLong(0);
        private AtomicLong write = new AtomicLong(0);

        public void addRead(long bytes) {
            read.addAndGet(bytes);
        }

        public void addWritten(long bytes) {
            read.addAndGet(bytes);
        }

        public long readBytes() {
            return this.read.get();
        }

        public long writtenBytes() {
            return this.write.get();
        }
    }
}

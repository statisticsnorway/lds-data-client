package no.ssb.lds.data.common.converter;

import io.reactivex.Observable;
import no.ssb.lds.data.common.model.GSIMDataset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Convert the data.
 */
public interface FormatConverter {

    /**
     * Returns true if this converter can handle the media type.
     */
    boolean doesSupport(String mediaType);

    String getMediaType();

    /**
     * Convert the input to the output.
     *
     * @return an observable with read/write status.
     */
    Observable<Status> write(InputStream input, SeekableByteChannel output, String mimeType, GSIMDataset dataset) throws IOException;

    /**
     * Convert back the input to the output.
     *
     * @return an observable with read/write status.
     */
    Observable<Status> read(SeekableByteChannel input, OutputStream output, String mimeType, GSIMDataset dataset);

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

        @Override
        public String toString() {
            return new StringJoiner(", ", Status.class.getSimpleName() + "[", "]")
                    .add("read=" + read)
                    .add("write=" + write)
                    .toString();
        }
    }
}

package no.ssb.lds.data.common.converter;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
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
     * @return a {@link CompletableSource} with read/write status.
     */
    Status write(InputStream input, SeekableByteChannel output, String mimeType, GSIMDataset dataset) throws IOException;

    /**
     * Convert back the input to the output.
     *
     * @return a {@link CompletableSource} with read/write status.
     */
    Status read(SeekableByteChannel input, OutputStream output, String mimeType, GSIMDataset dataset,
                Cursor<Long> cursor);

    class Cursor<T> {
        private final T after;
        private final Integer next;

        public Cursor(Integer next, T after) {
            this.after = after;
            this.next = next;
        }

        public T getAfter() {
            return after;
        }

        public Integer getNext() {
            return next;
        }
    }

    class Status implements CompletableSource {

        private AtomicLong read = new AtomicLong(0);
        private AtomicLong write = new AtomicLong(0);
        private Completable completable;

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

        void setCompletable(Completable completable) {
            this.completable = completable;
        }

        @Override
        public void subscribe(CompletableObserver completableObserver) {
            completable.subscribe(completableObserver);
        }

    }
}

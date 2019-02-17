package no.ssb.lds.data.common.converter;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.lds.data.common.FormatConverter;
import no.ssb.lds.data.common.InputStreamCounter;
import no.ssb.lds.data.common.OutputStreamCounter;
import no.ssb.lds.data.common.ParquetProvider;
import no.ssb.lds.data.common.SeekableByteChannelCounter;
import no.ssb.lds.data.common.model.GSIMDataset;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;

/**
 * Converts from a mediatype to parquet and back;
 */
public abstract class AbstractFormatConverter implements FormatConverter {

    private final GSIMDataset dataset;
    private final ParquetProvider provider;
    public AbstractFormatConverter(
            GSIMDataset dataset,
            ParquetProvider provider
    ) {
        this.dataset = dataset;
        this.provider = provider;
    }

    public GSIMDataset getDataset() {
        return dataset;
    }

    /**
     * Convert an input stream to a flowable of {@link Group}
     */
    protected abstract Flowable<Group> encode(InputStream input) throws IOException;

    protected abstract Completable decode(Flowable<Group> records, OutputStream output);

    @Override
    public final Observable<Status> write(InputStream input, SeekableByteChannel output) throws IOException {
        Status status = new Status();
        InputStreamCounter countedInput = new InputStreamCounter(input, status);
        SeekableByteChannelCounter countedOutput = new SeekableByteChannelCounter(output, status);
        return Observable.defer(() -> {
            ParquetWriter<Group> writer = provider.getWriter(countedOutput);
            Flowable<Group> groups = encode(countedInput);
            return groups.map(group -> {
                writer.write(group);
                return status;
            }).toObservable();
        });
    }

    @Override
    public final Observable<Status> read(SeekableByteChannel input, OutputStream output) {

        // Used to count the byte in and out.
        Status status = new Status();
        SeekableByteChannelCounter countedInput = new SeekableByteChannelCounter(input, status);
        OutputStreamCounter countedOutput = new OutputStreamCounter(output, status);

        return Observable.defer(() -> {
            Flowable<Group> groups = Flowable.generate(() -> {
                return provider.getReader(countedInput);
            }, (reader, emitter) -> {
                try {
                    Group nextGroup = reader.read();
                    if (nextGroup == null) {
                        emitter.onComplete();
                    } else {
                        emitter.onNext(nextGroup);
                    }
                } catch (IOException ioe) {
                    emitter.onError(ioe);
                }
            }, ParquetReader::close);
            // TODO: Link with status.
            return decode(groups, countedOutput).toObservable();
        });
    }

    public abstract boolean doesSupport(String mediaType);

}

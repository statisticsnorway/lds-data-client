package no.ssb.lds.data.common.converter;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.lds.data.common.InputStreamCounter;
import no.ssb.lds.data.common.OutputStreamCounter;
import no.ssb.lds.data.common.SeekableByteChannelCounter;
import no.ssb.lds.data.common.model.GSIMComponent;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMType;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts from a mediatype to parquet and back;
 */
public abstract class AbstractFormatConverter implements FormatConverter {

    private final ParquetProvider provider;

    public AbstractFormatConverter(
            ParquetProvider provider
    ) {
        this.provider = provider;
    }

    protected static Schema.Type getType(GSIMType gsimType) {
        switch (gsimType) {
            case INTEGER:
            case DATETIME:
                return Schema.Type.LONG;
            case FLOAT:
                return Schema.Type.DOUBLE;
            case BOOLEAN:
                return Schema.Type.BOOLEAN;
            case STRING:
                return Schema.Type.STRING;
            default:
                throw new IllegalArgumentException("unsupported type" + gsimType);
        }
    }

    /**
     * Convert an input stream to a flowable of {@link Group}
     */
    protected abstract Flowable<GenericRecord> encode(InputStream input, Schema schema) throws IOException;

    protected abstract Completable decode(Flowable<GenericRecord> records, OutputStream output, Schema schema);

    /**
     * Convert a GSIMDataset to an avro {@link Schema}.
     */
    protected Schema getSchema(GSIMDataset dataset) {
        List<Schema.Field> fieldList = new ArrayList<>();
        for (GSIMComponent component : dataset.getComponents()) {
            Schema.Field field = new Schema.Field(component.getName(), Schema.create(getType(component.getType())), "doc", (Object) null);
            fieldList.add(field);
        }
        Schema record = Schema.createRecord("dataset", "", "no.ssb.gsim.dataset", false, fieldList);
        record.addProp("no.ssb.gsim.dataset.id", dataset.getId());
        return record;
    }

    @Override
    public final Observable<Status> write(InputStream input, SeekableByteChannel output, String mimeType, GSIMDataset dataset) throws IOException {

        // Wrap input and output to count bytes.
        Status status = new Status();
        InputStreamCounter countedInput = new InputStreamCounter(input, status);
        SeekableByteChannelCounter countedOutput = new SeekableByteChannelCounter(output, status);

        return Observable.defer(() -> {
            ParquetWriter<GenericRecord> writer = provider.getWriter(countedOutput, getSchema(dataset));
            Flowable<GenericRecord> groups = encode(countedInput, getSchema(dataset));
            return groups.map(group -> {
                writer.write(group);
                return status;
            }).doFinally(writer::close).toObservable();
        });
    }

    @Override
    public final Observable<Status> read(SeekableByteChannel input, OutputStream output, String mimeType, GSIMDataset dataset) {

        // Wrap input and output to count bytes.
        Status status = new Status();
        SeekableByteChannelCounter countedInput = new SeekableByteChannelCounter(input, status);
        OutputStreamCounter countedOutput = new OutputStreamCounter(output, status);
        return Observable.defer(() -> {
            Schema avroSchema = getSchema(dataset);
            Flowable<GenericRecord> groups = Flowable.generate(() -> {
                return provider.getReader(countedInput, avroSchema);
            }, (reader, emitter) -> {
                try {
                    GenericRecord nextGroup = reader.read();
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
            return decode(groups, countedOutput, avroSchema).toObservable();
        });
    }

}

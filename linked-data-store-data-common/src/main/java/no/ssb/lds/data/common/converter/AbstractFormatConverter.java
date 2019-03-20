package no.ssb.lds.data.common.converter;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.common.model.GSIMComponent;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMType;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import no.ssb.lds.data.common.utils.InputStreamCounter;
import no.ssb.lds.data.common.utils.OutputStreamCounter;
import no.ssb.lds.data.common.utils.SeekableByteChannelCounter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter.PagedRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.filter2.compat.FilterCompat.*;

/**
 * Converts from a mediatype to parquet and back;
 */
@Deprecated
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

    protected abstract Flowable<GenericRecord> encode(InputStream input, Schema schema) throws IOException;

    protected abstract Completable decode(Flowable<GenericRecord> records, OutputStream output, Schema schema);

    /**
     * Convert a GSIMDataset to an avro {@link Schema}.
     */
    protected Schema getSchema(GSIMDataset dataset) {
        List<Schema.Field> fieldList = new ArrayList<>();
        for (GSIMComponent component : dataset.getComponents()) {
            Schema.Field field = new Schema.Field(
                    component.getName(),
                    Schema.create(getType(component.getType())),
                    "...",
                    (Object) null
            );
            fieldList.add(field);
        }

        // TODO: Add link back.
        Schema record = Schema.createRecord(
                "dataset", "...",
                "no.ssb.gsim.dataset",
                false,
                fieldList);
        record.addProp("no.ssb.gsim.dataset.id", dataset.getId());
        return record;
    }

    @Override
    public final Status write(InputStream input, SeekableByteChannel output, String mimeType, GSIMDataset dataset) throws IOException {

        // Wrap input and output to count bytes.
        Status status = new Status();
        InputStreamCounter countedInput = new InputStreamCounter(input, status);
        SeekableByteChannelCounter countedOutput = new SeekableByteChannelCounter(output, status);

        Completable completable = Completable.using(
                () -> provider.getWriter(countedOutput, getSchema(dataset)),
                writer -> {
                    Flowable<GenericRecord> groups = encode(countedInput, getSchema(dataset));
                    return groups.doOnNext(record -> {
                        writer.write(record);
                    }).ignoreElements();
                },
                writer -> writer.close()
        );

        status.setCompletable(completable);
        return status;
    }

    @Override
    public final Status read(SeekableByteChannel input, OutputStream output, String mimeType, GSIMDataset dataset,
                             Cursor<Long> cursor
    ) {

        // Wrap input and output to count bytes.
        Status status = new Status();
        SeekableByteChannelCounter countedInput = new SeekableByteChannelCounter(input, status);
        OutputStreamCounter countedOutput = new OutputStreamCounter(output, status);
        Schema schema = getSchema(dataset);

        // Convert to pos + size
        long start = Math.max(cursor.getAfter() + 1, 0);
        int size = cursor.getNext() + 1;

        Filter filter = FilterCompat.get(PagedRecordFilter.page(start, size));

        // Back pressured.
        Flowable<GenericRecord> groups = Flowable.generate(() -> {
            return provider.getReader(countedInput, schema, filter);
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
        }, reader -> {
            reader.close();
        });
        Completable completable = decode(groups.take(cursor.getNext()), countedOutput, schema);
        status.setCompletable(completable);
        return status;
    }

}

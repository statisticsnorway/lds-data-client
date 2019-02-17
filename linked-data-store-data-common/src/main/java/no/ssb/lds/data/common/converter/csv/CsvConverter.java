package no.ssb.lds.data.common.converter.csv;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.common.ParquetProvider;
import no.ssb.lds.data.common.converter.AbstractFormatConverter;
import no.ssb.lds.data.common.model.GSIMComponent;
import no.ssb.lds.data.common.model.GSIMDataset;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;

import static io.reactivex.Flowable.fromIterable;

public class CsvConverter extends AbstractFormatConverter {

    public static final String MEDIA_TYPE = "text/csv";

    // TODO: Generate. See https://github.com/apache/parquet-mr/blob/0d541fc6cfdc0de9f45d2d2d7606afdef1415750/parquet-column/src/test/java/org/apache/parquet/parser/TestParquetParser.java
    public static MessageType schema = MessageTypeParser.parseMessageType(
            " message people { " +
                    "required binary PERSON_ID (UTF8);" +
                    "optional int64 INCOME (UINT_64);" +
                    "optional binary GENDER (UTF8);" +
                    "optional binary MARITAL_STATUS (UTF8);" +
                    "optional binary MUNICIPALITY (UTF8);" +
                    "optional binary DATA_QUALITY (UTF8);" +
                    " }"
    );

    public static SimpleGroupFactory sfg = new SimpleGroupFactory(schema);

    public CsvConverter(GSIMDataset dataset, ParquetProvider provider) {
        super(dataset, provider);
    }

    private Group encodeRecord(CSVRecord record) {
        Group group = sfg.newGroup();
        for (GSIMComponent component : getDataset().getComponents()) {
            String name = component.getName();
            String value = record.get(name);
            switch (component.getType()) {
                case STRING:
                    group.add(name, value);
                    break;
                case INTEGER:
                    group.add(name, Long.parseLong(value));
                    break;
                case BOOLEAN:
                    group.add(name, Boolean.parseBoolean(value));
                    break;
                case FLOAT:
                    group.add(name, Double.parseDouble(value));
                    break;
                case DATETIME:
                    // TODO: Handle date time.
                case ARRAY:
                    throw new IllegalArgumentException("Unsupported type");
            }
        }
        return group;
    }

    @Override
    public Flowable<Group> encode(InputStream input) {
        // TODO: Detect charset
        // TODO: Support Hierarchical dataset.
        // TODO: Fail if no headers.
        return Flowable.defer(() -> {
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new InputStreamReader(input));
            return fromIterable(records).doFinally(records::close).map(this::encodeRecord);
        });
    }

    @Override
    public Completable decode(Flowable<Group> records, OutputStream output) {
        // TODO: Detect charset
        // TODO: Support Hierarchical dataset.
        // TODO: Fail if no headers.
        return Completable.error(new IllegalArgumentException("not implemented"));
    }

    public boolean doesSupport(String mediaType) {
        return MEDIA_TYPE.equals(mediaType);
    }
}

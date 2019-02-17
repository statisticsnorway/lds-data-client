package no.ssb.lds.data.common.converter.csv;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.common.converter.AbstractFormatConverter;
import no.ssb.lds.data.common.model.GSIMComponent;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

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

    public static Schema createAvroSchema() {
        return Schema.createRecord("name", "description", "no.ssb", false,
                List.of(
                        new Schema.Field("PERSON_ID", Schema.create(Schema.Type.STRING), "doc", (Object) null),
                        new Schema.Field("INCOME", Schema.create(Schema.Type.LONG), "doc", (Object) null),
                        new Schema.Field("GENDER", Schema.create(Schema.Type.STRING), "doc", (Object) null),
                        new Schema.Field("MARITAL_STATUS", Schema.create(Schema.Type.STRING), "doc", (Object) null),
                        new Schema.Field("MUNICIPALITY", Schema.create(Schema.Type.STRING), "doc", (Object) null),
                        new Schema.Field("DATA_QUALITY", Schema.create(Schema.Type.STRING), "doc", (Object) null)
                )
        );
    }

    private GenericRecord encodeRecord(CSVRecord record) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(createAvroSchema());
        for (GSIMComponent component : getDataset().getComponents()) {
            String name = component.getName();
            String value = record.get(name);
            switch (component.getType()) {
                case STRING:
                    recordBuilder.set(name, value);
                    break;
                case INTEGER:
                    recordBuilder.set(name, Long.parseLong(value));
                    break;
                case BOOLEAN:
                    recordBuilder.set(name, Boolean.parseBoolean(value));
                    break;
                case FLOAT:
                    recordBuilder.set(name, Double.parseDouble(value));
                    break;
                case DATETIME:
                    // TODO: Handle date time.
                case ARRAY:
                    throw new IllegalArgumentException("Unsupported type");
            }
        }
        return recordBuilder.build();
    }

    @Override
    public Flowable<GenericRecord> encode(InputStream input) {
        // TODO: Detect charset
        // TODO: Support Hierarchical dataset.
        // TODO: Fail if no headers.
        return Flowable.defer(() -> {
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new InputStreamReader(input));
            return fromIterable(records).doFinally(records::close).map(this::encodeRecord);
        });
    }

    @Override
    public Completable decode(Flowable<GenericRecord> records, OutputStream output) {
        // TODO: Detect charset
        // TODO: Support Hierarchical dataset.
        // TODO: Fail if no headers.
        List<String> names = new ArrayList<>();
        for (Schema.Field field : getSchema().getFields()) {
            names.add(field.name());
        }
        OutputStreamWriter out = new OutputStreamWriter(output);
        return Completable.defer(() -> {
            try {
                CSVPrinter csvPrinter = CSVFormat.RFC4180.withHeader(names.toArray(new String[]{}))
                        .print(out);
                return records.doOnNext(genericRecord -> {
                    List<Object> values = new ArrayList<>();
                    for (String name : names) {
                        values.add(genericRecord.get(name));
                    }
                    csvPrinter.printRecord(values);
                }).doFinally(() -> out.flush()).ignoreElements();
            } catch (IOException e) {
                return Completable.error(e);
            }
        });
    }

    @Override
    protected Schema getSchema() {
        return createAvroSchema();
    }

    public boolean doesSupport(String mediaType) {
        return MEDIA_TYPE.equals(mediaType);
    }
}

package no.ssb.lds.data.client.converters;

import io.reactivex.Flowable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvConverterTest {

    private CsvConverter converter;
    private Schema dimensionalSchema;

    @BeforeEach
    void setUp() {
        converter = new CsvConverter();
        dimensionalSchema = Schema.createRecord(List.of(
                new Schema.Field("string", Schema.create(Schema.Type.STRING), "A string", (Object) null),
                new Schema.Field("int", Schema.create(Schema.Type.INT), "An int", (Object) null),
                new Schema.Field("boolean", Schema.create(Schema.Type.BOOLEAN), "A boolean", (Object) null),
                new Schema.Field("float", Schema.create(Schema.Type.FLOAT), "A float", (Object) null),
                new Schema.Field("long", Schema.create(Schema.Type.LONG), "A long", (Object) null),
                new Schema.Field("double", Schema.create(Schema.Type.DOUBLE), "A double", (Object) null)
        ));
    }

    @Test
    void testEmpty() {

        List<GenericRecord> records = converter.read(
                InputStream.nullInputStream(),
                converter.getMediaType(),
                dimensionalSchema
        ).toList().blockingGet();

        assertThat(records).isEmpty();

        converter.write(
                Flowable.empty(),
                OutputStream.nullOutputStream(),
                converter.getMediaType(),
                dimensionalSchema
        ).blockingAwait();

    }
}
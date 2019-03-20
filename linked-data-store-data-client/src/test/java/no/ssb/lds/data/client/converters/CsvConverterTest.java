package no.ssb.lds.data.client.converters;

import io.reactivex.Flowable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

    @Test
    void testRead() {

        InputStream csvStream = new ByteArrayInputStream((
                "string,int,boolean,float,long,double\r\n" +
                        "foo,123,true,123.123,123,123.123"
        ).getBytes());

        List<GenericRecord> records = converter.read(csvStream, converter.getMediaType(), dimensionalSchema).toList()
                .blockingGet();

        assertThat(records).containsExactly(
                new GenericRecordBuilder(dimensionalSchema)
                        .set("string", "foo")
                        .set("int", 123)
                        .set("boolean", true)
                        .set("float", 123.123F)
                        .set("long", 123L)
                        .set("double", 123.123D)
                        .build()
        );
    }

    @Test
    void testWrite() {

        InputStream csvStream = new ByteArrayInputStream((
                "string,int,boolean,float,long,double\r\n" +
                        "foo,123,true,123.123,123,123.123"
        ).getBytes());


        GenericData.Record record = new GenericRecordBuilder(dimensionalSchema)
                .set("string", "foo")
                .set("int", 123)
                .set("boolean", true)
                .set("float", 123.123F)
                .set("long", 123L)
                .set("double", 123.123D)
                .build();

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        converter.write(
                Flowable.just(record),
                output,
                converter.getMediaType(), dimensionalSchema
        ).blockingAwait();

        assertThat(new String(output.toByteArray())).isEqualTo(
                "string,int,boolean,float,long,double\r\n" +
                "foo,123,true,123.123,123,123.123\r\n"
        );


    }
}
package no.ssb.lds.data.client;

import io.reactivex.Flowable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DataClientTest {

    public static final Schema DIMENSIONAL_SCHEMA = Schema.createRecord("root", "...", "no.ssb.dataset", false, List.of(
            new Schema.Field("string", Schema.create(Schema.Type.STRING), "A string", (Object) null),
            new Schema.Field("int", Schema.create(Schema.Type.INT), "An int", (Object) null),
            new Schema.Field("boolean", Schema.create(Schema.Type.BOOLEAN), "A boolean", (Object) null),
            new Schema.Field("float", Schema.create(Schema.Type.FLOAT), "A float", (Object) null),
            new Schema.Field("long", Schema.create(Schema.Type.LONG), "A long", (Object) null),
            new Schema.Field("double", Schema.create(Schema.Type.DOUBLE), "A double", (Object) null)
    ));

    private DataClient client;
    private String prefix;

    @BeforeEach
    void setUp() throws IOException {

        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(8 * 1024 * 1024);
        parquetConfiguration.setRowGroupSize(64 * 1024 * 1024);

        DataClient.Configuration clientConfiguration = new DataClient.Configuration();

        prefix = Files.createTempDirectory("lds-data-client").toString();
        clientConfiguration.setLocation(prefix);

        client = DataClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(prefix))
                .withConfiguration(clientConfiguration)
                .build();
    }

    @Test
    void testReadWRite() {

        GenericData.Record record = new GenericRecordBuilder(DIMENSIONAL_SCHEMA)
                .set("string", "foo")
                .set("int", 123)
                .set("boolean", true)
                .set("float", 123.123F)
                .set("long", 123L)
                .set("double", 123.123D)
                .build();

        Flowable<GenericRecord> records = Flowable.range(1, 1000).map(integer -> record);

        client.writeData("test", DIMENSIONAL_SCHEMA, records, "").blockingAwait();

        List<GenericRecord> readRecords = client.readData("test", DIMENSIONAL_SCHEMA, "", null)
                .toList().blockingGet();

        assertThat(readRecords).containsExactlyInAnyOrderElementsOf(records.toList().blockingGet());

    }
}
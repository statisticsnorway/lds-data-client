package no.ssb.lds.data.client;

import io.reactivex.Flowable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private GenericRecordBuilder recordBuilder;

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
        recordBuilder = new GenericRecordBuilder(DIMENSIONAL_SCHEMA);
    }

    @Test
    void testReadWRite() {

        GenericRecordBuilder record = recordBuilder
                .set("string", "foo")
                .set("boolean", true)
                .set("float", 123.123F)
                .set("long", 123L)
                .set("double", 123.123D);

        Flowable<GenericRecord> records = Flowable.range(1, 1000).map(integer -> record.set("int", integer)
                .build());

        client.writeData("test", DIMENSIONAL_SCHEMA, records, "").blockingAwait();

        List<GenericRecord> readRecords = client.readData("test", DIMENSIONAL_SCHEMA, "", null)
                .toList().blockingGet();

        assertThat(readRecords)
                .usingElementComparator(Comparator.comparingInt(r -> (Integer) r.get("int")))
                .containsExactlyInAnyOrderElementsOf(records.toList().blockingGet());

    }

    @Test
    void testWindowedWrite() {

        // Demonstrate the use of windowing (time and size) with the data client.

        Random random = new Random();
        Flowable<Long> unlimitedFlowable = Flowable.generate(() -> {
            AtomicLong size = new AtomicLong(random.nextInt(500) + 500);
            System.out.println("Starting generator with size " + size.get());
            return size;
        }, (atomicLong, emitter) -> {
            Thread.sleep(random.nextInt(10));
            long value = atomicLong.decrementAndGet();
            if (value <= 0L) {
                emitter.onComplete();
            } else {
                emitter.onNext(value);
            }
        }, atomicLong -> {
            System.out.println("Done generating");
        });

        unlimitedFlowable
                // Transform to records.
                .map(income -> {
                    return (GenericRecord) recordBuilder
                            .set("string", income.toString())
                            .set("int", income)
                            .set("boolean", income % 2 == 0)
                            .set("float", income / 2)
                            .set("long", income)
                            .set("double", income / 2)
                            .build();
                })
                // Buffer by size and time. Max 1000 or 5 seconds.
                .window(
                        2500,
                        TimeUnit.MILLISECONDS,
                        100,
                        true
                )
                // Write a file for each group.
                .flatMapCompletable(groupOfOneThousand -> {
                    System.out.println("Writing a group...");
                    return client.writeData("test" + System.currentTimeMillis(), DIMENSIONAL_SCHEMA, groupOfOneThousand, "");
                }, false, 10)
                // Wait.
                .blockingAwait();
    }
}
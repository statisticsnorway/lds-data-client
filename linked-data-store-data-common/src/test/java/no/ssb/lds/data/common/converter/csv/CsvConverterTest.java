package no.ssb.lds.data.common.converter.csv;

import no.ssb.lds.data.common.SeekableInMemoryByteChannel;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMDatasetTest;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

class CsvConverterTest {

    private GSIMDataset dataset;

    @BeforeEach
    void setUp() {
        dataset = new GSIMDatasetTest().createDataset();
    }

    @Test
    void testRead() throws IOException {
        CsvConverter converter = new CsvConverter(new ParquetProvider());
        InputStream csvInput = new ByteArrayInputStream((
                "PERSON_ID,INCOME,GENDER,MARITAL_STATUS,MUNICIPALITY,DATA_QUALITY\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD"
        ).getBytes());
        SeekableInMemoryByteChannel parquetOutput = new SeekableInMemoryByteChannel();

        converter.write(csvInput, parquetOutput, "", dataset)
                .doOnNext(status -> System.out.println(status))
                .ignoreElements()
                .blockingAwait();

        SeekableInMemoryByteChannel parquetInput = new SeekableInMemoryByteChannel(parquetOutput.array());
        parquetInput.truncate(parquetOutput.position());

        File file = File.createTempFile("test", "test");
        try (FileChannel channel = new FileOutputStream(file).getChannel()) {
            channel.transferFrom(parquetInput, 0, parquetInput.size());
        }
        parquetInput.position(0);


        ByteArrayOutputStream csvOutput = new ByteArrayOutputStream();


        converter.read(parquetInput, csvOutput, "", dataset)
                .doOnNext(status -> System.out.println(status))
                .ignoreElements()
                .blockingAwait();

        System.out.println(csvOutput);
    }
}
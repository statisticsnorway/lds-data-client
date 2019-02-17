package no.ssb.lds.data.common.converter.csv;

import no.ssb.lds.data.common.SeekableInMemoryByteChannel;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMDatasetTest;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;

class CsvConverterTest {

    private GSIMDataset dataset;

    @BeforeEach
    void setUp() {
        dataset = new GSIMDatasetTest().createDataset();
    }

    @Test
    void testRead() throws IOException {
        CsvConverter converter = new CsvConverter(dataset, new ParquetProvider());
        InputStream csvInput = new ByteArrayInputStream((
                "PERSON_ID,INCOME,GENDER,MARITAL_STATUS,MUNICIPALITY,DATA_QUALITY\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD"
        ).getBytes());
        SeekableInMemoryByteChannel parquetOutput = new SeekableInMemoryByteChannel();
        converter.write(csvInput, parquetOutput).ignoreElements().blockingAwait();
        SeekableInMemoryByteChannel parquetInput = new SeekableInMemoryByteChannel(parquetOutput.array());
        parquetInput.truncate(parquetOutput.position());
        ByteArrayOutputStream csvOutput = new ByteArrayOutputStream();
        converter.read(parquetInput, csvOutput).ignoreElements().blockingAwait();

        System.out.println(csvOutput);
    }
}
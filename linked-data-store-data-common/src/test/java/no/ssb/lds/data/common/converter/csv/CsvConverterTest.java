package no.ssb.lds.data.common.converter.csv;

import io.reactivex.Completable;
import no.ssb.lds.data.common.Configuration;
import no.ssb.lds.data.common.converter.FormatConverter.Cursor;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMDatasetTest;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import no.ssb.lds.data.common.utils.SeekableInMemoryByteChannel;
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
        Configuration configuration = new Configuration();
        configuration.setParquet(new Configuration.Parquet());
        configuration.getParquet().setPageSize(8 * 1024 * 1024);
        configuration.getParquet().setPageSize(8 * 1024 * 1024);
        configuration.getParquet().setRowGroupSize(8 * 8 * 1024 * 1024);

        CsvConverter converter = new CsvConverter(new ParquetProvider(configuration));
        InputStream csvInput = new ByteArrayInputStream((
                "PERSON_ID,INCOME,GENDER,MARITAL_STATUS,MUNICIPALITY,DATA_QUALITY\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD"
        ).getBytes());
        SeekableInMemoryByteChannel parquetOutput = new SeekableInMemoryByteChannel();

        Completable.wrap(converter.write(csvInput, parquetOutput, "", dataset))
                .blockingAwait();

        SeekableInMemoryByteChannel parquetInput = new SeekableInMemoryByteChannel(parquetOutput.array());
        parquetInput.truncate(parquetOutput.position());

        File file = File.createTempFile("test", "test");
        try (FileChannel channel = new FileOutputStream(file).getChannel()) {
            channel.transferFrom(parquetInput, 0, parquetInput.size());
        }
        parquetInput.position(0);


        ByteArrayOutputStream csvOutput = new ByteArrayOutputStream();


        Completable.wrap(converter.read(parquetInput, csvOutput, "", dataset, new Cursor<>(0, 0L)))
                .blockingAwait();

        System.out.println(csvOutput);
    }
}
package no.ssb.lds.data.common.converter.csv;

import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.model.GSIMDatasetTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

class CsvConverterTest {

    private GSIMDataset dataset;

    @BeforeEach
    void setUp() {
        dataset = new GSIMDatasetTest().createDataset();
    }

    @Test
    void testRead() throws IOException {
        CsvConverter converter = new CsvConverter(dataset, null);
        InputStream input = new ByteArrayInputStream((
                "PERSON_ID,INCOME,GENDER,MARITAL_STATUS,MUNICIPALITY,DATA_QUALITY\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD\r\n" +
                        "\"person number\",1234,MAN,SINGLE,\"0556\",GOOD"
        ).getBytes());
        OutputStream output = new ByteArrayOutputStream();
        converter.write(input, Channels.newChannel(output));
    }
}
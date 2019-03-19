package no.ssb.lds.data.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.converter.FormatConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

/**
 * Data client is an abstraction to read and write Parquet files on bucket storage.
 * <p>
 * The data client supports CSV and JSON type conversions and can be extended by implementing the
 * {@link FormatConverter} interface (see {@link no.ssb.lds.data.common.converter.csv.CsvConverter} and
 * {@link no.ssb.lds.data.common.converter.json.JsonConverter} for examples).
 * <p>
 * One can also read and write {@link GenericRecord}s.
 */
public abstract class DataClient {

    protected final Configuration configuration;

    public DataClient(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration);
    }

    /**
     * Convert and write binary data.
     *
     * @param dataId    an opaque identifier for the data.
     * @param schema    the schema used to parse the data.
     * @param input     the binary data.
     * @param mediaType the media type of the binary data.
     * @param token     an authentication token.
     * @return a completable that completes once the data is saved.
     */
    public abstract Completable convertAndWrite(String dataId, Schema schema, InputStream input, String mediaType,
                                                String token);

    /**
     * Read data and convert to binary data.
     *
     * @param dataId    an opaque identifier for the data.
     * @param schema    the schema used to parse the data.
     * @param mediaType the media type of the binary data.
     * @param token     an authentication token.
     * @return a completable that completes once the data is saved.
     */
    public abstract OutputStream readAndConvert(String dataId, Schema schema, String mediaType, String token);

    /**
     * Write a sequence of {@link GenericRecord}s to the bucket storage.
     *
     * @param dataId  an opaque identifier for the data.
     * @param schema  the schema used to create the records.
     * @param records the records to write.
     * @param token   an authentication token.
     * @return a completable that completes once the data is saved.
     */
    public abstract Completable writeData(String dataId, Schema schema, Flowable<GenericRecord> records, String token);

    /**
     * Read a sequence of {@link GenericRecord}s from the bucket storage.
     *
     * @param dataId the identifier for the data.
     * @param schema the schema used to create the records.
     * @param token  an authentication token.
     * @return a {@link Flowable} of records.
     */
    public abstract Flowable<GenericRecord> readData(String dataId, Schema schema, String token);

    public static class Configuration {

        private final BinaryBackend backend;
        private List<FormatConverter> converters;

        public Configuration(BinaryBackend backend) {
            this.backend = backend;
        }

        public BinaryBackend getBackend() {
            return backend;
        }

        public List<FormatConverter> getConverters() {
            return converters;
        }

        public void setConverters(List<FormatConverter> converters) {
            this.converters = converters;
        }

    }
}

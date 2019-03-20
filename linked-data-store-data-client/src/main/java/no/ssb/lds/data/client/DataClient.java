package no.ssb.lds.data.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
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
public class DataClient {

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
    public Completable convertAndWrite(String dataId, Schema schema, InputStream input, String mediaType,
                                       String token) {
        for (FormatConverter converter : configuration.getConverters()) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = converter.read(input, mediaType, schema);
                return writeData(dataId, schema, records, token);
            }
        }
        return Completable.error(new IllegalArgumentException("unsupported type " + mediaType));
    }

    /**
     * Read data and convert to binary data.
     *
     * @param dataId       an opaque identifier for the data.
     * @param schema       the schema used to parse the data.
     * @param outputStream the output stream to read the data into.
     * @param mediaType    the media type of the binary data.
     * @param token        an authentication token.
     * @return a completable that completes once the data is read.
     */
    public Completable readAndConvert(String dataId, Schema schema, OutputStream outputStream,
                                      String mediaType, String token) {
        for (FormatConverter converter : configuration.getConverters()) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = readData(dataId, schema, token);
                return converter.write(records, outputStream, mediaType, schema);
            }
        }
        throw new IllegalArgumentException("unsupported type " + mediaType);
    }

    /**
     * Write a sequence of {@link GenericRecord}s to the bucket storage.
     *
     * @param dataId  an opaque identifier for the data.
     * @param schema  the schema used to create the records.
     * @param records the records to write.
     * @param token   an authentication token.
     * @return a completable that completes once the data is saved.
     */
    public Completable writeData(String dataId, Schema schema, Flowable<GenericRecord> records, String token) {
        BinaryBackend backend = configuration.getBackend();
        ParquetProvider provider = configuration.getParquetProvider();
        SeekableByteChannel writableChannel = backend.write(dataId);
        ParquetWriter<GenericRecord> parquetWriter = provider.getWriter(writableChannel, schema);
        return records.doOnNext(record -> parquetWriter.write(record)).ignoreElements();
    }

    /**
     * Read a sequence of {@link GenericRecord}s from the bucket storage.
     *
     * @param dataId the identifier for the data.
     * @param schema the schema used to create the records.
     * @param token  an authentication token.
     * @return a {@link Flowable} of records.
     */
    public Flowable<GenericRecord> readData(String dataId, Schema schema, String token) {
        BinaryBackend backend = configuration.getBackend();
        ParquetProvider provider = configuration.getParquetProvider();
        SeekableByteChannel readableChannel = backend.read(dataId);
        ParquetReader<GenericRecord> parquetReader = provider.getReader(readableChannel, schema);
        return Flowable.generate(emitter -> {
            GenericRecord read = parquetReader.read();
            if (read == null) {
                emitter.onComplete();
            } else {
                emitter.onNext(read);
            }
        });
    }

    public static class Configuration {

        private final BinaryBackend backend;
        private List<FormatConverter> converters;
        private ParquetProvider parquetProvider;

        public Configuration(BinaryBackend backend) {
            this.backend = backend;
        }

        public ParquetProvider getParquetProvider() {
            return parquetProvider;
        }

        public void setParquetProvider(ParquetProvider parquetProvider) {
            this.parquetProvider = parquetProvider;
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

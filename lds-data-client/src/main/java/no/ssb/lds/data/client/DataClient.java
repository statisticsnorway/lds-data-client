package no.ssb.lds.data.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.client.converters.FormatConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter.PagedRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Data client is an abstraction to read and write Parquet files on bucket storage.
 * <p>
 * The data client supports CSV and JSON type conversions and can be extended by implementing the
 * {@link FormatConverter} interface (see {@link no.ssb.lds.data.client.converters.CsvConverter} and
 * {@link no.ssb.lds.data.client.converters.JsonConverter} for examples).
 * <p>
 * One can also read and write {@link GenericRecord}s.
 */
public class DataClient {

    private final BinaryBackend backend;
    private final List<FormatConverter> converters;
    private final ParquetProvider provider;
    private final Configuration configuration;

    private DataClient(Builder builder) {
        this.backend = Objects.requireNonNull(builder.binaryBackend);
        this.converters = Objects.requireNonNull(builder.converters);
        this.provider = Objects.requireNonNull(builder.parquetProvider);
        this.configuration = Objects.requireNonNull(builder.configuration);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns true if the data client has a converter that support the mediaType.
     */
    public boolean canConvert(String mediaType) {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                return true;
            }
        }
        return false;
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
                                       String token) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = converter.read(input, mediaType, schema);
                return writeData(dataId, schema, records, token);
            }
        }
        throw new UnsupportedMediaTypeException("unsupported type " + mediaType);
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
                                      String mediaType, String token) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = readData(configuration.getLocation() + dataId, schema, token, null);
                return converter.write(records, outputStream, mediaType, schema);
            }
        }
        throw new UnsupportedMediaTypeException("unsupported type " + mediaType);
    }

    public Completable readAndConvert(String dataId, Schema schema, OutputStream outputStream,
                                      String mediaType, String token, Cursor<Long> cursor) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = readData(configuration.getLocation() + dataId, schema, token, cursor);
                return converter.write(records, outputStream, mediaType, schema);
            }
        }
        throw new UnsupportedMediaTypeException("unsupported type " + mediaType);
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
        return Completable.using(() -> {
            SeekableByteChannel writableChannel = backend.write(configuration.getLocation() + dataId);
            return provider.getWriter(writableChannel, schema);
        }, parquetWriter -> {
            return records.doOnNext(record -> parquetWriter.write(record)).ignoreElements();
        }, parquetWriter -> {
            parquetWriter.close();
        });

    }

    /**
     * Read a sequence of {@link GenericRecord}s from the bucket storage.
     *
     * @param dataId the identifier for the data.
     * @param schema the schema used to create the records.
     * @param token  an authentication token.
     * @param cursor
     * @return a {@link Flowable} of records.
     */
    public Flowable<GenericRecord> readData(String dataId, Schema schema, String token, Cursor<Long> cursor) {
        // TODO: Do something with token.
        // TODO: Handle projection.
        // TODO: Handle filtering.

        FilterCompat.Filter filter;
        if (cursor != null) {
            // Convert to pos + size
            long start = Math.max(cursor.getAfter() + 1, 0);
            int size = cursor.getNext() + 1;

            filter = FilterCompat.get(PagedRecordFilter.page(start, size));
        } else {
            filter = null;
        }

        return Flowable.generate(() -> {
            SeekableByteChannel readableChannel = backend.read(configuration.getLocation() + dataId);
            return provider.getReader(readableChannel, schema, filter);
        }, (parquetReader, emitter) -> {
            GenericRecord read = parquetReader.read();
            if (read == null) {
                emitter.onComplete();
            } else {
                emitter.onNext(read);
            }
        }, parquetReader -> {
            parquetReader.close();
        });
    }

    public static class Configuration {

        private String location;

        public Configuration() {
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }

    public static class Builder {

        private ParquetProvider parquetProvider;
        private BinaryBackend binaryBackend;
        private List<FormatConverter> converters = new ArrayList<>();
        private Configuration configuration;


        public Builder withParquetProvider(ParquetProvider parquetProvider) {
            this.parquetProvider = parquetProvider;
            return this;
        }

        public Builder withBinaryBackend(BinaryBackend binaryBackend) {
            this.binaryBackend = binaryBackend;
            return this;
        }

        public Builder withFormatConverter(FormatConverter formatConverter) {
            this.converters.add(formatConverter);
            return this;
        }

        public Builder withFormatConverters(List<FormatConverter> formatConverters) {
            this.converters.addAll(formatConverters);
            return this;
        }

        public Builder withConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public DataClient build() {
            return new DataClient(this);
        }

    }
}

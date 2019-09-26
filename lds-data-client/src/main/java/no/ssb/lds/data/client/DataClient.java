package no.ssb.lds.data.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.lds.data.client.converters.FormatConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Data client is an abstraction to read and write Parquet files on bucket storage.
 * <p>
 * The data client supports CSV and JSON type conversions and can be extended by implementing the
 * {@link FormatConverter} interface (see {@link no.ssb.lds.data.client.converters.CsvConverter} and
 * {@link no.ssb.lds.data.client.converters.JsonConverter} for examples).
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
     * Checks if the client can convert from and to a media type
     *
     * @param mediaType the media type to check against
     * @return true if the data client has a converter that support the media type.
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
     * @throws UnsupportedMediaTypeException if the client does not support the media type.
     */
    public Completable convertAndWrite(String dataId, Schema schema, InputStream input, String mediaType,
                                       String token) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = converter.read(input, mediaType, schema);
                return writeAllData(dataId, schema, records, token);
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
     * @throws UnsupportedMediaTypeException if the client does not support the media type.
     */
    public Completable readAndConvert(String dataId, Schema schema, OutputStream outputStream,
                                      String mediaType, String token) throws UnsupportedMediaTypeException {
        return readAndConvert(dataId, schema, outputStream, mediaType, token, null);
    }

    public Completable readAndConvert(String dataId, Schema schema, OutputStream outputStream,
                                      String mediaType, String token, Cursor<Long> cursor) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = readData(dataId, schema, token, cursor);
                return converter.write(records, outputStream, mediaType, schema);
            }
        }
        throw new UnsupportedMediaTypeException("unsupported type " + mediaType);
    }

    /**
     * Write an unbounded sequence of {@link GenericRecord}s to the bucket storage.
     * <p>
     * The records will be written in "batches" of size count or when the timespan duration elapsed. The last value of
     * each batch is returned in an {@link Observable}.
     *
     * @param idSupplier  a supplier for the id called each time a file is flushed.
     * @param records     the records to write.
     * @param timeWindow  the period of time before a batch should be written.
     * @param unit        the unit of time that applies to the timespan argument.
     * @param countWindow the maximum size of a batch before it should be written.
     * @return an {@link Observable} emitting the last record in each batch.
     */
    public <R extends GenericRecord> Observable<R> writeDataUnbounded(
            Supplier<String> idSupplier, Schema schema, Flowable<R> records, long timeWindow, TimeUnit unit, long countWindow,
            String token) {
        return records.window(timeWindow, unit, countWindow, true).switchMapMaybe(recordsWindow -> {
            return writeData(idSupplier.get(), schema, recordsWindow, token).lastElement();
        }).toObservable();
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
    public Completable writeAllData(String dataId, Schema schema, Flowable<GenericRecord> records, String token) {
        return writeData(dataId, schema, records, token).ignoreElements();
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
    public <R extends GenericRecord> Flowable<R> writeData(String dataId, Schema schema, Flowable<R> records, String token) {
        return Flowable.defer(() -> {
            DataWriter writer = writeData(dataId, schema, token);
            return records.doAfterNext(writer::save)
                    .doOnComplete(writer::close)
                    .doOnError(throwable -> writer.cancel());
        });
    }

    /**
     * Create a {@link DataWriter}
     * <p>
     * {@link DataWriter}s can be used to write data in a sequential manner. Buffering is then delegated to the
     * parquet layer. Remember to close all instances.
     *
     * @param dataId an opaque identifier for the data.
     * @param schema the schema used to create the records.
     * @param token  an authentication token.
     * @return an instance of {@link DataWriter} ready to accept records.
     * @throws IOException if any I/O errors occurs.
     */
    public DataWriter writeData(String dataId, Schema schema, String token) throws IOException {
        return new DataWriter(dataId, schema);
    }

    /**
     * Read a sequence of {@link GenericRecord}s from the bucket storage.
     *
     * @param dataId the identifier for the data.
     * @param schema the schema used to create the records.
     * @param token  an authentication token.
     * @param cursor a cursor on record number.
     * @return a {@link Flowable} of records.
     */
    public Flowable<GenericRecord> readData(String dataId, Schema schema, String token, Cursor<Long> cursor) {
        // TODO: Do something with token.
        // TODO: Handle projection.
        // TODO: Handle filtering.
        if (cursor != null) {
            // Convert to pos + size
            long start = Math.max(cursor.getAfter(), 0);
            int size = Math.max(cursor.getNext(), 0);
            // Note the size + 1 here. The filter implementation goes through all the groups unless
            // we return one extra and limit with actual size. This will probably be fixed by parquet team at some
            // point.
            FilterCompat.Filter filter = FilterCompat.get(new PagedRecordFilter(start, start + size + 1));
            return readRecords(dataId, schema, filter).limit(size);
        } else {
            return readRecords(dataId, schema, FilterCompat.NOOP);
        }
    }

    private Flowable<GenericRecord> readRecords(String dataId, Schema schema, FilterCompat.Filter filter) {
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

    public ParquetMetadata readMetadata(String dataId, String token) throws IOException {
        String path = configuration.getLocation() + dataId;
        try (SeekableByteChannel channel = backend.read(path)) {
            ParquetFileReader parquetFileReader = provider.getMetadata(channel);
            return parquetFileReader.getFooter();
        }
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

    /**
     * Writer abstraction.
     */
    public class DataWriter implements AutoCloseable {
        private final String tmpPath;
        private final String path;
        private final ParquetWriter<GenericRecord> parquetWriter;


        private DataWriter(String datasetId, Schema schema) throws IOException {
            path = configuration.getLocation() + datasetId;
            tmpPath = path + ".tmp";
            SeekableByteChannel channel = backend.write(tmpPath);
            parquetWriter = provider.getWriter(channel, schema);
        }

        /**
         * Push down a generic record.
         * <p>
         * Note that the record might be buffered. Calling {@link #close()} after this method
         * guaranties that the given record is written.
         *
         * @param record the record to save.
         */
        public void save(GenericRecord record) throws IOException {
            parquetWriter.write(record);
        }

        public void cancel() throws IOException {
            try {
                parquetWriter.close();
            } finally {
                backend.delete(tmpPath);
            }
        }

        /**
         * Write all buffered records, close the file and rename it.
         */
        @Override
        public void close() throws IOException {
            try {
                parquetWriter.close();
                backend.move(tmpPath, path);
            } catch (IOException ioe) {
                try {
                    cancel();
                } catch (IOException deleteIoe) {
                    ioe.addSuppressed(deleteIoe);
                }
                throw ioe;
            }
        }
    }
}

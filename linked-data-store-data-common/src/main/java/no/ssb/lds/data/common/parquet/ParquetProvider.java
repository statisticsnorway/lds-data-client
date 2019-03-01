package no.ssb.lds.data.common.parquet;

import no.ssb.lds.data.common.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;

public class ParquetProvider {

    public static final int CHUNK_SIZE = 8 * 1024 * 1024; // 8MB
    private final Configuration configuration;

    public ParquetProvider(Configuration configuration) {
        this.configuration = configuration;
    }


    public ParquetReader<GenericRecord> getReader(SeekableByteChannel input, Schema schema) throws IOException {
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new InputFile() {

            @Override
            public long getLength() throws IOException {
                return input.size();
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return new DelegatingSeekableInputStream(Channels.newInputStream(input)) {
                    @Override
                    public long getPos() throws IOException {
                        return input.position();
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        input.position(newPos);
                    }
                };
            }
        }).build();
        return reader;
    }

    public ParquetWriter<GenericRecord> getWriter(SeekableByteChannel output, Schema schema) throws IOException {
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new OutputFile() {

            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                return new DelegatingPositionOutputStream(Channels.newOutputStream(output)) {
                    @Override
                    public long getPos() throws IOException {
                        return output.position();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return new DelegatingPositionOutputStream(Channels.newOutputStream(output)) {
                    @Override
                    public long getPos() throws IOException {
                        return output.position();
                    }
                };
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        }).withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(configuration.getParquet().getPageSize())
                .withRowGroupSize(CHUNK_SIZE * 4)
                .build();
        return writer;
    }

}

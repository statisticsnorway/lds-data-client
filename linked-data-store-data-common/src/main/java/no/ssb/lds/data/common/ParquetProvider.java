package no.ssb.lds.data.common;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;

import java.nio.channels.SeekableByteChannel;

public interface ParquetProvider {

    ParquetReader<Group> getReader(SeekableByteChannel input);

    ParquetWriter<Group> getWriter(SeekableByteChannel output);

}

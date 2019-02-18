package no.ssb.lds.data.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Binary file system abstraction.
 */
public interface BinaryBackend {

    SeekableByteChannel read(String path) throws FileNotFoundException;

    SeekableByteChannel write(String path) throws IOException;
}

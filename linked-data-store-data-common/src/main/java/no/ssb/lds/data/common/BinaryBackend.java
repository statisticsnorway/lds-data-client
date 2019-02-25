package no.ssb.lds.data.common;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/**
 * Binary file system abstraction.
 */
public interface BinaryBackend {


    SeekableByteChannel read(String path) throws IOException;

    SeekableByteChannel write(String path) throws IOException;
}

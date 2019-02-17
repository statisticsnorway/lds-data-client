package no.ssb.lds.data.common;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Binary file system abstraction.
 */
public interface BinaryBackend {

    SeekableByteChannel read(String path);

    SeekableByteChannel write(String path);
}

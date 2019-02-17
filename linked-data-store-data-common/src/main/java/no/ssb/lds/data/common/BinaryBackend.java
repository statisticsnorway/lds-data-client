package no.ssb.lds.data.common;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Binary file system abstraction.
 */
public interface BinaryBackend {

    ReadableByteChannel read(String path);

    WritableByteChannel write(String path);
}

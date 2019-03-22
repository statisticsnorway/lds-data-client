package no.ssb.lds.data.client;

import io.reactivex.Flowable;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/**
 * Binary file system abstraction.
 */
public interface BinaryBackend {

    Flowable<String> list(String path) throws IOException;

    SeekableByteChannel read(String path) throws IOException;

    SeekableByteChannel write(String path) throws IOException;
}

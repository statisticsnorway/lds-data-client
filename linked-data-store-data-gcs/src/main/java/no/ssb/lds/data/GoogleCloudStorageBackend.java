package no.ssb.lds.data;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * A simple BinaryBackend for Google Cloud Storage.
 */
public class GoogleCloudStorageBackend implements BinaryBackend {

    private final Storage storage;
    private final String prefix;
    private final Integer writeChunkSize;
    private final Integer readChunkSize;

    public GoogleCloudStorageBackend(Configuration configuration) {
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.prefix = configuration.getDataPrefix();
        this.writeChunkSize = configuration.getGoogleCloud().getWriteChunkSize();
        this.readChunkSize = configuration.getGoogleCloud().getReadChunkSize();
    }

    @Override
    public SeekableByteChannel read(String path) throws IOException {
        Blob blob = storage.get(getBlobId(path));
        ReadChannel reader = blob.reader();
        reader.setChunkSize(readChunkSize);
        return new SeekableReadChannel(reader, readChunkSize, blob.getSize());
    }

    @Override
    public SeekableByteChannel write(String path) throws IOException {
        Blob blob = storage.create(BlobInfo.newBuilder(getBlobId(path)).build());
        WriteChannel writer = blob.writer();
        writer.setChunkSize(writeChunkSize);
        return new SeekableByteChannel() {

            long pos = 0;

            @Override
            public int read(ByteBuffer dst) throws IOException {
                throw new UnsupportedOperationException("not readable");
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                int written = writer.write(src);
                pos += written;
                return written;
            }

            @Override
            public long position() throws IOException {
                return pos;
            }

            @Override
            public SeekableByteChannel position(long newPosition) throws IOException {
                throw new UnsupportedOperationException("not seekable");
            }

            @Override
            public long size() throws IOException {
                return position();
            }

            @Override
            public SeekableByteChannel truncate(long size) throws IOException {
                throw new UnsupportedOperationException("truncate not supported");
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        };
    }

    private BlobId getBlobId(String path) throws IOException {
        try {
            URI uri = new URI(prefix + path);
            String bucket = uri.getHost();
            String name = uri.getPath();
            if (name.startsWith("/")) {
                name = name.substring(1);
            }
            return BlobId.of(bucket, name);
        } catch (URISyntaxException use) {
            throw new IOException("could not get bucket and name from " + prefix + path);
        }
    }
}

package no.ssb.lds.data.service.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.huxhorn.sulky.ulid.ULID;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.undertow.attribute.ExchangeAttributes;
import io.undertow.predicate.Predicate;
import io.undertow.predicate.Predicates;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.client.BinaryBackend;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Lists the versions of a data.
 */
public class ListVersionsHandler implements HttpHandler {

    public static final String DATA_ID = "dataId";
    public static final String PATH = "/data/{" + DATA_ID + "}";

    // TODO: The same logic could be lifted to a parent class.
    private static final List<String> MEDIA_TYPES = List.of(
            "application/no.ssb.lds.data.versions+json",
            "application/json"
    );
    private static final Predicate MEDIA_TYPE_PREDICATE = Predicates.contains(
            ExchangeAttributes.requestHeader(Headers.ACCEPT), MEDIA_TYPES.toArray(String[]::new)
    );

    private final ObjectMapper mapper = new ObjectMapper();
    private final ParquetProvider provider;
    private final BinaryBackend backend;

    private final Logger logger = LoggerFactory.getLogger(ListVersionsHandler.class);

    public ListVersionsHandler(ParquetProvider provider, BinaryBackend backend) {
        this.provider = Objects.requireNonNull(provider);
        this.backend = Objects.requireNonNull(backend);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        if (Methods.OPTIONS.equals(exchange.getRequestMethod())) {
            HeaderMap responseHeaders = exchange.getResponseHeaders();
            exchange.setStatusCode(StatusCodes.OK);
            responseHeaders.add(Headers.ALLOW, Methods.GET_STRING);
            responseHeaders.add(Headers.ALLOW, Methods.OPTIONS_STRING);
            responseHeaders.addAll(Headers.ACCEPT, MEDIA_TYPES);
            return;
        }

        // Check the media type.
        if (!MEDIA_TYPE_PREDICATE.resolve(exchange)) {
            exchange.setStatusCode(StatusCodes.UNSUPPORTED_MEDIA_TYPE);
            return;
        }

        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String dataId = parameters.get(DATA_ID);

        Iterable<VersionRepresentation> versions = backend.list(dataId).concatMapEager(path -> {
            return Flowable.just(path).subscribeOn(Schedulers.computation())
                    .map(version -> {
                        logger.debug("Reading metadata for {}", version);
                        try (SeekableByteChannel channel = backend.read(version)) {
                            Long size = channel.size();
                            return new VersionRepresentation(version.split("/")[1], size, provider.getMetadata(channel));
                        }
                    });

        }).toList().blockingGet();

        try {
            exchange.setStatusCode(StatusCodes.OK);
            exchange.startBlocking();
            mapper.writeValue(exchange.getOutputStream(), versions);
        } catch (Exception e) {
            logger.warn("failed to list versions ({})", exchange, e);
        }
    }

    /**
     * Version representation.
     */
    public static class VersionRepresentation {

        private final UUID id;
        private final Instant createdAt;
        private final String createdBy;
        private final Long rows;
        private final ParquetMetadata metadata;
        private final Long size;

        public VersionRepresentation(String versionId, Long size, ParquetFileReader reader) {
            this.id = UUID.fromString(versionId);
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(id.getMostSignificantBits());
            bb.putLong(id.getLeastSignificantBits());
            this.createdAt = Instant.ofEpochMilli(ULID.fromBytes(bb.array()).timestamp());
            this.createdBy = reader.getFileMetaData().getCreatedBy();
            this.rows = reader.getRecordCount();
            this.metadata = reader.getFooter();
            this.size = size;
        }

        public ParquetMetadata getMetadata() {
            return metadata;
        }

        public String getCreatedAt() {
            return createdAt.toString();
        }

        public String getCreatedBy() {
            return createdBy;
        }

        public UUID getId() {
            return id;
        }

        public Long getRows() {
            return rows;
        }

        public Long getSize() {
            return size;
        }

    }
}

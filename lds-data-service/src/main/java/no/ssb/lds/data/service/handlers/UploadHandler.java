package no.ssb.lds.data.service.handlers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.huxhorn.sulky.ulid.ULID;
import io.reactivex.Completable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.ResponseCodeHandler;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.client.BinaryBackend;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.UnsupportedMediaTypeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

/**
 * Handle uploads.
 * <p>
 * A concurrent map of UUID / UploadHandle is used to save the current handles.
 */
public class UploadHandler implements HttpHandler {

    public static final String UPLOAD_ID = "uploadId";
    public static final String PATH = "/upload/{" + UPLOAD_ID + "}";
    private static final Logger LOG = LoggerFactory.getLogger(UploadHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ULID ulid = new ULID();
    private final ConcurrentMap<UUID, Handle> uploads;

    private final BinaryBackend backend;
    private final DataClient dataClient;

    private final Scheduler conversionScheduler = Schedulers.from(
            Executors.newCachedThreadPool(),
            true
    );

    public UploadHandler(
            BinaryBackend backend,
            DataClient client) {
        this.dataClient = client;
        this.uploads = new ConcurrentHashMap<>();
        this.backend = backend;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String uploadIdString = parameters.get(UPLOAD_ID);
        UUID uploadId = UUID.fromString(uploadIdString);
        if (!uploads.containsKey(uploadId)) {
            ResponseCodeHandler.HANDLE_404.handleRequest(exchange);
        } else {
            HttpString method = exchange.getRequestMethod();
            if (Methods.GET.equals(method)) {
                handleStatus(exchange, uploadId);
            } else if (Methods.POST.equals(method)) {
                handleUpload(exchange, uploadId);
            } else if (Methods.DELETE.equals(method)) {
                handleCancel(exchange, uploadId);
            }
        }
    }

    /**
     * Returns a UUID equal to the original dataset id if it was a UUID.
     * <p>
     * This is to avoid upload contention. The original ID is saved in the
     * parquet file and upload status.
     */
    private UUID getDataUUID(String dataId) {
        String originalId = dataId;
        try {
            return UUID.fromString(originalId);
        } catch (IllegalArgumentException iae) {
            // Generate a UUID to avoid upload performances issues:
            return UUID.randomUUID();
        }
    }

    /**
     * Returns a ULID.
     */
    private UUID getVersionUUID(Instant instant) {
        ULID.Value value = ulid.nextValue(instant.toEpochMilli());
        return new UUID(
                value.getMostSignificantBits(),
                value.getLeastSignificantBits()
        );
    }

    private void handleCancel(HttpServerExchange exchange, UUID uploadId) {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        Handle cancelled = uploads.remove(uploadId);
        if (cancelled != null) {
            cancelled.getDisposable().dispose();
            exchange.setStatusCode(StatusCodes.OK);
        } else {
            exchange.setStatusCode(StatusCodes.GONE);
        }
    }

    private void handleStatus(HttpServerExchange exchange, UUID uploadId) throws IOException {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        Handle handle = uploads.get(uploadId);
        exchange.startBlocking();
        mapper.writeValue(exchange.getOutputStream(), new StatusRepresentation(handle));
    }

    private void handleUpload(HttpServerExchange exchange, UUID uploadId) throws IOException, UnsupportedMediaTypeException {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        Handle handle = uploads.get(uploadId);
        String mediaType = handle.getMediaType();

        // Check media type.
        HeaderValues contentTypes = exchange.getRequestHeaders().get(Headers.CONTENT_TYPE);
        if (!contentTypes.contains(mediaType)) {
            exchange.setStatusCode(StatusCodes.UNSUPPORTED_MEDIA_TYPE);
            return;
        }

        try {
            exchange.startBlocking();
            Completable completable = dataClient.convertAndWrite(handle.getPath(), handle.getSchema(),
                    exchange.getInputStream(), mediaType, "");
            completable.doOnDispose(() -> LOG.info("Cancelled."))
                    .doOnSubscribe(disposable -> {
                        LOG.info("Subscribed.");
                        handle.setDisposable(disposable);
                    })
                    .subscribeOn(conversionScheduler)
                    .doFinally(() -> {
                        LOG.info("Done.");
                    }).blockingAwait();

            // TODO: Update GSIM object.
            // client...

            // Sends the location of the created resource.
            exchange.setStatusCode(StatusCodes.CREATED);
            exchange.getResponseHeaders().add(
                    Headers.LOCATION,
                    GetDataHandler.PATH
                            .replace("{" + GetDataHandler.DATA_ID + "}", handle.getDataId())
                            .replace("{" + GetDataHandler.VERSION_ID + "}", handle.getVersionId())
            );
        } finally {
            uploads.remove(uploadId);
        }
    }

    public UUID createUpload(String dataId, String mediaType, Schema schema) {
        UUID uploadId = UUID.randomUUID();
        Handle handle = new Handle(
                schema,
                uploadId,
                getDataUUID(dataId),
                getVersionUUID(Instant.now()),
                mediaType
        );
        uploads.put(uploadId, handle);
        return uploadId;
    }

    public static class Handle {

        private final UUID versionId;
        private final UUID dataId;
        private final UUID uploadId;
        private final String mediaType;
        private final Schema schema;
        private Disposable disposable;

        public Handle(Schema schema, UUID uploadId, UUID dataId, UUID versionId, String mediaType) {
            this.schema = schema;
            this.versionId = versionId;
            this.dataId = dataId;
            this.uploadId = uploadId;
            this.mediaType = mediaType;
        }

        @JsonIgnore
        public Schema getSchema() {
            return schema;
        }

        @JsonProperty
        public String getMediaType() {
            return mediaType;
        }

        @JsonIgnore
        public Disposable getDisposable() {
            return disposable;
        }

        public void setDisposable(Disposable disposable) {
            this.disposable = disposable;
        }

        @JsonProperty
        public String getUploadId() {
            return uploadId.toString();
        }

        @JsonProperty
        public String getDataId() {
            return dataId.toString();
        }

        @JsonProperty
        public String getVersionId() {
            return versionId.toString();
        }

        @JsonProperty
        public String getPath() {
            return String.join("/", dataId.toString(), versionId.toString());
        }
    }

    /**
     * Simple jackson binding compatible wrapper.
     */
    private static class StatusRepresentation {
        private final Handle handle;

        private StatusRepresentation(Handle handle) {
            this.handle = handle;
        }

        @JsonProperty
        public String getUploadId() {
            return handle.getUploadId();
        }

        @JsonProperty
        public String getVersionId() {
            return handle.getVersionId();
        }

        @JsonProperty
        public String getDatasetId() {
            return handle.getDataId();
        }
    }
}

package no.ssb.lds.data.service.handlers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Completable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.ResponseCodeHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.model.GSIMDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public class UploadHandler implements HttpHandler {

    public static final String UPLOAD_ID = "uploadId";
    public static final String PATH = "/upload/{" + UPLOAD_ID + "}";
    private final Logger LOG = LoggerFactory.getLogger(UploadHandler.class);

    // The datasets we are uploading to.
    private final ConcurrentMap<String, GSIMDataset> uploads;

    // Maps the upload handles to status.
    private final ConcurrentMap<String, UUID> uploadsToVersion;

    // The statuses.
    private final ConcurrentMap<UUID, FormatConverter.Status> statuses;

    // The disposable.
    private final ConcurrentMap<UUID, Disposable> disposable;

    private final ObjectMapper mapper = new ObjectMapper();

    private final List<FormatConverter> converters;
    private final BinaryBackend backend;
    private final Scheduler conversionScheduler = Schedulers.from(Executors.newCachedThreadPool(), true);

    public UploadHandler(
            ConcurrentMap<String, GSIMDataset> uploads,
            List<FormatConverter> converters,
            BinaryBackend backend
    ) {
        this.uploads = uploads;
        this.converters = converters;
        this.backend = backend;
        this.statuses = new ConcurrentHashMap<>();
        this.uploadsToVersion = new ConcurrentHashMap<>();
        this.disposable = new ConcurrentHashMap<>();
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String uploadId = parameters.get(UPLOAD_ID);
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
     * Must return a UUID that is <strong>lexicographically</strong> sortable
     * by time of creation.
     */
    private UUID getVersionUUID(Instant instant) {
        // TODO: Use ulid or ksuid.
        return UUID.randomUUID();
    }

    private void handleCancel(HttpServerExchange exchange, String uploadId) {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        uploads.remove(uploadId);
        UUID versionUUID = uploadsToVersion.remove(uploadId);
        Disposable disposable = this.disposable.get(versionUUID);
        disposable.dispose();
        //FormatConverter.Status status = statuses.remove(versionUUID);
        //status.dispose();

        exchange.setStatusCode(StatusCodes.OK);
    }

    private void handleStatus(HttpServerExchange exchange, String uploadId) throws IOException {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        UUID versionUUID = uploadsToVersion.get(uploadId);
        GSIMDataset dataset = uploads.get(uploadId);
        FormatConverter.Status status = statuses.get(versionUUID);
        exchange.startBlocking();
        StatusRepresentation representation = new StatusRepresentation(status, versionUUID, uploadId, dataset.getId());
        mapper.writeValue(exchange.getOutputStream(), representation);
    }

    private void handleUpload(HttpServerExchange exchange, String uploadId) throws IOException {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        // Find the converter base on the content type.
        Optional<FormatConverter> converter = findFormatConverter(exchange);
        if (converter.isPresent()) {

            FormatConverter formatConverter = converter.get();
            String mediaType = formatConverter.getMediaType();

            UUID versionId = getVersionUUID(Instant.now());
            GSIMDataset dataset = uploads.get(uploadId);
            String datasetId = dataset.getId();

            try (SeekableByteChannel channel = backend.write(String.format("%s/%s", datasetId, versionId))) {

                exchange.startBlocking();
                FormatConverter.Status status = formatConverter.write(
                        exchange.getInputStream(),
                        channel,
                        mediaType,
                        dataset
                );
                statuses.put(versionId, status);
                uploadsToVersion.put(uploadId, versionId);

                Completable.wrap(status)
                        .doOnDispose(() -> LOG.info("Cancelled."))
                        .doOnSubscribe(disposable -> {
                            LOG.info("Subscribed.");
                            this.disposable.put(versionId, disposable);
                        })
                        .subscribeOn(conversionScheduler)
                        .doFinally(() -> {
                            LOG.info("Done.");
                            uploadsToVersion.remove(uploadId);
                            uploads.remove(uploadId);
                            statuses.remove(versionId);
                            disposable.remove(versionId);
                        }).blockingAwait();

                // TODO: Update GSIM object.
                // client...

                // Sends the location of the created resource.
                exchange.setStatusCode(StatusCodes.CREATED);
                exchange.getResponseHeaders().add(
                        Headers.LOCATION,
                        GetDataHandler.PATH.replace("{" + GetDataHandler.DATA_ID + "}", datasetId)
                                .replace("{" + GetDataHandler.VERSION_ID + "}", versionId.toString())
                );
            }

        } else {
            // TODO: Correct error header.
        }
    }

    private Optional<FormatConverter> findFormatConverter(HttpServerExchange exchange) {
        Iterator<String> mediaTypes = exchange.getRequestHeaders().get(Headers.CONTENT_TYPE).descendingIterator();
        while (mediaTypes.hasNext()) {
            String mediaType = mediaTypes.next();
            for (FormatConverter converter : converters) {
                if (converter.doesSupport(mediaType)) {
                    return Optional.of(converter);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Simple jackson binding compatible wrapper.
     */
    private static class StatusRepresentation {
        private final FormatConverter.Status status;
        private final UUID versionId;
        private final String uploadId;
        private final String datasetId;

        private StatusRepresentation(FormatConverter.Status status, UUID versionId, String uploadId, String datasetId) {
            this.status = status;
            this.versionId = versionId;
            this.uploadId = uploadId;
            this.datasetId = datasetId;
        }

        @JsonProperty
        public String getUploadId() {
            return uploadId;
        }

        @JsonProperty
        public long readBytes() {
            return status.readBytes();
        }

        @JsonProperty
        public long writtenBytes() {
            return status.writtenBytes();
        }

        @JsonProperty
        public UUID getVersionId() {
            return versionId;
        }

        @JsonProperty
        public String getDatasetId() {
            return datasetId;
        }
    }
}

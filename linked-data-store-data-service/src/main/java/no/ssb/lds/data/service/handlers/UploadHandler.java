package no.ssb.lds.data.service.handlers;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.ResponseCodeHandler;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.client.ClientV1;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.model.GSIMDataset;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

public class UploadHandler implements HttpHandler {

    public static final String UPLOAD_ID = "uploadId";
    public static final String PATH = "/upload/{" + UPLOAD_ID + "}";

    private final ConcurrentMap<String, GSIMDataset> uploads;
    private final List<FormatConverter> converters;
    private final BinaryBackend backend;
    private final ClientV1 client;

    public UploadHandler(ConcurrentMap<String, GSIMDataset> uploads, List<FormatConverter> converters, BinaryBackend backend, ClientV1 client) {
        this.uploads = uploads;
        this.converters = converters;
        this.backend = backend;
        this.client = client;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String uploadId = parameters.get(UPLOAD_ID);
        if (!uploads.containsKey(uploadId)) {
            ResponseCodeHandler.HANDLE_404.handleRequest(exchange);
        } else {
            handleUpload(exchange, uploadId);
        }

    }

    private void handleUpload(HttpServerExchange exchange, String uploadId) throws IOException {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        Optional<FormatConverter> converter = findFormatConverter(exchange);
        if (converter.isPresent()) {
            FormatConverter formatConverter = converter.get();
            String mediaType = formatConverter.getMediaType();
            exchange.startBlocking();
            // TODO: Should use the requested header?
            // TODO: Do something with status.

            String versionId = Instant.now().toString();
            GSIMDataset dataset = uploads.get(uploadId);
            String datasetId = dataset.getId();
            try (SeekableByteChannel channel = backend.write(String.format("%s/%s", datasetId, versionId))) {

                formatConverter.write(exchange.getInputStream(), channel, mediaType, dataset)
                        .ignoreElements().blockingAwait();
                // TODO: Update GSIM object.
                // client...
                exchange.setStatusCode(StatusCodes.CREATED);
                exchange.getResponseHeaders().add(
                        Headers.LOCATION,
                        GetDataHandler.PATH.replace("{" + GetDataHandler.DATA_ID + "}", datasetId)
                                .replace("{" + GetDataHandler.VERSION_ID + "}", versionId)
                );
            } finally {
                uploads.remove(uploadId);
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
}

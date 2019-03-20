package no.ssb.lds.data.service.handlers;

import io.reactivex.Completable;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.ResponseCodeHandler;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.client.ClientV1;
import no.ssb.lds.data.client.BinaryBackend;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.model.GSIMDataset;

import java.nio.channels.SeekableByteChannel;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static no.ssb.lds.data.common.converter.FormatConverter.Cursor;
import static no.ssb.lds.data.common.converter.FormatConverter.Status;

public class GetDataHandler implements HttpHandler {

    public static final String DATA_ID = "dataId";
    public static final String VERSION_ID = "versionId";
    public static final String PATH = "/data/{" + DATA_ID + "}/{" + VERSION_ID + "}";
    public static final String LATEST = "latest";

    private final BinaryBackend backend;
    private final ClientV1 client;
    private final List<FormatConverter> converters;


    public GetDataHandler(BinaryBackend backend, ClientV1 client, List<FormatConverter> converters) {
        this.backend = backend;
        this.client = client;
        this.converters = converters;
    }

    private Cursor<Long> getCursor(HttpServerExchange exchange) {
        // TODO Look for start and size for now. This should be extended later.
        Map<String, Deque<String>> queryParameters = exchange.getQueryParameters();
        long start = 0L;
        int size = 100;
        Deque<String> startParam = queryParameters.get("start");
        if (startParam != null && !startParam.isEmpty()) {
            start = Long.parseLong(startParam.getFirst());
        }
        Deque<String> sizeParam = queryParameters.get("size");
        if (sizeParam != null && !sizeParam.isEmpty()) {
            size = Integer.parseInt(sizeParam.getFirst());
        }
        return new Cursor<>(size, start);
    }

    public void handleGet(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String dataId = parameters.get(DATA_ID);
        String versionId = parameters.get(VERSION_ID);

        String path;
        if (versionId.equals(LATEST)) {
            path = backend.list(dataId).blockingFirst();
            if (path == null) {
                ResponseCodeHandler.HANDLE_404.handleRequest(exchange);
                return;
            }
        } else {
            path = String.format("%s/%s", dataId, versionId);
        }

        GSIMDataset dataset = client.getDataset("todo");

        SeekableByteChannel channel = backend.read(path);

        Optional<FormatConverter> converter = findFormatConverter(exchange);
        if (converter.isPresent()) {
            FormatConverter formatConverter = converter.get();
            String mediaType = formatConverter.getMediaType();
            exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, mediaType);
            Cursor<Long> cursor = getCursor(exchange);
            exchange.startBlocking();
            // TODO: Should use the requested header?
            Status status = formatConverter.read(channel, exchange.getOutputStream(), mediaType, dataset, cursor);
            Completable.wrap(status).blockingAwait();
            // TODO: is flush() called by close()?
            exchange.getOutputStream().flush();
            exchange.endExchange();
        } else {
            exchange.setStatusCode(StatusCodes.UNSUPPORTED_MEDIA_TYPE);
        }
    }

    public void handleHead(HttpServerExchange exchange) throws Exception {

    }

    public void handleOptions(HttpServerExchange exchange) throws Exception {
        // TODO: Set content types.
        // TODO: Set methods.
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (Methods.GET.equals(exchange.getRequestMethod())) {
            handleGet(exchange);
        }
    }

    /**
     * Finds the first format converter that is compatible with the requested content type from the exchange.
     */
    private Optional<FormatConverter> findFormatConverter(HttpServerExchange exchange) {
        Iterator<String> mediaTypes = exchange.getRequestHeaders().get(Headers.ACCEPT).descendingIterator();
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

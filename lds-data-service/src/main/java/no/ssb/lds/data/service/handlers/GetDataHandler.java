package no.ssb.lds.data.service.handlers;

import io.reactivex.Completable;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.ResponseCodeHandler;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.gsim.client.GsimClient;
import no.ssb.lds.data.client.BinaryBackend;
import no.ssb.lds.data.client.Cursor;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.UnsupportedMediaTypeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;

public class GetDataHandler implements HttpHandler {

    public static final String DATA_ID = "dataId";
    public static final String VERSION_ID = "versionId";
    public static final String PATH = "/data/{" + DATA_ID + "}/{" + VERSION_ID + "}";
    public static final String LATEST = "latest";
    private static final Logger logger = LoggerFactory.getLogger(GetDataHandler.class);
    private final BinaryBackend backend;
    private final DataClient dataClient;
    private final GsimClient gsimClient;


    public GetDataHandler(BinaryBackend backend, GsimClient gsimClient, DataClient dataClient) {
        this.backend = backend;
        this.gsimClient = gsimClient;
        this.dataClient = dataClient;
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

        // Check media type.
        Iterator<String> mediaTypes = exchange.getRequestHeaders().get(Headers.ACCEPT).descendingIterator();
        String mediaType = null;
        while (mediaTypes.hasNext()) {
            String nextMediaType = mediaTypes.next();
            if (dataClient.canConvert(nextMediaType)) {
                mediaType = nextMediaType;
                break;
            }
        }
        if (mediaType == null) {
            exchange.setStatusCode(StatusCodes.UNSUPPORTED_MEDIA_TYPE);
            return;
        }

        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String dataId = parameters.get(DATA_ID);
        String versionId = parameters.get(VERSION_ID);

        // Check if exists.
        String path;
        if (LATEST.equals(versionId)) {
            path = backend.list(dataId).blockingFirst();
            if (path == null) {
                ResponseCodeHandler.HANDLE_404.handleRequest(exchange);
                return;
            }
        } else {
            path = String.format("%s/%s", dataId, versionId);
        }

        Schema schema = gsimClient.getSchema(dataId).blockingGet();
        Cursor<Long> cursor = getCursor(exchange);

        exchange.startBlocking();
        OutputStream outputStream = exchange.getOutputStream();
        try {
            Completable completable = dataClient.readAndConvert(path, schema, outputStream, mediaType, "", cursor);
            completable.blockingAwait();
        } catch (UnsupportedMediaTypeException umte) {
            logger.debug("unsupported media type: " + mediaType);
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
}

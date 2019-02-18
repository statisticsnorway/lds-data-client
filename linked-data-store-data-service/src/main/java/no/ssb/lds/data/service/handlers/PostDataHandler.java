package no.ssb.lds.data.service.handlers;

import io.reactivex.Completable;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.client.Client;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.model.GSIMDataset;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static io.undertow.util.Headers.LOCATION;

public class PostDataHandler implements HttpHandler {

    private static final String DATA_ID = "dataId";
    public static final String PATH = "/data/{" + DATA_ID + "}";

    private final ConcurrentMap<String, GSIMDataset> uploads;

    private final Client client;

    public PostDataHandler(ConcurrentMap<String, GSIMDataset> uploads, Client client) {
        this.uploads = uploads;
        this.client = client;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (Methods.POST.equals(exchange.getRequestMethod())) {
            handlePost(exchange);
        }
    }

    private void handlePost(HttpServerExchange exchange) {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        // Extract path variables.
        Map<String, String> parameters = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
        String dataId = parameters.get(DATA_ID);

        GSIMDataset dataset = client.getDataset(dataId);
        if (uploads.containsValue(dataset)) {
            // TODO.

        } else {
            String uploadId = UUID.randomUUID().toString();
            uploads.put(uploadId, dataset);
            String location = UploadHandler.PATH.replace("{" + UploadHandler.UPLOAD_ID + "}", uploadId);
            exchange.getResponseHeaders().add(LOCATION, location);
            exchange.setStatusCode(StatusCodes.ACCEPTED);
        }
    }
}

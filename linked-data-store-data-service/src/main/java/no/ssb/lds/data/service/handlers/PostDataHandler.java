package no.ssb.lds.data.service.handlers;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.lds.data.client.Client;
import no.ssb.lds.data.common.model.GSIMDataset;

import java.util.Map;
import java.util.UUID;

import static io.undertow.util.Headers.LOCATION;

public class PostDataHandler implements HttpHandler {

    private static final String DATA_ID = "dataId";
    public static final String PATH = "/data/{" + DATA_ID + "}/upload";

    private final UploadHandler uploadHandler;

    private final Client client;

    public PostDataHandler(UploadHandler uploadHandler, Client client) {
        this.uploadHandler = uploadHandler;
        this.client = client;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
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
        UUID uploadId = uploadHandler.createUpload(dataset);
        String location = UploadHandler.PATH.replace("{" + UploadHandler.UPLOAD_ID + "}", uploadId.toString());
        exchange.getResponseHeaders().add(LOCATION, location);
        exchange.setStatusCode(StatusCodes.ACCEPTED);
    }
}

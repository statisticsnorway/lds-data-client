package no.ssb.lds.data.service.handlers;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import no.ssb.gsim.client.GsimClient;
import no.ssb.gsim.client.graphql.GetUnitDatasetQuery;
import no.ssb.lds.data.client.DataClient;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static io.undertow.util.Headers.LOCATION;

public class PostDataHandler implements HttpHandler {

    private static final String DATA_ID = "dataId";
    public static final String PATH = "/data/{" + DATA_ID + "}/upload";

    private final UploadHandler uploadHandler;

    private final GsimClient client;
    private final DataClient dataClient;

    public PostDataHandler(UploadHandler uploadHandler, GsimClient client, DataClient dataClient) {
        this.uploadHandler = uploadHandler;
        this.client = client;
        this.dataClient = dataClient;
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

        // Check media type.
        Iterator<String> mediaTypes = exchange.getRequestHeaders().get(Headers.CONTENT_TYPE).descendingIterator();
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

        Schema schema = client.getSchema(dataId).blockingGet();
        UUID uploadId = uploadHandler.createUpload(dataId, mediaType, schema);

        String location = UploadHandler.PATH.replace("{" + UploadHandler.UPLOAD_ID + "}", uploadId.toString());
        exchange.getResponseHeaders().add(LOCATION, location);
        exchange.setStatusCode(StatusCodes.ACCEPTED);
    }
}

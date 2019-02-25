package no.ssb.lds.data.service;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.server.handlers.PathTemplateHandler;
import io.undertow.util.Methods;
import no.ssb.lds.data.GoogleCloudStorageBackend;
import no.ssb.lds.data.client.ClientV1;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.converter.csv.CsvConverter;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import no.ssb.lds.data.service.handlers.GetDataHandler;
import no.ssb.lds.data.service.handlers.PostDataHandler;
import no.ssb.lds.data.service.handlers.UploadHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Server {

    public static void main(String[] args) throws IOException {

        Undertow.Builder builder = Undertow.builder();

        Map<String, String> config = new HashMap<>();
        config.put("lds.url", "http://localhost:9090/");
        config.put("data", "gs://ssb-data-a/data/");
        config.put("backend", "HADOOP");

        String backendType = config.get("backend");
        String prefix = config.get("data");
        BinaryBackend backend = new GoogleCloudStorageBackend(prefix);

//        if (backendType.equals("LOCAL")) {
//            backend = new LocalBackend(prefix);
//        } else if (backendType.equals("HADOOP")) {
//            Configuration configuration = new Configuration();
//            FileSystem fileSystem = FileSystem.newInstance(URI.create("file://home/hadrien/Projects/SSB/linked-data-store-data/data/hadoop/"), configuration);
//            backend = new HadoopBackend(fileSystem);
//        } else {
//            throw new IllegalArgumentException("unkown backend " + backendType);
//        }

        // Fetch the datasets from LDS.
        ClientV1 clientV1 = new ClientV1(config.get("lds.url") + "graphql");

        // List of uploads.
        ConcurrentHashMap<String, GSIMDataset> uploads = new ConcurrentHashMap<>();

        // Supported converters.
        List<FormatConverter> converters = List.of(new CsvConverter(new ParquetProvider()));

        PathTemplateHandler pathTemplate = Handlers.pathTemplate(true);

        // Handles get requests.
        pathTemplate.add(GetDataHandler.PATH, new AllowedMethodsHandler(
                new GetDataHandler(backend, clientV1, converters),
                Methods.GET, Methods.HEAD, Methods.OPTIONS
        ));

        // Creates uploads.
        pathTemplate.add(PostDataHandler.PATH, new AllowedMethodsHandler(
                new PostDataHandler(uploads, clientV1),
                Methods.POST, Methods.OPTIONS
        ));

        // Handles upload and conversion.
        pathTemplate.add(UploadHandler.PATH, new AllowedMethodsHandler(
                new UploadHandler(uploads, converters, backend),
                Methods.DELETE, Methods.GET, Methods.POST, Methods.OPTIONS
        ));

        builder.addHttpListener(8080, "0.0.0.0", pathTemplate);

        builder.build().start();

    }
}

package no.ssb.lds.data.service;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.server.handlers.PathTemplateHandler;
import io.undertow.util.Methods;
import no.ssb.lds.data.client.ClientV1;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.converter.csv.CsvConverter;
import no.ssb.lds.data.common.model.GSIMDataset;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import no.ssb.lds.data.service.handlers.GetDataHandler;
import no.ssb.lds.data.service.handlers.PostDataHandler;
import no.ssb.lds.data.service.handlers.UploadHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Server {

    public static void main(String[] args) throws MalformedURLException {

        Undertow.Builder builder = Undertow.builder();

        Map<String, String> config = new HashMap<>();
        config.put("lds.url", "http://localhost:9090/");
        config.put("data", "./data/");

        // Fetch the datasets from LDS.
        ClientV1 clientV1 = new ClientV1(config.get("lds.url") + "graphql");

        // Idiot backend for now.
        BinaryBackend backend = new BinaryBackend() {
            @Override
            public SeekableByteChannel read(String path) throws FileNotFoundException {
                File file = new File(config.get("data") + path);
                return new FileInputStream(file).getChannel();
            }

            @Override
            public SeekableByteChannel write(String path) throws IOException {
                File file = new File(config.get("data") + path);
                File dir = file.getParentFile();
                if (!dir.exists() && !dir.mkdirs()) {
                    throw new IOException("could not create " + dir);
                }
                if (!file.createNewFile()) {
                    throw new IOException("file " + file + " already exist");
                }
                return new FileOutputStream(file).getChannel();
            }
        };

        // List of uploads.
        ConcurrentHashMap<String, GSIMDataset> uploads = new ConcurrentHashMap<>();

        // Supported converters.
        List<FormatConverter> converters = List.of(new CsvConverter(new ParquetProvider()));

        PathTemplateHandler pathTemplate = Handlers.pathTemplate(true);

        // Handles get requests.
        pathTemplate.add(GetDataHandler.PATH, new AllowedMethodsHandler(
                new GetDataHandler(backend, clientV1, converters), Methods.GET, Methods.HEAD, Methods.OPTIONS
        ));

        // Creates uploads.
        pathTemplate.add(PostDataHandler.PATH, new AllowedMethodsHandler(
                new PostDataHandler(uploads, clientV1), Methods.POST, Methods.OPTIONS
        ));

        // Handles upload and conversion.
        pathTemplate.add(UploadHandler.PATH, new AllowedMethodsHandler(
                new UploadHandler(uploads, converters, backend, clientV1), Methods.POST, Methods.OPTIONS
        ));

        builder.addHttpListener(8080, "0.0.0.0", pathTemplate);

        builder.build().start();

    }
}

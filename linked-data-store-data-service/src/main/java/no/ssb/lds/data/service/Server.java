package no.ssb.lds.data.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.PathTemplateHandler;
import io.undertow.util.Methods;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.lds.data.GoogleCloudStorageBackend;
import no.ssb.lds.data.client.ClientV1;
import no.ssb.lds.data.common.BinaryBackend;
import no.ssb.lds.data.common.Configuration;
import no.ssb.lds.data.common.converter.FormatConverter;
import no.ssb.lds.data.common.converter.csv.CsvConverter;
import no.ssb.lds.data.common.converter.csv.JsonConverter;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import no.ssb.lds.data.service.handlers.GetDataHandler;
import no.ssb.lds.data.service.handlers.ListVersionsHandler;
import no.ssb.lds.data.service.handlers.PostDataHandler;
import no.ssb.lds.data.service.handlers.UploadHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws IOException {

        // TODO: Fix number parsing.
        Configuration configuration = getConfiguration();

        logger.info("Initializing lds client");
        // Fetch the datasets from LDS.
        ClientV1 clientV1 = new ClientV1(
                configuration.getLdsServer().toString() + configuration.getGraphqlPath()
        );

        logger.info("Initializing google cloud storage backend");
        BinaryBackend backend = new GoogleCloudStorageBackend(configuration);

        // Supported converters.
        logger.info("Initializing converters");
        ParquetProvider parquetProvider = new ParquetProvider(configuration);
        List<FormatConverter> converters = List.of(
                new CsvConverter(parquetProvider),
                new JsonConverter(parquetProvider, new ObjectMapper())
        );
        for (FormatConverter converter : converters) {
            logger.info("Converter {}", converter);
        }

        PathTemplateHandler pathTemplate = Handlers.pathTemplate(true);

        // Handles get requests.
        pathTemplate.add(GetDataHandler.PATH, new AllowedMethodsHandler(
                new GetDataHandler(backend, clientV1, converters),
                Methods.GET, Methods.HEAD, Methods.OPTIONS
        ));

        // Handles upload and conversion.
        UploadHandler uploadHandler = new UploadHandler(converters, backend);
        pathTemplate.add(UploadHandler.PATH, new AllowedMethodsHandler(
                uploadHandler,
                Methods.DELETE, Methods.GET, Methods.POST, Methods.OPTIONS
        ));

        // Handles get requests.
        pathTemplate.add(ListVersionsHandler.PATH, new AllowedMethodsHandler(
                new ListVersionsHandler(parquetProvider, backend),
                Methods.GET, Methods.HEAD, Methods.OPTIONS
        ));

        // Creates uploads.
        pathTemplate.add(PostDataHandler.PATH, new AllowedMethodsHandler(
                new PostDataHandler(uploadHandler, clientV1),
                Methods.GET, Methods.POST, Methods.OPTIONS
        ));

        HttpHandler rootHandler;
        if (logger.isTraceEnabled()) {
            rootHandler = Handlers.requestDump(pathTemplate);
        } else {
            rootHandler = pathTemplate;
        }

        rootHandler = Handlers.header(rootHandler, "Access-Control-Allow-Origin", "*");
        rootHandler = Handlers.header(rootHandler, "Access-Control-Allow-Headers", "*");
        rootHandler = Handlers.header(rootHandler, "Access-Control-Expose-Headers", "*");


        // Wait for all requests to complete
        GracefulShutdownHandler shutdownHandler = Handlers.gracefulShutdown(rootHandler);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down");
            shutdownHandler.shutdown();
            try {
                logger.info("Waiting for all requests to complete...");
                shutdownHandler.awaitShutdown();
            } catch (InterruptedException e) {
                logger.warn("Could not shutdown gracefully");
            }
        }, "undertow shutdown hook"));

        Integer port = configuration.getPort();
        String host = configuration.getHost();
        logger.info("Starting undertow on {}:{}", host, port);
        Undertow.builder().addHttpListener(port, host, shutdownHandler).build().start();
    }

    private static Map<String, Object> splitMap(SortedMap<String, String> config) {
        TreeMap<String, Object> map = new TreeMap<>();
        for (String key : config.keySet()) {
            List<String> parts = Arrays.asList(key.split("\\."));
            if (parts.size() > 1) {
                String baseKey = parts.get(0);
                if (!map.containsKey(baseKey)) {
                    map.put(baseKey, new TreeMap<String, String>());
                }
                if (map.get(baseKey) instanceof TreeMap) {
                    SortedMap<String, String> subMap = (TreeMap<String, String>) map.get(baseKey);
                    subMap.put(String.join(".", parts.subList(1, parts.size())), config.get(key));
                } else {
                    logger.warn("Root configuration {}={} dropped", baseKey, map.get(baseKey));
                }
            } else {
                map.put(key, config.get(key));
            }
        }
        for (String key : map.keySet()) {
            Object value = map.get(key);
            if (value instanceof SortedMap) {
                map.replace(key, splitMap((SortedMap<String, String>) value));
            }
        }
        return map;
    }

    /**
     * Parse and transform the configuration.
     */
    private static Configuration getConfiguration() throws IOException {
        try {
            DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                    .propertiesResource("application.properties")
                    .propertiesResource("application_override.properties")
                    .environment("LSD_DATA_")
                    .systemProperties()
                    .build();

            // Write the configuration to JSON then parse it again.
            ObjectMapper mapper = new ObjectMapper();
            mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

            Map<String, Object> objectMap = splitMap(new TreeMap<>(configuration.asMap()));
            String stringConfiguration = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectMap);
            logger.debug("Got configuration: \n" + stringConfiguration);
            return mapper.readValue(stringConfiguration, Configuration.class);
        } catch (Exception e) {
            throw new IOException("Configuration error: " + e.getMessage(), e);
        }
    }
}

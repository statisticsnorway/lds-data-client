package no.ssb.lds.data.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.PathTemplateHandler;
import io.undertow.util.Methods;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.gsim.client.GsimClient;
import no.ssb.lds.data.GoogleCloudStorageBackend;
import no.ssb.lds.data.client.BinaryBackend;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.ParquetProvider;
import no.ssb.lds.data.client.converters.FormatConverter;
import no.ssb.lds.data.service.handlers.GetDataHandler;
import no.ssb.lds.data.service.handlers.ListVersionsHandler;
import no.ssb.lds.data.service.handlers.PostDataHandler;
import no.ssb.lds.data.service.handlers.UploadHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws IOException {

        Configuration configuration = getConfiguration();

        BinaryBackend backend;
        if (configuration.getGoogleCloud() != null) {
            logger.info("Initializing google cloud storage backend");
            backend = new GoogleCloudStorageBackend(configuration.getGoogleCloud());
        } else {
            throw new IllegalArgumentException("missing backend");
        }

        if (configuration.getCache() != null) {
            Caffeine<Object, Object> cache = Caffeine.from(configuration.getCache().getSpec());
            logger.info("Setting up in memory cache: {}", cache);
            backend = new CachedBackend(
                    backend, cache, configuration.getCache().getBlockSize(),
                    ByteBuffer::allocateDirect
            );
        }

        ObjectMapper mapper = new ObjectMapper();

        DataClient.Builder dataClientBuilder = DataClient.builder().withBinaryBackend(backend);
        logger.info("Initializing data client");
        List<FormatConverter> converters = List.of(
                new no.ssb.lds.data.client.converters.JsonConverter(mapper),
                new no.ssb.lds.data.client.converters.CsvConverter()
        );

        for (FormatConverter converter : converters) {
            logger.info(" - converter {}", converter);
            dataClientBuilder.withFormatConverter(converter);
        }

        ParquetProvider parquetProvider = new ParquetProvider(configuration.getParquet());
        dataClientBuilder.withParquetProvider(parquetProvider);
        dataClientBuilder.withConfiguration(configuration.getData());

        logger.info("Initializing GSIM client");
        DataClient dataClient = dataClientBuilder.build();
        GsimClient gsimClient = GsimClient.builder()
                .withDataClient(dataClient)
                .withConfiguration(configuration.getGsim())
                .build();

        PathTemplateHandler pathTemplate = Handlers.pathTemplate(true);

        // Handles get requests.
        pathTemplate.add(GetDataHandler.PATH, new AllowedMethodsHandler(
                new GetDataHandler(backend, gsimClient, dataClient),
                Methods.GET, Methods.HEAD, Methods.OPTIONS
        ));

        // Handles upload and conversion.
        UploadHandler uploadHandler = new UploadHandler(backend, dataClient);
        pathTemplate.add(UploadHandler.PATH, new AllowedMethodsHandler(
                uploadHandler,
                Methods.DELETE, Methods.GET, Methods.POST, Methods.OPTIONS
        ));

        // Handles get requests.
        pathTemplate.add(ListVersionsHandler.PATH, new AllowedMethodsHandler(
                new ListVersionsHandler(parquetProvider, backend, configuration.getData().getLocation()),
                Methods.GET, Methods.HEAD, Methods.OPTIONS
        ));

        // Creates uploads.
        pathTemplate.add(PostDataHandler.PATH, new AllowedMethodsHandler(
                new PostDataHandler(uploadHandler, gsimClient, dataClient),
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
            ObjectWriter prettyPrinter = mapper.writerWithDefaultPrettyPrinter();
            String stringConfiguration = prettyPrinter.writeValueAsString(objectMap);
            Configuration readValue = mapper.readValue(stringConfiguration, Configuration.class);

            if (logger.isDebugEnabled()) {
                logger.debug("Got configuration: \n" + stringConfiguration);
            } else {
                logger.info("Got configuration: \n" + prettyPrinter.writeValueAsString(readValue));
            }

            return readValue;
        } catch (Exception e) {
            throw new IOException("Configuration error: " + e.getMessage(), e);
        }
    }
}

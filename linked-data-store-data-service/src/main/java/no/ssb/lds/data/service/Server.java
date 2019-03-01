package no.ssb.lds.data.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.AllowedMethodsHandler;
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
import no.ssb.lds.data.common.parquet.ParquetProvider;
import no.ssb.lds.data.service.handlers.GetDataHandler;
import no.ssb.lds.data.service.handlers.PostDataHandler;
import no.ssb.lds.data.service.handlers.UploadHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Server {

    public static void main(String[] args) throws IOException {

        // TODO: Fix number parsing.
        Configuration configuration = getConfiguration();

//        Configuration configuration = new Configuration();
//        configuration.setParquet(new Configuration.Parquet());
//        configuration.setGoogleCloud(new Configuration.GoogleCloud());
//        configuration.getParquet().setPageSize(8 * 1024 * 1024);
//        configuration.getParquet().setRowGroupSize(8 * 8 * 1024 * 1024);


        Undertow.Builder builder = Undertow.builder();

        // TODO: Make this dynamic.
        BinaryBackend backend = new GoogleCloudStorageBackend(configuration);

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
        ClientV1 clientV1 = new ClientV1(
                configuration.getLdsServer().toString() + configuration.getGraphqlPath()
        );

        // Supported converters.
        List<FormatConverter> converters = List.of(new CsvConverter(new ParquetProvider(configuration)));

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

        // Creates uploads.
        pathTemplate.add(PostDataHandler.PATH, new AllowedMethodsHandler(
                new PostDataHandler(uploadHandler, clientV1),
                Methods.POST, Methods.OPTIONS
        ));

        builder.addHttpListener(8080, "0.0.0.0", pathTemplate);

        builder.build().start();

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
                    // TODO: Warn about dropped root properties.
                    // # foo is dropped.
                    // foo.bar=xx
                    // foo=yy
                    //
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
            return mapper.readValue(mapper.writeValueAsString(objectMap), Configuration.class);
        } catch (Exception e) {
            throw new IOException("Could not create configuration: " + e.getMessage(), e);
        }
    }
}

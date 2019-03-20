package no.ssb.lds.data.common.converter.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import no.ssb.lds.data.common.converter.AbstractFormatConverter;
import no.ssb.lds.data.common.parquet.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

@Deprecated
public class JsonConverter extends AbstractFormatConverter {

    public static final int BUFFER_SIZE = 1024;
    private static final String MEDIA_TYPE = "application/json";
    private final ObjectMapper mapper;

    public JsonConverter(ParquetProvider provider, ObjectMapper mapper) {
        super(provider);
        this.mapper = mapper;
    }

    @Override
    protected Flowable<GenericRecord> encode(InputStream input, Schema schema) throws IOException {

        List<Schema.Field> fields = schema.getFields();
        JsonParser parser = mapper.getFactory().createParser(input);
        parser.nextToken();
        parser.nextValue();
        MappingIterator<ObjectNode> objects = mapper.readValues(parser, ObjectNode.class);

        List<Deque<JsonNode>> buffers = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            buffers.add(field.pos(), new ArrayDeque<>(BUFFER_SIZE));
        }

        List<Deque<Object>> convertedBuffers = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            convertedBuffers.add(field.pos(), new ArrayDeque<>(BUFFER_SIZE));
        }

        ArrayList<Function<JsonNode, Object>> converters = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            converters.add(i, getConverter(fields.get(i)));
        }

        // Flow of objects.
        Flowable<ObjectNode> nodes = Flowable.generate(emitter -> {
            try {
                if (objects.hasNext()) {
                    emitter.onNext(objects.next());
                } else {
                    emitter.onComplete();
                }
            } catch (Exception e) {
                emitter.onError(e);
            }
        });


        // Convert to columns
        Flowable<List<List<JsonNode>>> columns = nodes.buffer(BUFFER_SIZE).map(rows -> {
            List<List<JsonNode>> group = new ArrayList<>();
            for (Schema.Field field : fields) {
                List<JsonNode> column = new ArrayList<>(BUFFER_SIZE);
                for (JsonNode row : rows) {
                    column.add(row.get(field.name()));
                }
                group.add(column);
            }
            return group;
        });

        Flowable<List<List<Object>>> singleFlowable = columns.map(cols ->
                Flowable.fromIterable(cols)
                        .zipWith(converters, (column, converter) ->
                                Flowable.fromIterable(column)
                                        .subscribeOn(Schedulers.computation())
                                        .map(converter::apply)
                                        .toList().blockingGet()
                        ).toList().blockingGet()
        );

        return singleFlowable.concatMapIterable(lists -> {
            ArrayList<GenericRecordBuilder> builders = new ArrayList<>(BUFFER_SIZE);
            for (int i = 0; i < lists.get(0).size(); i++) {
                builders.add(new GenericRecordBuilder(schema));
            }
            for (Schema.Field field : fields) {
                List<Object> column = lists.get(field.pos());
                for (int i = 0; i < column.size(); i++) {
                    builders.get(i).set(field, column.get(i));
                }
            }
            return builders;
        }).map(GenericRecordBuilder::build);


//        int left = BUFFER_SIZE;
//        while (left-- > 0 && objects.hasNext()) {
//            JsonNode object = objects.next();
//            for (Schema.Field field : fields) {
//                buffers.get(field.pos()).push(object.get(field.name()));
//            }
//        }
//
//        // Convert
//        for (Schema.Field field : fields) {
//            Function<JsonNode, Object> converter = converters.get(field.pos());
//            Deque<JsonNode> buffer = buffers.get(field.pos());
//            Deque<Object> convertedBuffer = convertedBuffers.get(field.pos());
//            if (!convertedBuffer.isEmpty()) {
//                throw new AssertionError();
//            }
//            while (!buffer.isEmpty()) {
//                convertedBuffer.push(converter.apply(buffer.pop()));
//            }
//        }
//
//        int buffered = BUFFER_SIZE - left;
//        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
//        while (buffered-- > 0) {
//            for (Schema.Field field : schema.getFields()) {
//                Deque<Object> deque = convertedBuffers.get(field.pos());
//                recordBuilder.set(field, deque.pop());
//            }
//            return recordBuilder.build();
//        }

    }

    private Function<JsonNode, Object> getConverter(Schema.Field field) {
        Schema.Type type = field.schema().getType();
        field.schema().getLogicalType();
        Function<JsonNode, Object> converter;
        switch (type) {
            case STRING:
                converter = JsonNode::asText;
                break;
            case LONG:
                converter = JsonNode::asLong;
                break;
            case BOOLEAN:
                converter = JsonNode::asBoolean;
                break;
            case FLOAT:
                converter = JsonNode::asDouble;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type");
        }
        return converter;
    }

    @Override
    protected Completable decode(Flowable<GenericRecord> records, OutputStream output, Schema schema) {
        return Completable.using(
                () -> {
                    JsonGenerator generator = mapper.getFactory().createGenerator(output);
                    generator.writeStartArray();
                    return generator;
                },
                generator -> {
                    return records.doOnNext(record -> {
                        generator.writeRawValue(record.toString());
                    }).ignoreElements();
                },
                generator -> {
                    generator.writeEndArray();
                    generator.flush();
                }
        );
    }

    @Override
    public boolean doesSupport(String mediaType) {
        return mediaType.toLowerCase().startsWith(getMediaType());
    }

    @Override
    public String getMediaType() {
        return "application/json";
    }
}

package io.kestra.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.params.ParameterizedTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

@MicronautTest
public class KafkaTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;


    @Test
    void fromAsString() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Map<String, String> mapProperties = new LinkedHashMap<>();
        mapProperties.put("bootstrap.servers", "localhost:9092");

        Map<String, String> configProperties = new LinkedHashMap<>();
        configProperties.put("schema.registry.url", "http://localhost:8085");

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 50; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("key", "string")
                .put("value", Map.of(
                    "username", "Kestra",
                    "tweet", "Kestra is open source",
                    "timestamp", System.currentTimeMillis() / 1000
                ))
                .put("timestamp", Instant.now().toEpochMilli())
                .build()
            );
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Produce task = Produce.builder()
            .properties(mapProperties)
            .serializerConfig(configProperties)
            .avroValueSchema("{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name of the user account on Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The content of the user's Twitter message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix epoch time in milliseconds\"}],\"doc:\":\"A basic schema for storing Twitter messages\"}")
            .keySerializer("String")
            .valueSerializer("AVRO")
            .topic("newTopic")
            .partition(0)
            .from(uri.toString())
            .build();

        Produce.Output runOutput = task.run(runContext);
    }

    @Test
    void fromAsMapAvro() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Map<String, String> mapProperties = new LinkedHashMap<>();
        mapProperties.put("bootstrap.servers", "localhost:9092");

        Map<String, String> configProperties = new LinkedHashMap<>();
        configProperties.put("schema.registry.url", "http://localhost:8085");

        Produce task = Produce.builder()
            .properties(mapProperties)
            .serializerConfig(configProperties)
            .avroValueSchema("{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name of the user account on Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The content of the user's Twitter message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix epoch time in milliseconds\"}],\"doc:\":\"A basic schema for storing Twitter messages\"}")
            .keySerializer("String")
            .valueSerializer("AVRO")
            .topic("testTopic")
            .partition(0)
            .from(ImmutableMap.builder()
                .put("key", "string")
                .put("value", Map.of(
                    "username", "Kestra",
                    "tweet", "Kestra is open source",
                    "timestamp", System.currentTimeMillis() / 1000
                ))
                .put("timestamp", Instant.now().toEpochMilli())
                .build()
            )
            .build();

        Produce.Output runOutput = task.run(runContext);
    }

    static Stream<Arguments> sourceAsMap() {
//        Map to test null value in not Void serializer
        HashMap<String, Object> map = new HashMap<>();
        map.put("key", "string");
        map.put("value", null);
        map.put("timestamp", Instant.now().toEpochMilli());
//        Map to test Void serializer
        HashMap<String, Object> mapVoid = new HashMap<>();
        map.put("key", "{\"Test\":\"OK\"}");
        map.put("value", null);
        map.put("timestamp", Instant.now().toEpochMilli());

        return Stream.of(
            Arguments.of("String", "Integer", ImmutableMap.builder()
                .put("key", "string")
                .put("value", 1)
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
            Arguments.of("Double", "Long", ImmutableMap.builder()
                .put("key", 1.2D)
                .put("value", 1L)
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
//            Used to test null value insertion
            Arguments.of("String", "String", map),
            Arguments.of("Short", "ByteArray", ImmutableMap.builder()
                .put("key", (short) 5)
                .put("value", new byte[]{0b000101})
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
            Arguments.of("ByteBuffer", "UUID", ImmutableMap.builder()
                .put("key", ByteBuffer.allocate(10))
                .put("value", UUID.randomUUID())
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
            Arguments.of("JSON", "Void",mapVoid)
        );
    }

    @ParameterizedTest
    @MethodSource("sourceAsMap")
    void fromAsMap(String keySerializer, String valueSerializer, Map from) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Map<String, String> mapProperties = new LinkedHashMap<>();
        mapProperties.put("bootstrap.servers", "localhost:9092");

        Map<String, String> configProperties = new LinkedHashMap<>();
        configProperties.put("schema.registry.url", "http://localhost:8085");

        Produce task = Produce.builder()
            .properties(mapProperties)
            .serializerConfig(configProperties)
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)
            .topic("randomTopic")
            .partition(0)
            .from(from)
            .build();

        Produce.Output runOutput = task.run(runContext);
    }

    @Test
    void fromAsArray() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Map<String, String> mapProperties = new LinkedHashMap<>();
        mapProperties.put("bootstrap.servers", "localhost:9092");

        Map<String, String> configProperties = new LinkedHashMap<>();
        configProperties.put("schema.registry.url", "http://localhost:8085");

        Produce task = Produce.builder()
            .properties(mapProperties)
            .serializerConfig(configProperties)
            .avroValueSchema("{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name of the user account on Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The content of the user's Twitter message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix epoch time in milliseconds\"}],\"doc:\":\"A basic schema for storing Twitter messages\"}")
            .keySerializer("String")
            .valueSerializer("AVRO")
            .topic("lastTopic")
            .partition(0)
            .from(List.of(
                ImmutableMap.builder()
                    .put("key", "string")
                    .put("value", Map.of(
                        "username", "Kestra",
                        "tweet", "Kestra is open source",
                        "timestamp", System.currentTimeMillis() / 1000
                    ))
                    .put("timestamp", Instant.now().toEpochMilli())
                    .build(),
                ImmutableMap.builder()
                    .put("key", "string")
                    .put("value", Map.of(
                        "username", "Kestra",
                        "tweet", "Kestra is open source",
                        "timestamp", System.currentTimeMillis() / 1000
                    ))
                    .put("timestamp", Instant.now().toEpochMilli())
                    .build()
            ))
            .build();

        Produce.Output runOutput = task.run(runContext);
    }
}

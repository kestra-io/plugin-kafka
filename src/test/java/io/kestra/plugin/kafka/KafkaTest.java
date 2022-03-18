package io.kestra.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.params.ParameterizedTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
public class KafkaTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    @Value("${kafka.registry}")
    private String registry;

    @SuppressWarnings("unchecked")
    @Test
    void fromAsString() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".trs");
        OutputStream output = new FileOutputStream(tempFile);

        for (int i = 0; i < 50; i++) {
            FileSerde.write(output, ImmutableMap.builder()
                .put("key", "key-" + i)
                .put("value", ImmutableMap.builder()
                    .put("username", "Kestra-" + i)
                    .put("tweet", "Kestra is open source")
                    .put("timestamp", System.currentTimeMillis() / 1000)
                    .put("instant", ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant())
                    .put("zonedDatetimeMillis", ZonedDateTime.parse("2022-01-03T00:00:00+01:00"))
                    .put("zonedDatetimeMicros", ZonedDateTime.parse("2022-01-03T00:00:00+01:00"))
                    .put("offsetDatetimeMillis", OffsetDateTime.parse("2022-01-03T00:00:00+01:00"))
                    .put("offsetDatetimeMicros", OffsetDateTime.parse("2022-01-03T00:00:00+01:00"))
                    .build()
                )
                .put("timestamp", Instant.now().toEpochMilli())
                .build()
            );
        }

        URI uri = storageInterface.put(URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Produce task = Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry, "avro.use.logical.type.converters", "true"))
            .valueAvroSchema(
                "{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[" +
                    "{\"name\":\"username\",\"type\":\"string\"}," +
                    "{\"name\":\"tweet\",\"type\":\"string\"}," +
                    "{\"name\":\"timestamp\",\"type\":\"long\"}," +
                    "{\"name\":\"instant\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"zonedDatetimeMillis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"zonedDatetimeMicros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}," +
                    "{\"name\":\"offsetDatetimeMillis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                    "{\"name\":\"offsetDatetimeMicros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}" +
                    "]}"
            )
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.AVRO)
            .topic(topic)
            .from(uri.toString())
            .build();

        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(50));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "auto.offset.reset" , "earliest",
                "max.poll.records", "15"
            ))
            .groupId(IdUtils.create())
            .serdeProperties(task.getSerdeProperties())
            .keyDeserializer(task.getKeySerializer())
            .valueDeserializer(task.getValueSerializer())
            .pollDuration(Duration.ofSeconds(5))
            .topic(task.getTopic())
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(50));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(consumeOutput.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.size(), is(50));

        Map<String, Object> value = (Map<String, Object>) result.get(0).get("value");
        assertThat(value.get("instant"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("zonedDatetimeMillis"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("zonedDatetimeMicros"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("offsetDatetimeMillis"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("offsetDatetimeMicros"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
    }

    @Test
    void fromAsMapAvro() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry))
            .valueAvroSchema("{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name of the user account on Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The content of the user's Twitter message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix epoch time in milliseconds\"}],\"doc:\":\"A basic schema for storing Twitter messages\"}")
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.AVRO)
            .topic(topic)
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
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "max.poll.records", "15"
            ))
            .serdeProperties(task.getSerdeProperties())
            .keyDeserializer(task.getKeySerializer())
            .valueDeserializer(task.getValueSerializer())
            .pollDuration(Duration.ofSeconds(5))
            .topic(task.getTopic())
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    static Stream<Arguments> sourceAsMap() {
        // Map to test null value in not Void serializer
        HashMap<String, Object> map = new HashMap<>();
        map.put("key", "string");
        map.put("value", null);
        map.put("timestamp", Instant.now().toEpochMilli());

        // Map to test Void serializer
        HashMap<String, Object> mapVoid = new HashMap<>();
        map.put("key", "{\"Test\":\"OK\"}");
        map.put("value", null);
        map.put("timestamp", Instant.now().toEpochMilli());

        return Stream.of(
            Arguments.of(SerdeType.STRING, SerdeType.INTEGER, ImmutableMap.builder()
                .put("key", "string")
                .put("value", 1)
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
            Arguments.of(SerdeType.DOUBLE, SerdeType.LONG, ImmutableMap.builder()
                .put("key", 1.2D)
                .put("value", 1L)
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),

            // Used to test null value insertion
            Arguments.of(SerdeType.STRING, SerdeType.STRING, map),
            Arguments.of(SerdeType.SHORT, SerdeType.BYTE_ARRAY, ImmutableMap.builder()
                .put("key", (short) 5)
                .put("value", new byte[]{0b000101})
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
            Arguments.of(SerdeType.BYTE_BUFFER, SerdeType.UUID, ImmutableMap.builder()
                .put("key", ByteBuffer.allocate(10))
                .put("value", UUID.randomUUID())
                .put("timestamp", Instant.now().toEpochMilli())
                .build()),
            Arguments.of(SerdeType.JSON, SerdeType.VOID,mapVoid)
        );
    }

    @ParameterizedTest
    @MethodSource("sourceAsMap")
    void fromAsMap(SerdeType keySerializer, SerdeType valueSerializer, Map<Object,Object> from) throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry))
            .keySerializer(keySerializer)
            .valueSerializer(valueSerializer)
            .topic(topic)
            .from(from)
            .build();

        Produce.Output runOutput = task.run(runContext);

        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "max.poll.records", "15"
            ))
            .serdeProperties(task.getSerdeProperties())
            .keyDeserializer(task.getKeySerializer())
            .valueDeserializer(task.getValueSerializer())
            .pollDuration(Duration.ofSeconds(5))
            .topic(List.of(topic))
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
    }

    @Test
    void fromAsArray() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry))
            .valueAvroSchema("{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name of the user account on Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The content of the user's Twitter message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix epoch time in milliseconds\"}],\"doc:\":\"A basic schema for storing Twitter messages\"}")
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.AVRO)
            .topic(topic)
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

        assertThat(runOutput.getMessagesCount(), is(2));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "max.poll.records", "15"
            ))
            .serdeProperties(task.getSerdeProperties())
            .keyDeserializer(task.getKeySerializer())
            .valueDeserializer(task.getValueSerializer())
            .pollDuration(Duration.ofSeconds(5))
            .topic(List.of(topic))
            .build();

        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(2));
    }
}

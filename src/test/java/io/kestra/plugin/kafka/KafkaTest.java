package io.kestra.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class KafkaTest {
    public static final String AVRO_SCHEMA_SIMPLE = "{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name of the user account on Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The content of the user's Twitter message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix epoch time in milliseconds\"}],\"doc:\":\"A basic schema for storing Twitter messages\"}";
    public static final String AVRO_SCHEMA_COMPLEX = """
        {
            "type": "record",
            "name": "twitter_schema",
            "namespace": "io.kestra.examples",
            "fields": [
              {
                "name": "username",
                "type": "string"
              },
              {
                "name": "tweet",
                "type": "string"
              },
              {
                "name": "stat",
                "type": [
                  "null", {
                    "type": "record",
                    "name": "stat",
                    "fields": [
                      {
                        "name": "followers_count",
                        "type": "long"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        """;

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
            HashMap<Object, Object> data = new HashMap<>();
            data.put("username", "Kestra-" + i);
            data.put("tweet", "Kestra is open source");
            data.put("timestamp", System.currentTimeMillis() / 1000);
            data.put("instant", ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant());
            data.put("zonedDatetimeMillis", ZonedDateTime.parse("2022-01-03T00:00:00+01:00"));
            data.put("zonedDatetimeMicros", ZonedDateTime.parse("2022-01-03T00:00:00+01:00"));
            data.put("offsetDatetimeMillis", OffsetDateTime.parse("2022-01-03T00:00:00+01:00"));
            data.put("offsetDatetimeMicros", OffsetDateTime.parse("2022-01-03T00:00:00+01:00"));
            data.put("unionLogical", i < 25 ? ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant() : null);

            FileSerde.write(output, ImmutableMap.builder()
                .put("key", "key-" + i)
                .put("value", data)
                .put("timestamp", Instant.now().toEpochMilli())
                .build()
            );
        }

        URI uri = storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

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
                    "{\"name\":\"offsetDatetimeMicros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}," +
                    "{\"name\":\"unionLogical\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"null\"]}" +
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

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, consumeOutput.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(result.size(), is(50));

        Map<String, Object> value = (Map<String, Object>) result.get(0).get("value");
        assertThat(value.get("instant"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("zonedDatetimeMillis"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("zonedDatetimeMicros"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("offsetDatetimeMillis"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("offsetDatetimeMicros"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));
        assertThat(value.get("unionLogical"), is(ZonedDateTime.parse("2022-01-03T00:00:00+01:00").toInstant()));

        value = (Map<String, Object>) result.get(49).get("value");
        assertThat(value.get("unionLogical"), is(nullValue()));
    }

    @Test
    void fromAsMapAvro() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Produce task = Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry))
            .valueAvroSchema(AVRO_SCHEMA_SIMPLE)
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.AVRO)
            .topic(topic)
            .from(record())
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
            Arguments.of(SerdeType.STRING, SerdeType.STRING, ImmutableMap.builder()
                .put("key", "{{ \"apple\" ~ \"pear\" ~ \"banana\" }}")
                .put("value", "{{ max(5,10) }}")
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
            .valueAvroSchema(AVRO_SCHEMA_SIMPLE)
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.AVRO)
            .topic(topic)
            .from(List.of(record(), record()))
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

    @Test
    void consumeProduce() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        File tempFile = createRecordFile();
        URI uri = storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Produce task = createProduceTask(topic, uri);
        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "max.poll.records", "15"
            ))
            .keyDeserializer(SerdeType.STRING)
            .valueDeserializer(SerdeType.STRING)
            .pollDuration(Duration.ofSeconds(5))
            .topic(List.of(topic))
            .build();
        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
        assertOutputFile(runContext, consumeOutput);
        Produce reproduce = createProduceTask(topic, consumeOutput.getUri());
        Produce.Output reproduceRunOutput = reproduce.run(runContext);
        assertThat(reproduceRunOutput.getMessagesCount(), is(1));
    }

    @Test
    void invalidBrokers() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        TimeoutException e = assertThrows(TimeoutException.class, () -> {
            Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", "localhost:1234", "max.block.ms", "1000"))
                .transactional(false) // if transactional the exception would be 'java.lang.IllegalStateException: Cannot attempt operation `commitTransaction` because the previous call to `initTransactions` timed out and must be retried'
                .topic(topic)
                .from(List.of(record(), record()))
                .build();

            task.run(runContext);
        });
        assertThat(e.getMessage(), containsString("not present in metadata"));


        e = assertThrows(TimeoutException.class, () -> {
            Consume task = Consume.builder()
                .properties(Map.of("bootstrap.servers", "localhost:1234", "default.api.timeout.ms", "1000"))
                .topic(topic)
                .build();

            task.run(runContext);
        });
        assertThat(e.getMessage(), containsString("Timeout expired"));

        SerializationException ex = assertThrows(SerializationException.class, () -> {
            Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", "http://localhost:1234"))
                .valueAvroSchema(AVRO_SCHEMA_SIMPLE)
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .from(record())
                .build();

            task.run(runContext);
        });

        assertThat(ex.getCause().getMessage(), containsString("Connection refused"));
    }

    @Test
    void produceComplexAvro() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        HashMap<String, Object> map = new HashMap<>();
        map.put("username", "loic");
        map.put("tweet", "Something clever");
        map.put("stat", Map.of("followers_count", 10L));
        
        Produce reproduce = Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry))
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.AVRO)
            .topic(topic)
            .valueAvroSchema(AVRO_SCHEMA_COMPLEX)
            .from(Map.of("value", map))
            .build();
        Produce.Output reproduceRunOutput = reproduce.run(runContext);
        assertThat(reproduceRunOutput.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withUnion_andRecord() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("product", Map.of("id", "v1"));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "product",
                              "type": [
                                "null",
                                {"type": "record", "name": "Version", "fields": [{"name": "id", "type": "string"}]}
                              ]
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }


    @Test
    void produceAvro_withUnion_andRecord_null() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("product", null);

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "product",
                              "type": [
                                "null",
                                {"type": "record", "name": "Version", "fields": [{"name": "id", "type": "string"}]}
                              ]
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withRecord() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("address", Map.of("city", "Paris", "country", "FR", "longitude", 2.3522, "latitude", 48.8566));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "address",
                              "type": {
                                "type": "record",
                                "name": "Address",
                                "fields": [
                                  {"name": "city", "type": "string"},
                                  {"name": "country", "type": "string"},
                                  {"name": "longitude", "type": "float"},
                                  {"name": "latitude", "type": "float"}
                                ]
                              }
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withMap() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("map", Map.of("foo", 42, "bar", 17));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "map",
                              "type": {"type": "map", "values": "int"}
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withMap_andRecord() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("map", Map.of("foo", Map.of("id", "v1"), "bar", Map.of("id", "v2")));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "map",
                              "type": {"type": "map", "values": {"type": "record", "name": "Version", "fields": [{"name": "id", "type": "string"}]}}
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withArray() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("array", List.of("foo", "bar"));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "array",
                              "type": {"type": "array", "items": "string"}
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withArray_andRecord() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("array", List.of(Map.of("id", "v1"), Map.of("id", "v2")));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "array",
                              "type": {"type": "array", "items": {"type": "record", "name": "Version", "fields": [{"name": "id", "type": "string"}]}}
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withEnum() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("state", "SUCCESS");

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "state",
                              "type": {"name": "StateEnum", "type": "enum", "symbols": ["SUCCESS", "FAILED"]}
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void produceAvro_withFixed() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();

        Map<String, Object> value = Map.of("base64", Base64.getEncoder().encode("Hello, World!".getBytes(UTF_8)));

        Produce task = Produce.builder()
                .properties(Map.of("bootstrap.servers", this.bootstrap))
                .serdeProperties(Map.of("schema.registry.url", this.registry))
                .keySerializer(SerdeType.STRING)
                .valueSerializer(SerdeType.AVRO)
                .topic(topic)
                .valueAvroSchema("""
                        {
                          "type": "record",
                          "name": "Sample",
                          "namespace": "io.kestra.examples",
                          "fields": [
                            {
                              "name": "base64",
                              "type": {"name": "Base64", "type": "fixed", "size": 16}
                            }
                          ]
                        }
                        """)
                .from(Map.of("value", value))
                .build();

        Produce.Output output = task.run(runContext);
        assertThat(output.getMessagesCount(), is(1));
    }

    @Test
    void shouldConsumeGivenTopicPattern() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        File tempFile = createRecordFile();
        URI uri = storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Produce task = createProduceTask(topic, uri);
        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "max.poll.records", "15",
                "auto.offset.reset", "earliest",
                "metadata.max.age.ms", "100"
            ))
            .keyDeserializer(SerdeType.STRING)
            .valueDeserializer(SerdeType.STRING)
            .pollDuration(Duration.ofSeconds(5))
            .topicPattern(topic.substring(0, 6) + ".*")
            .groupId(IdUtils.create())
            .build();
        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));
        assertOutputFile(runContext, consumeOutput);
        Produce reproduce = createProduceTask(topic, consumeOutput.getUri());
        Produce.Output reproduceRunOutput = reproduce.run(runContext);
        assertThat(reproduceRunOutput.getMessagesCount(), is(1));
    }

    @Test
    void shouldConsumeGivenTopicPartition() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        String topic = "tu_" + IdUtils.create();
        File tempFile = createRecordFile();
        URI uri = storageInterface.put(null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Produce task = createProduceTask(topic, uri);
        Produce.Output runOutput = task.run(runContext);
        assertThat(runOutput.getMessagesCount(), is(1));

        Consume consume = Consume.builder()
            .properties(Map.of(
                "bootstrap.servers", this.bootstrap,
                "max.poll.records", "15",
                "auto.offset.reset", "earliest",
                "metadata.max.age.ms", "100"
            ))
            .keyDeserializer(SerdeType.STRING)
            .valueDeserializer(SerdeType.STRING)
            .pollDuration(Duration.ofSeconds(5))
            .topic(topic)
            .partitions(List.of(0))
            .groupId(IdUtils.create())
            .build();
        Consume.Output consumeOutput = consume.run(runContext);
        assertThat(consumeOutput.getMessagesCount(), is(1));

        assertOutputFile(runContext, consumeOutput);
        Produce reproduce = createProduceTask(topic, consumeOutput.getUri());
        Produce.Output reproduceRunOutput = reproduce.run(runContext);
        assertThat(reproduceRunOutput.getMessagesCount(), is(1));
    }

    private static void assertOutputFile(RunContext runContext, Consume.Output consumeOutput) throws IOException {
        InputStream is = runContext.storage().getFile(consumeOutput.getUri());
        Flux<Map> reader = FileSerde.readAll(new BufferedReader(new InputStreamReader(is)), Map.class);
        Map<String, Object> result = reader.blockLast();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.containsKey("key"));
        Assertions.assertTrue(result.containsKey("value"));
        Assertions.assertTrue(result.containsKey("timestamp"));
        Assertions.assertTrue(result.containsKey("partition"));
        Assertions.assertTrue(result.containsKey("offset"));
    }

    private static File createRecordFile() throws IOException {
        HashMap<String, Object> map = new HashMap<>();
        map.put("key", "string");
        map.put("value", "string");
        map.put("headers", ImmutableMap.of("headerKey", "headerValue"));
        map.put("timestamp", Instant.now().toEpochMilli());

        File tempFile = File.createTempFile("consumeProduce", ".txt");
        OutputStream output = new FileOutputStream(tempFile);
        FileSerde.write(output, map);
        return tempFile;
    }

    private Produce createProduceTask(final String topic, final URI uri) {
        return Produce.builder()
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.STRING)
            .topic(topic)
            .from(uri.toString())
            .build();
    }

    private Map<String, Object> record() {
        return ImmutableMap.<String, Object>builder()
            .put("key", "string")
            .put("value", Map.of(
                "username", "Kestra",
                "tweet", "Kestra is open source",
                "timestamp", System.currentTimeMillis() / 1000
            ))
            .put("timestamp", Instant.now().toEpochMilli())
            .build();
    }
}

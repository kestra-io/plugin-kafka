package io.kestra.plugin.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import jakarta.annotation.Nullable;

import java.util.*;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Send a message to a Kafka topic."
)
@Plugin(
    examples = {
        @Example(
            title = "Read a CSV file, transform it and send it to Kafka.",
            full = true,
            code = """
                id: send_message_to_kafka
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE
                    description: A CSV file with columns: id, username, tweet, and timestamp.
                
                tasks:
                  - id: csv_to_ion
                    type: io.kestra.plugin.serdes.csv.CsvToIon
                    from: "{{ inputs.file }}"
                
                  - id: ion_to_avro_schema
                    type: io.kestra.plugin.scripts.nashorn.FileTransform
                    from: "{{ outputs.csv_to_ion.uri }}"
                    script: |
                      var result = {
                        "key": row.id,
                        "value": {
                          "username": row.username,
                          "tweet": row.tweet
                        },
                        "timestamp": row.timestamp,
                        "headers": {
                          "key": "value"
                        }
                      };
                      row = result
                
                  - id: avro_to_kafka
                    type: io.kestra.plugin.kafka.Produce
                    from: "{{ outputs.ion_to_avro_schema.uri }}"
                    keySerializer: STRING
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                    topic: test_kestra
                    valueAvroSchema: |
                      {"type":"record","name":"twitter_schema","namespace":"io.kestra.examples","fields":[{"name":"username","type":"string"},{"name":"tweet","type":"string"}]}
                    valueSerializer: AVRO
                """
        )
    }
)
public class Produce extends AbstractKafkaConnection implements RunnableTask<Produce.Output> {
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Kafka topic to which the message should be sent.",
        description = "Could also be passed inside the `from` property using the key `topic`."
    )
    @Nullable
    private Property<String> topic;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The content of the message to be sent to Kafka.",
        description = "Can be a Kestra internal storage URI, a map (i.e. a list of key-value pairs) or a list of maps. " +
            "The following keys are supported: `key`, `value`, `partition`, `timestamp`, and `headers`.",
        anyOf = {String.class, List.class, Map.class}
    )
    @PluginProperty(dynamic = true)
    @Deprecated
    private Object from;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setFrom(Object from) {
        this.from = from;

        if (from instanceof String str) {
            var prop = Property.of(URI.create(str));
            this.data = Data.<Map>builder().fromURI(prop).build();
        } else if (from instanceof List list) {
            var prop = Property.of((List<Map<String, Object>>) list);
            this.data = Data.<Map>builder().fromList(prop).build();
        } else if (from instanceof Map map) {
            var prop = Property.of((Map<String, Object>) map);
            this.data = Data.<Map>builder().fromMap(prop).build();
        } else {
            throw new IllegalArgumentException("'from' is from an unknown type: " + from.getClass().getName());
        }
    }

    @NotNull
    private Data<Map> data;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The serializer used for the key.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> keySerializer = Property.of(SerdeType.STRING);

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The serializer used for the value.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> valueSerializer = Property.of(SerdeType.STRING);

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Avro Schema if the key is set to `AVRO` type."
    )
    private Property<String> keyAvroSchema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Avro Schema if the value is set to `AVRO` type."
    )
    @Nullable
    private Property<String> valueAvroSchema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Whether the producer should be transactional."
    )
    @Builder.Default
    private Property<Boolean> transactional = Property.of(Boolean.TRUE);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient List<String> connectionCheckers = new ArrayList<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Output run(RunContext runContext) throws Exception {

        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        Properties properties = createProperties(this.properties, runContext);
        if (Boolean.TRUE.equals(transactional.as(runContext, Boolean.class))) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, IdUtils.create());
        }

        Properties serdesProperties = createProperties(this.serdeProperties, runContext);

        Serializer keySerial = getTypedSerializer(this.keySerializer.as(runContext, SerdeType.class),
            parseAvroSchema(runContext, keyAvroSchema)
        );
        Serializer valSerial = getTypedSerializer(this.valueSerializer.as(runContext, SerdeType.class),
            parseAvroSchema(runContext, valueAvroSchema)
        );

        keySerial.configure(serdesProperties, true);
        valSerial.configure(serdesProperties, false);


        ;
        try (KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(properties, keySerial, valSerial)) {
            if (Boolean.TRUE.equals(transactional.as(runContext, Boolean.class))) {
                producer.initTransactions();
                producer.beginTransaction();
            }

            Integer count = data.flux(runContext, Map.class, map -> map)
                .map(throwFunction(row -> {
                    producer.send(this.producerRecord(runContext, producer, (Map<String, Object>) row));
                    return 1;
                }))
                .reduce(Integer::sum)
                .blockOptional()
                .orElse(0);

            // metrics
            runContext.metric(Counter.of("records", count));

            if (Boolean.TRUE.equals(transactional.as(runContext, Boolean.class))) {
                producer.commitTransaction();
            }

            return Output.builder()
                .messagesCount(count)
                .build();
        }
    }

    @Nullable
    private static AvroSchema parseAvroSchema(RunContext runContext, @Nullable Property<String> avroSchema) throws IllegalVariableEvaluationException {
        return Optional.ofNullable(avroSchema).map(throwFunction(schema -> schema.as(runContext, String.class))).map(AvroSchema::new).orElse(null);
    }

    private ProducerRecord<Object, Object> producerRecord(RunContext runContext, KafkaProducer<Object, Object> producer, Map<String, Object> map) throws Exception {
        Object key;
        Object value;
        String topic = null;

        map = runContext.render(map);

        key = map.get("key");
        value = map.get("value");

        if (map.containsKey("topic")) {
            topic = runContext.render((String) map.get("topic"));
        } else if (this.topic != null) {
            topic = this.topic.as(runContext, String.class);
        }

        // just to test that connection to brokers works
        if (!this.connectionCheckers.contains(topic)) {
            this.connectionCheckers.add(topic);
            producer.partitionsFor(topic);
        }

        return new ProducerRecord<>(
            runContext.render(topic),
            (Integer) map.get("partition"),
            this.processTimestamp(map.get("timestamp")),
            key,
            value,
            this.processHeaders(map.get("headers"))
        );
    }

    private Long processTimestamp(Object timestamp) {
        if (timestamp == null) {
            return null;
        }

        if (timestamp instanceof Long) {
            return (Long) timestamp;
        }

        if (timestamp instanceof ZonedDateTime) {
            return ((ZonedDateTime) timestamp).toInstant().toEpochMilli();
        }

        if (timestamp instanceof Instant) {
            return ((Instant) timestamp).toEpochMilli();
        }

        if (timestamp instanceof LocalDateTime) {
            return ((LocalDateTime) timestamp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }

        if (timestamp instanceof String) {
            try {
                return ZonedDateTime.parse((String) timestamp).toInstant().toEpochMilli();
            } catch (Exception ignored) {
                return Instant.parse((String) timestamp).toEpochMilli();
            }
        }

        throw new IllegalArgumentException("Invalid type of timestamp with type '" + timestamp.getClass() + "'");
    }

    private Iterable<Header> processHeaders(Object headers) {
        if (headers == null) {
            return null;
        }

        if (headers instanceof Map) {
            return ((Map<?, ?>) headers)
                .entrySet()
                .stream()
                .map(o -> new RecordHeader((String)o.getKey(), ((String)o.getValue()).getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
        }

        // a message coming from a Consume task will have headers as a list of Map
        if (headers instanceof List) {
            return ((List<Map<String, String>>) headers)
                .stream()
                .map(map -> map.entrySet().stream().findAny().get()) // there is only one entry
                .map(o -> new RecordHeader(o.getKey(), o.getValue().getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
        }

        throw new IllegalArgumentException("Invalid type of headers with type '" + headers.getClass() + "'");
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of messages sent to a Kafka topic."
        )
        private final Integer messagesCount;
    }
}

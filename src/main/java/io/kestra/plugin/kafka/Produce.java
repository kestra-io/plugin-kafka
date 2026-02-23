package io.kestra.plugin.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish records to Kafka topics",
    description = """
        Reads messages from `from` data (key/value/topic/partition/timestamp/headers) and sends them with configurable serializers (default STRING) and transactional sends enabled by default.
        Requires `bootstrap.servers`; topic can come from task config or each payload.
        Avro serializers require the matching schema (`keyAvroSchema` / `valueAvroSchema`) and `serdeProperties` (for example `schema.registry.url`).
        Produced records can be consumed by both classic consumer groups (`groupType: CONSUMER`) and share groups (`groupType: SHARE`) in Kafka consume and trigger tasks.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Send a string to a Kafka topic",
            full = true,
            code = """
                id: kafka_producer
                namespace: company.team

                tasks:
                  - id: kafka_producer
                    type: io.kestra.plugin.kafka.Produce
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: example_topic
                    from: 
                      key: "{{ execution.id }}"
                      value: "Hello, World!"
                      timestamp: "{{ execution.startDate }}"
                      headers:
                        x-header: some value
                    keySerializer: STRING
                    valueSerializer: STRING
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                """
        ),
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
                    type: io.kestra.plugin.graalvm.js.FileTransform
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
                    topic: example_topic
                    valueAvroSchema: |
                      {"type":"record","name":"twitter_schema","namespace":"io.kestra.examples","fields":[{"name":"username","type":"string"},{"name":"tweet","type":"string"}]}
                    valueSerializer: AVRO
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records sent to Kafka topic."
        )
    }
)
public class Produce extends AbstractKafkaConnection implements RunnableTask<Produce.Output>, Data.From {
    @Schema(
        title = "Target Kafka topic when not provided in payload",
        description = "Can be overridden per record by setting `topic` inside `from`."
    )
    private Property<String> topic;

    private Object from;

    @Schema(
        title = "Serializer used for the key",
        description = "Default STRING. Options: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> keySerializer = Property.ofValue(SerdeType.STRING);

    @Schema(
        title = "Serializer used for the value",
        description = "Default STRING. Options: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @Builder.Default
    private Property<SerdeType> valueSerializer = Property.ofValue(SerdeType.STRING);

    @Schema(
        title = "Avro schema when the key serializer is AVRO",
        description = "Required if `keySerializer` is `AVRO`."
    )
    private Property<String> keyAvroSchema;

    @Schema(
        title = "Avro schema when the value serializer is AVRO",
        description = "Required if `valueSerializer` is `AVRO`."
    )
    private Property<String> valueAvroSchema;

    @Schema(
        title = "Enable transactional producer",
        description = "Defaults to true; generates a transactional.id automatically so sends are committed atomically."
    )
    @Builder.Default
    private Property<Boolean> transactional = Property.ofValue(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient List<String> connectionCheckers = new ArrayList<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Output run(RunContext runContext) throws Exception {

        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        final boolean transactional = runContext.render(this.transactional).as(Boolean.class).orElse(true);

        Properties properties = createProperties(this.properties, runContext);
        if (transactional) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, IdUtils.create());
        }

        Properties serdesProperties = createProperties(this.serdeProperties, runContext);

        Serializer keySerial = getTypedSerializer(runContext.render(this.keySerializer).as(SerdeType.class).orElse(SerdeType.STRING),
            parseAvroSchema(runContext, keyAvroSchema));
        Serializer valSerial = getTypedSerializer(runContext.render(this.valueSerializer).as(SerdeType.class).orElse(SerdeType.STRING),
            parseAvroSchema(runContext, valueAvroSchema));

        keySerial.configure(serdesProperties, true);
        valSerial.configure(serdesProperties, false);


        KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(properties, keySerial, valSerial);
        try {
            if (transactional) {
                producer.initTransactions();
                producer.beginTransaction();
            }

            Integer count = Data.from(from).read(runContext)
                .map(throwFunction(row -> {
                    producer.send(this.producerRecord(runContext, producer, row));
                    return 1;
                }))
                .reduce(Integer::sum)
                .blockOptional().orElse(0);

            // metrics
            runContext.metric(Counter.of("records", count));

            return Output.builder()
                .messagesCount(count)
                .build();
        } finally {
            if (transactional) {
                producer.commitTransaction();
            }
            producer.flush();
            producer.close();
        }
    }

    @Nullable
    private static AvroSchema parseAvroSchema(RunContext runContext, @Nullable Property<String> avroSchema) throws IllegalVariableEvaluationException {
        return runContext.render(avroSchema).as(String.class)
            .map(AvroSchema::new)
            .orElse(null);
    }

    private ProducerRecord<Object, Object> producerRecord(RunContext runContext, KafkaProducer<Object, Object> producer, Map<String, Object> map) throws Exception {
        Object key;
        Object value;
        String topic;

        map = runContext.render(map);

        key = map.get("key");
        value = map.get("value");

        if (map.containsKey("topic")) {
            topic = runContext.render((String) map.get("topic"));
        } else {
            topic = runContext.render(this.topic).as(String.class).orElse(null);
        }

        // just to test that connection to brokers works
        if (!this.connectionCheckers.contains(topic)) {
            this.connectionCheckers.add(topic);
            producer.partitionsFor(topic);
        }

        return new ProducerRecord<>(
            topic,
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

        if (timestamp instanceof Long t) {
            return t;
        }

        if (timestamp instanceof ZonedDateTime dateTime) {
            return dateTime.toInstant().toEpochMilli();
        }

        if (timestamp instanceof Instant instant) {
            return instant.toEpochMilli();
        }

        if (timestamp instanceof LocalDateTime dateTime) {
            return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }

        if (timestamp instanceof String t) {
            try {
                return ZonedDateTime.parse(t).toInstant().toEpochMilli();
            } catch (Exception ignored) {
                return Instant.parse(t).toEpochMilli();
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

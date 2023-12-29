package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
import java.util.*;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Send a message to a Kafka topic"
)
@Plugin(
    examples = {
        @Example(
            title = "Read a CSV file, transform it and send it to Kafka",
            full = true,
            code = {
                "id: send_message_to_kafka",
                "namespace: dev",
                "inputs:",
                "  - type: FILE",
                "    name: file",
                "    description: a CSV file with columns id, username, tweet, and timestamp",
                "",
                "tasks:",
                "  - id: csv_to_ion",
                "    type: io.kestra.plugin.serdes.csv.CsvReader",
                "    from: \"{{ inputs.file }}\"",
                "  - id: ion_to_avro_schema",
                "    type: io.kestra.plugin.scripts.nashorn.FileTransform",
                "    from: \"{{ outputs.csvReader.uri }}\"",
                "    script: |",
                "      var result = {",
                "        \"key\": row.id,",
                "        \"value\": {",
                "          \"username\": row.username,",
                "          \"tweet\": row.tweet",
                "        },",
                "        \"timestamp\": row.timestamp,",
                "        \"headers\": {",
                "          \"key\": \"value\"",
                "        }",
                "      };",
                "      row = result",
                "  - id: avro_to_kafka",
                "    type: io.kestra.plugin.kafka.Produce",
                "    from: \"{{ outputs.fileTransform.uri }}\"",
                "    keySerializer: STRING",
                "    properties:",
                "      bootstrap.servers: local:9092",
                "    serdeProperties:",
                "      schema.registry.url: http://local:8085",
                "    topic: test_kestra",
                "    valueAvroSchema: |",
                "      {\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"io.kestra.examples\",\"fields\":[{\"name\":\"username\",\"type\":\"string\"},{\"name\":\"tweet\",\"type\":\"string\"}]}",
                "    valueSerializer: AVRO\n"
            }
        )
    }
)
public class Produce extends AbstractKafkaConnection implements RunnableTask<Produce.Output> {
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Kafka topic to which the message should be sent",
        description = "Could also be passed inside the `from` property using the key `topic`"
    )
    @PluginProperty(dynamic = true)
    private String topic;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The content of the message to be sent to Kafka",
        description = "Can be an internal storage uri, a map (i.e. a list of key-value pairs) or a list of maps. " +
            "The following keys are supported: `key`, `value`, `partition`, `timestamp`, and `headers`.",
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The serializer used for the key",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    @Builder.Default
    private SerdeType keySerializer = SerdeType.STRING;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The serializer used for the value",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    @Builder.Default
    private SerdeType valueSerializer = SerdeType.STRING;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Avro Schema if the key is set to `AVRO` type"
    )
    @PluginProperty(dynamic = true)
    private String keyAvroSchema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Avro Schema if the value is set to `AVRO` type"
    )
    @PluginProperty(dynamic = true)
    private String valueAvroSchema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Whether the producer should be transactional"
    )
    @Builder.Default
    @PluginProperty
    private boolean transactional = true;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient List<String> connectionCheckers = new ArrayList<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Output run(RunContext runContext) throws Exception {

        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        Properties properties = createProperties(this.properties, runContext);
        if (transactional) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, IdUtils.create());
        }

        Properties serdesProperties = createProperties(this.serdeProperties, runContext);

        Serializer keySerial = getTypedSerializer(this.keySerializer);
        Serializer valSerial = getTypedSerializer(this.valueSerializer);

        keySerial.configure(serdesProperties, true);
        valSerial.configure(serdesProperties, false);


        KafkaProducer<Object, Object> producer = null;
        try {
            producer = new KafkaProducer<Object, Object>(properties, keySerial, valSerial);
            Integer count = 1;

            if (transactional) {
                producer.initTransactions();
                producer.beginTransaction();
            }

            if (this.from instanceof String || this.from instanceof List) {
                Flowable<Object> flowable;
                Flowable<Integer> resultFlowable;
                if (this.from instanceof String) {
                    URI from = new URI(runContext.render((String) this.from));
                    try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                        flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                        resultFlowable = this.buildFlowable(flowable, runContext, producer);

                        count = resultFlowable
                            .reduce(Integer::sum)
                            .blockingGet();
                    }
                } else {
                    flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                    resultFlowable = this.buildFlowable(flowable, runContext, producer);

                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                }
            } else {
                producer.send(this.producerRecord(runContext, producer, (Map<String, Object>) this.from));
            }

            // metrics
            runContext.metric(Counter.of("records", count));

            return Output.builder()
                .messagesCount(count)
                .build();
        } finally {
            if (producer != null) {
                if (transactional) {
                    producer.commitTransaction();
                }
                producer.flush();
                producer.close();
            }
        }
    }

    private GenericRecord buildAvroRecord(RunContext runContext, String dataSchema, Map<String, Object> map) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(dataSchema));
        return buildAvroRecord(schema, map);
    }

    private GenericRecord buildAvroRecord(Schema schema, Map<String, Object> map) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        for (String k : map.keySet()) {
            Object value = map.get(k);
            Schema fieldSchema = schema.getField(k).schema();
            if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
                avroRecord.put(k, buildAvroRecord(fieldSchema, (Map<String, Object>) value));
            } else {
                avroRecord.put(k, value);
            }
        }
        return avroRecord;
    }

    @SuppressWarnings("unchecked")
    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RunContext runContext, KafkaProducer<Object, Object> producer) {
        return flowable
            .map(row -> {
                producer.send(this.producerRecord(runContext, producer, (Map<String, Object>) row));
                return 1;
            });
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<Object, Object> producerRecord(RunContext runContext, KafkaProducer<Object, Object> producer, Map<String, Object> map) throws Exception {
        Object key;
        Object value;
        String topic;

        map = runContext.render(map);

        if (this.keySerializer == SerdeType.AVRO) {
            key = buildAvroRecord(runContext, this.keyAvroSchema, (Map<String, Object>) map.get("key"));
        } else {
            key = map.get("key");
        }

        if (this.valueSerializer == SerdeType.AVRO) {
            value = buildAvroRecord(runContext, this.valueAvroSchema, (Map<String, Object>) map.get("value"));
        } else {
            value = map.get("value");
        }


        if (map.containsKey("topic")) {
            topic = runContext.render((String) map.get("topic"));
        } else {
            topic = runContext.render(this.topic);
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
                .map(o -> new RecordHeader(o.getKey(), convertHeaderValue(o.getValue())))
                .collect(Collectors.toList());
        }

        throw new IllegalArgumentException("Invalid type of headers with type '" + headers.getClass() + "'");
    }

    /**
     * @param value an Array of bytes serialized as a string like "[104, 101, 97, 100, 101, 114, 86, 97, 108, 117, 101]"
     */
    private byte[] convertHeaderValue(String value) {
        if(value == null) {
            return null;
        }
        if(value.length() <= 2) {
            return new byte[]{};
        }

        var tokens = value.substring(1, value.length() -1).split(",");
        var bytes = new byte[tokens.length];
        for(int i = 0; i < tokens.length; i++) {
            bytes[i] = Byte.valueOf(tokens[i].trim());
        }
        return bytes;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of messages sent to a Kafka topic"
        )
        private final Integer messagesCount;
    }
}

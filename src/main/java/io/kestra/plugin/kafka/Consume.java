package io.kestra.plugin.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Await;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from one or more Kafka topics."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topic: test_kestra",
                "properties:",
                "  bootstrap.servers: localhost:9092",
                "serdeProperties:",
                "  schema.registry.url: http://localhost:8085",
                "keyDeserializer: STRING",
                "valueDeserializer: AVRO",
            }
        ),
        @Example(
            title = "Connect to a Kafka cluster with SSL.",
            code = {
                "properties:",
                "  security.protocol: SSL",
                "  bootstrap.servers: localhost:19092",
                "  ssl.key.password: my-ssl-password",
                "  ssl.keystore.type: PKCS12",
                "  ssl.keystore.location: my-base64-encoded-keystore",
                "  ssl.keystore.password: my-ssl-password",
                "  ssl.truststore.location: my-base64-encoded-truststore",
                "  ssl.truststore.password: my-ssl-password",
                "topic:",
                "- kestra_workerinstance",
                "keyDeserializer: STRING",
                "valueDeserializer: STRING"
            }
        )
    }
)
public class Consume extends AbstractKafkaConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
    private Object topic;

    private String groupId;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The deserializer used for the key.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @Builder.Default
    private SerdeType keyDeserializer = SerdeType.STRING;


    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The deserializer used for the value.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @Builder.Default
    private SerdeType valueDeserializer = SerdeType.STRING;

    private String since;

    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(5);

    private Integer maxRecords;

    private Duration maxDuration;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Output run(RunContext runContext) throws Exception {
        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        Properties properties = createProperties(this.properties, runContext);
        if (this.groupId != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, runContext.render(groupId));

            if (!properties.contains(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                // by default, we disable auto-commit
                properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            }
        }

        if (!properties.contains(ConsumerConfig.ISOLATION_LEVEL_CONFIG)) {
            // by default, we only read committed offsets in case of transactions
            properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }

        Properties serdesProperties = createProperties(this.serdeProperties, runContext);
        serdesProperties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        Deserializer keySerial = getTypedDeserializer(this.keyDeserializer);
        Deserializer valSerial = getTypedDeserializer(this.valueDeserializer);

        keySerial.configure(serdesProperties, true);
        valSerial.configure(serdesProperties, false);

        File tempFile = runContext.tempFile(".ion").toFile();
        try(
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));
            KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(properties, keySerial, valSerial);
        ) {
            this.affectTopics(runContext, consumer);

            Map<String, Integer> count = new HashMap<>();
            AtomicInteger total = new AtomicInteger();
            ZonedDateTime started = ZonedDateTime.now();
            ConsumerRecords<Object, Object> records;
            boolean empty;

            do {
                records = consumer.poll(this.pollDuration);
                empty = records.isEmpty();

                records.forEach(throwConsumer(record -> {
                    // using HashMap for null values
                    Map<Object, Object> map = new HashMap<>();

                    map.put("key", record.key());
                    map.put("value", record.value());
                    map.put("headers", processHeaders(record.headers()));
                    map.put("topic", record.topic());
                    map.put("partition", record.partition());
                    map.put("timestamp", Instant.ofEpochMilli(record.timestamp()));

                    FileSerde.write(output, map);

                    total.getAndIncrement();
                    count.compute(record.topic(), (s, integer) -> integer == null ? 1 : integer + 1);
                }));
            }
            while (!this.ended(empty, total, started));

            if (this.groupId != null) {
                consumer.commitSync();
            }

            // flush & close
            consumer.close();
            output.flush();

            count
                .forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));

            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
    }


    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(Boolean empty, AtomicInteger count, ZonedDateTime start) {
        if (empty) {
            return true;
        }

        if (this.maxRecords != null && count.get() > this.maxRecords) {
            return true;
        }

        if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    Collection<String> topics(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.topic instanceof String) {
            return List.of(runContext.render((String) this.topic));
        } else if (this.topic instanceof List) {
            return runContext.render((List<String>) this.topic);
        } else {
            throw new IllegalArgumentException("Invalid topics with type '" + this.topic.getClass().getName() + "'");
        }
    }

    private void affectTopics(RunContext runContext, KafkaConsumer<Object, Object> consumer) throws IllegalVariableEvaluationException {
        Collection<String> topics = this.topics(runContext);

        if (this.groupId != null) {
            consumer.subscribe(topics);

            // Wait for for the subscription to happen, this avoids possible no result for the first poll due to the poll timeout
            Await.until(() -> !consumer.subscription().isEmpty(), this.maxDuration != null ? this.maxDuration : this.pollDuration);
        } else {
            List<TopicPartition> partitions = topics
                .stream()
                .flatMap(s -> consumer.partitionsFor(s).stream())
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());

            consumer.assign(partitions);

            if (this.since == null) {
                consumer.seekToBeginning(partitions);
            } else {
                Long timestamp = ZonedDateTime
                    .parse(runContext.render(this.since))
                    .toInstant()
                    .toEpochMilli();

                consumer
                    .offsetsForTimes(
                        partitions.stream()
                            .map(topicPartition -> new AbstractMap.SimpleEntry<>(
                                topicPartition,
                                timestamp
                            ))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    )
                    .forEach((topicPartition, offsetAndTimestamp) -> {
                        consumer.seek(topicPartition, offsetAndTimestamp.timestamp());
                    });
            }
        }
    }

    private static List<Pair<String, String>> processHeaders(Headers headers) {
         return StreamSupport
            .stream(headers.spliterator(), false)
            .map(header -> Pair.of(header.key(), Arrays.toString(header.value())))
            .collect(Collectors.toList());
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of messages consumed from a Kafka topic."
        )
        private final Integer messagesCount;

        @Schema(
            title = "URI of a file in Kestra's internal storage containing the messages."
        )
        private URI uri;
    }
}

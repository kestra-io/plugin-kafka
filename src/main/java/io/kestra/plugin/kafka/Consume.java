package io.kestra.plugin.kafka;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Await;
import io.kestra.core.utils.Rethrow;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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

    private String topicPattern;

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

    @Getter(AccessLevel.PACKAGE)
    private ConsumerSubscription subscription;

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
        try (
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));
            KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(properties, keySerial, valSerial);
        ) {
            this.subscription = topicSubscription(runContext);
            this.subscription.subscribe(consumer, this);

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

    ConsumerSubscription topicSubscription(final RunContext runContext) throws IllegalVariableEvaluationException {
        validateConfiguration();

        if (this.topic != null && groupId == null) {
            return new TopicPartitionsSubscription(evaluateTopics(runContext), evaluateSince(runContext));
        }

        if (this.topic != null) {
            return new TopicListSubscription(groupId, evaluateTopics(runContext));
        }

        if (this.topicPattern != null) {
            try {
                return new PatternTopicSubscription(groupId, Pattern.compile(this.topicPattern));
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex for `topicPattern`: " + this.topicPattern);
            }
        }
        throw new IllegalArgumentException("Failed to create KafkaConsumer subscription");
    }

    /**
     * @return the configured `topic` list.
     */
    @SuppressWarnings("unchecked")
    private List<String> evaluateTopics(final RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> topics;
        if (this.topic instanceof String) {
            topics = List.of(runContext.render((String) this.topic));
        } else if (this.topic instanceof List) {
            topics = runContext.render((List<String>) this.topic);
        } else {
            throw new IllegalArgumentException("Invalid topics with type '" + this.topic.getClass().getName() + "'");
        }
        return topics;
    }

    /**
     * @return the configured `since` property.
     */
    @Nullable
    private Long evaluateSince(final RunContext runContext) throws IllegalVariableEvaluationException {
        return Optional.ofNullable(this.since)
            .map(Rethrow.throwFunction(runContext::render))
            .map(ZonedDateTime::parse)
            .map(ChronoZonedDateTime::toInstant)
            .map(Instant::toEpochMilli)
            .orElse(null);
    }

    /**
     * Validates the task's configurations.
     */
    @VisibleForTesting
    void validateConfiguration() {
        if (this.topic == null && this.topicPattern == null) {
            throw new IllegalArgumentException(
                "Invalid Configuration: You must configure one of the following two settings: `topic` or `topicPattern`."
            );
        }

        if (this.topic != null && this.topicPattern != null) {
            throw new IllegalArgumentException(
                "Invalid Configuration: Both `topic` and `topicPattern` was configured. You must configure only one of the following two settings: `topic` or `topicPattern`."
            );
        }

        if (this.topicPattern != null && this.groupId == null) {
            throw new IllegalArgumentException(
                "Invalid Configuration: `groupId` can not be null when `topicPattern` is configured."
            );
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

    /**
     * Interface to wrap a {@link Consumer} subscription.
     */
    @VisibleForTesting
    interface ConsumerSubscription {

        void subscribe(Consumer<Object, Object> consumer, ConsumeInterface consumeInterface);

        default void waitForSubscription(final Consumer<Object, Object> consumer,
                                         final ConsumeInterface consumeInterface) {
            var timeout = consumeInterface.getMaxDuration() != null ?
                consumeInterface.getMaxDuration() :
                consumeInterface.getPollDuration();
            // Wait for the subscription to happen, this avoids possible no result for the first poll due to the poll timeout
            Await.until(() -> !consumer.subscription().isEmpty(), timeout);
        }
    }

    /**
     * A pattern subscription.
     */
    @VisibleForTesting
    record PatternTopicSubscription(String group, Pattern pattern) implements ConsumerSubscription {
        @Override
        public void subscribe(final Consumer<Object, Object> consumer,
                              final ConsumeInterface consumeInterface) {
            consumer.subscribe(pattern);
        }

        @Override
        public String toString() {
            return "[pattern=" + pattern + ", group=" + group + "]";
        }
    }

    /**
     * A topic list subscription.
     */
    @VisibleForTesting
    record TopicListSubscription(String group, List<String> topics) implements ConsumerSubscription {

        @Override
        public void subscribe(final Consumer<Object, Object> consumer, final ConsumeInterface consumeInterface) {
            consumer.subscribe(topics);
            waitForSubscription(consumer, consumeInterface);
        }

        @Override
        public String toString() {
            return "[topics=" + topics + ", group=" + group + "]";
        }
    }

    /**
     * A topic-partitions subscription.
     */
    @VisibleForTesting
    record TopicPartitionsSubscription(List<String> topics,
                                               Long fromTimestamp) implements ConsumerSubscription {

        @Override
        public void subscribe(final Consumer<Object, Object> consumer, final ConsumeInterface consumeInterface) {
            List<TopicPartition> topicPartitions = allPartitionsForTopics(consumer, topics);
            consumer.assign(topicPartitions);
            if (this.fromTimestamp == null) {
                consumer.seekToBeginning(topicPartitions);
                return;
            }

            Map<TopicPartition, Long> topicPartitionsTimestamp = topicPartitions
                .stream()
                .collect(Collectors.toMap(Function.identity(), tp -> fromTimestamp));

            consumer
                .offsetsForTimes(topicPartitionsTimestamp)
                .forEach((tp, offsetAndTimestamp) -> {
                    consumer.seek(tp, offsetAndTimestamp.timestamp());
                });
        }

        private List<TopicPartition> allPartitionsForTopics(final Consumer<?, ?> consumer, final List<String> topics) {
            return topics
                .stream()
                .flatMap(s -> consumer.partitionsFor(s).stream())
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
        }

        @Override
        public String toString() {
            return "[topics=" + topics + "]";
        }
    }
}

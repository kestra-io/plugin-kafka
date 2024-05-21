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
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

    private List<Integer> partitions;

    private String groupId;

    @Builder.Default
    private SerdeType keyDeserializer = SerdeType.STRING;

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
    public KafkaConsumer<Object, Object> consumer(RunContext runContext) throws Exception {
        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        final Properties consumerProps = createProperties(this.properties, runContext);

        if (this.groupId != null) {
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, runContext.render(groupId));
        } else if (consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            // groupId can be passed from properties
            this.groupId = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        }

        if (!consumerProps.contains(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            // by default, we disable auto-commit
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }

        if (!consumerProps.contains(ConsumerConfig.ISOLATION_LEVEL_CONFIG)) {
            // by default, we only read committed offsets in case of transactions
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        }

        final Properties serdesProperties = createProperties(this.serdeProperties, runContext);

        // by default, enable Avro LogicalType
        serdesProperties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        final Deserializer keyDeserializer = getTypedDeserializer(this.keyDeserializer);
        final Deserializer valDeserializer = getTypedDeserializer(this.valueDeserializer);

        keyDeserializer.configure(serdesProperties, true);
        valDeserializer.configure(serdesProperties, false);

        return new KafkaConsumer<Object, Object>(consumerProps, keyDeserializer, valDeserializer);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        File tempFile = runContext.tempFile(".ion").toFile();
        try (
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));
            KafkaConsumer<Object, Object> consumer = this.consumer(runContext)
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
                    FileSerde.write(output, this.recordToMessage(record));

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
                .uri(runContext.storage().putFile(tempFile))
                .build();
        }
    }

    public Message recordToMessage(ConsumerRecord<Object, Object> record) {
        return Message.builder()
            .key(record.key())
            .value(record.value())
            .headers(processHeaders(record.headers()))
            .topic(record.topic())
            .partition(record.partition())
            .timestamp(Instant.ofEpochMilli(record.timestamp()))
            .offset(record.offset())
            .build();
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

    public ConsumerSubscription topicSubscription(final RunContext runContext) throws IllegalVariableEvaluationException {
        validateConfiguration();

        if (this.topic != null && (partitions != null && !partitions.isEmpty())) {
            List<TopicPartition> topicPartitions = getTopicPartitions(runContext);
            return TopicPartitionsSubscription.forTopicPartitions(groupId, topicPartitions, evaluateSince(runContext));
        }

        if (this.topic != null && groupId == null) {
            return TopicPartitionsSubscription.forTopics(null, evaluateTopics(runContext), evaluateSince(runContext));
        }

        if (this.topic != null) {
            return new TopicListSubscription(groupId, evaluateTopics(runContext));
        }

        if (this.topicPattern != null) {
            try {
                return new TopicPatternSubscription(groupId, Pattern.compile(this.topicPattern));
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex for `topicPattern`: " + this.topicPattern);
            }
        }
        throw new IllegalArgumentException("Failed to create KafkaConsumer subscription");
    }

    private List<TopicPartition> getTopicPartitions(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> topics = evaluateTopics(runContext);
        return topics.stream()
            .flatMap(topic -> partitions.stream().map(partition -> new TopicPartition(topic, partition)))
            .toList();
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
                "Invalid Configuration: `groupId` cannot be null when `topicPattern` is configured."
            );
        }
    }

    @VisibleForTesting
    static List<Pair<String, String>> processHeaders(final Headers headers) {
        return StreamSupport
            .stream(headers.spliterator(), false)
            .map(header -> Pair.of(header.key(), new String(header.value(), StandardCharsets.UTF_8)))
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
     * A topic pattern subscription.
     */
    @VisibleForTesting
    record TopicPatternSubscription(String groupId, Pattern pattern) implements ConsumerSubscription {
        @Override
        public void subscribe(final Consumer<Object, Object> consumer,
                              final ConsumeInterface consumeInterface) {
            consumer.subscribe(pattern);
        }

        @Override
        public String toString() {
            return "[Subscription pattern=" + pattern + ", groupId=" + groupId + "]";
        }
    }

    /**
     * A topic list subscription.
     */
    @VisibleForTesting
    record TopicListSubscription(String groupId, List<String> topics) implements ConsumerSubscription {

        @Override
        public void subscribe(final Consumer<Object, Object> consumer, final ConsumeInterface consumeInterface) {
            consumer.subscribe(topics);
            waitForSubscription(consumer, consumeInterface);
        }

        @Override
        public String toString() {
            return "[Subscription topics=" + topics + ", groupId=" + groupId + "]";
        }
    }

    /**
     * A topic-partitions subscription.
     */
    @VisibleForTesting
    static final class TopicPartitionsSubscription implements ConsumerSubscription {

        private final String groupId;
        private final List<String> topics;
        private final Long fromTimestamp;
        private List<TopicPartition> topicPartitions;

        public static TopicPartitionsSubscription forTopicPartitions(final String groupId,
                                                                     final List<TopicPartition> topicPartitions,
                                                                     final Long fromTimestamp) {
            return new TopicPartitionsSubscription(
                groupId,
                topicPartitions,
                topicPartitions.stream().map(TopicPartition::topic).toList(),
                fromTimestamp
            );
        }

        public static TopicPartitionsSubscription forTopics(final String groupId,
                                                            final List<String> topics,
                                                            final Long fromTimestamp) {
            return new TopicPartitionsSubscription(groupId, null, topics, fromTimestamp);
        }

        TopicPartitionsSubscription(final String groupId,
                                    final List<TopicPartition> topicPartitions,
                                    final List<String> topics,
                                    Long fromTimestamp) {
            this.groupId = groupId;
            this.topicPartitions = topicPartitions;
            this.topics = topics;
            this.fromTimestamp = fromTimestamp;
        }

        @Override
        public void subscribe(final Consumer<Object, Object> consumer, final ConsumeInterface consumeInterface) {
            if (this.topicPartitions == null) {
                this.topicPartitions = allPartitionsForTopics(consumer, topics);
            }

            consumer.assign(this.topicPartitions);
            if (this.fromTimestamp == null) {
                consumer.seekToBeginning(this.topicPartitions);
                return;
            }

            Map<TopicPartition, Long> topicPartitionsTimestamp = this.topicPartitions
                .stream()
                .collect(Collectors.toMap(Function.identity(), tp -> fromTimestamp));

            consumer
                .offsetsForTimes(topicPartitionsTimestamp)
                .forEach((tp, offsetAndTimestamp) -> {
                    consumer.seek(tp, offsetAndTimestamp.timestamp());
                });
        }

        @VisibleForTesting
        List<String> topics() {
            return topics;
        }

        @VisibleForTesting
        Long fromTimestamp() {
            return fromTimestamp;
        }

        @VisibleForTesting
        List<TopicPartition> topicPartitions() {
            return topicPartitions;
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
            return "[Subscription topics=" + topics + ", groupId=" + groupId + "]";
        }
    }
}

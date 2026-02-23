package io.kestra.plugin.kafka;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Await;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Read Kafka records into internal storage",
    description = """
        Consumes from configured topics or regex with manual offset commits (auto-commit disabled) and committed-only reads by default.
        Writes all fetched records to Kestra internal storage as ION at `uri` and returns the count.
        Defaults: pollDuration PT5S, STRING deserializers, Avro logical type converters enabled.
        Use `groupType: CONSUMER` (default, backward compatible) for classic consumer groups, or `groupType: SHARE` for queue semantics with share groups and explicit acknowledgements.
        In `SHARE` mode, `topic` and `groupId` are required, and `topicPattern`, `partitions`, and `since` are not supported.
        Use `since`, `maxRecords`, `maxDuration`, or header filters to stop early.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Consume data from a Kafka topic",
            code = """
                id: kafka_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.kafka.Consume
                    topic: test_kestra
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                    keyDeserializer: STRING
                    valueDeserializer: AVRO
                """
        ),
        @Example(
            full = true,
            title = "Connect to a Kafka cluster with SSL.",
            code = """
                id: kafka_consume
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.kafka.Consume
                    properties:
                      security.protocol: SSL
                      bootstrap.servers: localhost:19092
                      ssl.key.password: my-ssl-password
                      ssl.keystore.type: PKCS12
                      ssl.keystore.location: my-base64-encoded-keystore
                      ssl.keystore.password: my-ssl-password
                      ssl.truststore.location: my-base64-encoded-truststore
                      ssl.truststore.password: my-ssl-password
                    topic:
                      - kestra_workerinstance
                    keyDeserializer: STRING
                    valueDeserializer: STRING
                """
        ),
        @Example(
            full = true,
            title = "Consume data from a Kafka topic and write it to a JSON file",
            code = """
                id: consume_kafka_messages
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.kafka.Consume
                    topic: topic_test
                    properties:
                      bootstrap.servers: localhost:9093
                      auto.offset.reset: earliest
                    pollDuration: PT20S
                    maxRecords: 50
                    keyDeserializer: STRING
                    valueDeserializer: JSON

                  - id: write_json
                    type: io.kestra.plugin.serdes.json.IonToJson
                    newLine: true
                    from: "{{ outputs.consume.uri }}"
                """
        ),
        @Example(
            full = true,
            title = "Consume only records whose headers match",
            code = """
                id: consume_with_headers
                namespace: company.team

                tasks:
                  - id: consume_filtered
                    type: io.kestra.plugin.kafka.Consume
                    topic: orders
                    properties:
                      bootstrap.servers: localhost:9092
                      auto.offset.reset: earliest
                    keyDeserializer: STRING
                    valueDeserializer: JSON
                    headerFilters:
                      event-type: order_created
                      region: us-east
                """
        ),
        @Example(
            full = true,
            title = "Consume queue-style with Kafka share groups",
            code = """
                id: consume_kafka_share_group
                namespace: company.team

                tasks:
                  - id: consume_queue
                    type: io.kestra.plugin.kafka.Consume
                    topic: orders
                    properties:
                      bootstrap.servers: localhost:9092
                    groupId: orders-share-group
                    groupType: SHARE
                    acknowledgeType: ACCEPT
                    keyDeserializer: STRING
                    valueDeserializer: JSON
                """
        )
    },
    metrics = {
        @Metric(
            name = "records",
            type = Counter.TYPE,
            unit = "records",
            description = "Number of records consumed from Kafka topic."
        )
    }
)
public class Consume extends AbstractKafkaConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
    private Object topic;

    private Property<String> topicPattern;

    private Property<List<Integer>> partitions;

    private Property<String> groupId;

    @Schema(
        title = "Group protocol to consume with",
        description = """
            `CONSUMER` (default) keeps backward-compatible Kafka consumer-group behavior.
            `SHARE` enables Kafka share-group queue semantics with explicit acknowledgements.
            In `SHARE` mode, `groupId` and `topic` are required, and `topicPattern`, `partitions`, and `since` are not supported.
            """
    )
    @Builder.Default
    private Property<GroupType> groupType = Property.ofValue(GroupType.CONSUMER);

    @Schema(
        title = "Acknowledgement action for SHARE group type",
        description = """
            Used only when `groupType` is `SHARE`.
            `ACCEPT` (default) marks a record as processed, `RELEASE` returns it to the queue, `REJECT` negatively acknowledges it,
            and `RENEW` extends the acquisition lock timeout for the current delivery attempt without changing record state.
            Ignored when `groupType` is `CONSUMER`.
            """
    )
    @Builder.Default
    private Property<QueueAcknowledgeType> acknowledgeType = Property.ofValue(QueueAcknowledgeType.ACCEPT);

    @Builder.Default
    private Property<SerdeType> keyDeserializer = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<SerdeType> valueDeserializer = Property.ofValue(SerdeType.STRING);

    private OnSerdeError onSerdeError;

    private Property<String> since;

    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(5));

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Getter(AccessLevel.PACKAGE)
    private ConsumerSubscription subscription;

    @Schema(
        title = "Filter messages by Kafka headers",
        description = "Consume records only when all header key/value pairs match exactly (last header wins, UTF-8 comparison)"
    )
    private Property<Map<String, String>> headerFilters;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public KafkaConsumer<Object, Object> consumer(RunContext runContext) throws Exception {
        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        final Properties consumerProps = createProperties(this.properties, runContext);
        final Optional<String> renderedGroupId = runContext.render(groupId).as(String.class);
        if (renderedGroupId.isPresent()) {
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, renderedGroupId.get());
        } else if (consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            // groupId can be passed from properties
            this.groupId = Property.ofValue(consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
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

        final Deserializer keyDeserializer = getTypedDeserializer(runContext.render(this.keyDeserializer).as(SerdeType.class).orElse(SerdeType.STRING));
        final Deserializer valDeserializer = getTypedDeserializer(runContext.render(this.valueDeserializer).as(SerdeType.class).orElse(SerdeType.STRING));

        keyDeserializer.configure(serdesProperties, true);
        valDeserializer.configure(serdesProperties, false);

        return new KafkaConsumer<Object, Object>(consumerProps, keyDeserializer, valDeserializer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ShareConsumer<Object, Object> shareConsumer(RunContext runContext) throws Exception {
        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        final var consumerProps = createProperties(this.properties, runContext);
        final var rGroupId = runContext.render(groupId).as(String.class);
        rGroupId.ifPresent(value -> consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, value));

        if (!consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalArgumentException(
                "Invalid Configuration: You must configure `groupId` or set `group.id` in `properties` when `groupType` is `SHARE`."
            );
        }

        if (!consumerProps.containsKey(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG)) {
            consumerProps.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        }

        final var serdesProperties = createProperties(this.serdeProperties, runContext);
        serdesProperties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        final Deserializer keyDeserializer = getTypedDeserializer(runContext.render(this.keyDeserializer).as(SerdeType.class).orElse(SerdeType.STRING));
        final Deserializer valDeserializer = getTypedDeserializer(runContext.render(this.valueDeserializer).as(SerdeType.class).orElse(SerdeType.STRING));

        keyDeserializer.configure(serdesProperties, true);
        valDeserializer.configure(serdesProperties, false);

        return new KafkaShareConsumer<Object, Object>(consumerProps, keyDeserializer, valDeserializer);
    }

    GroupType resolveGroupType(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.groupType).as(GroupType.class).orElse(GroupType.CONSUMER);
    }

    QueueAcknowledgeType resolveAcknowledgeType(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.acknowledgeType).as(QueueAcknowledgeType.class).orElse(QueueAcknowledgeType.ACCEPT);
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try {
            if (resolveGroupType(runContext) == GroupType.SHARE) {
                return runWithShareConsumer(runContext, tempFile);
            }
            return runWithConsumer(runContext, tempFile);
        } catch (KafkaException e) {
            return handleSerdeError(runContext, tempFile, e);
        }
    }

    private Output runWithConsumer(RunContext runContext, File tempFile) throws Exception {
        try (
            var output = new BufferedOutputStream(new FileOutputStream(tempFile));
            var consumer = this.consumer(runContext)
        ) {
            this.subscription = topicSubscription(runContext);
            this.subscription.subscribe(runContext, consumer, this);

            var count = new HashMap<String, Integer>();
            var total = new AtomicInteger();
            var started = ZonedDateTime.now();
            ConsumerRecords<Object, Object> records;
            boolean empty;

            do {
                records = consumer.poll(runContext.render(this.pollDuration).as(Duration.class).orElse(Duration.ofSeconds(5)));
                empty = records.isEmpty();
                total.addAndGet(processConsumerRecords(runContext, records, consumerRecord -> {
                    FileSerde.write(output, this.recordToMessage(consumerRecord));
                    count.compute(consumerRecord.topic(), (s, integer) -> integer == null ? 1 : integer + 1);
                }));
            }
            while (!this.ended(runContext, empty, total, started));

            output.flush();
            var uri = runContext.storage().putFile(tempFile);

            if (this.groupId != null) {
                // Important - always commit the consumer offsets after
                // records are fully written to Kestra's internal storage
                consumer.commitSync();
            }

            count.forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));

            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(uri)
                .build();
        }
    }

    private Output runWithShareConsumer(RunContext runContext, File tempFile) throws Exception {
        validateShareConfiguration();
        try (
            var output = new BufferedOutputStream(new FileOutputStream(tempFile));
            var consumer = this.shareConsumer(runContext)
        ) {
            shareSubscribe(runContext, consumer);

            var count = new HashMap<String, Integer>();
            var total = new AtomicInteger();
            var started = ZonedDateTime.now();
            ConsumerRecords<Object, Object> records;
            boolean empty;

            do {
                records = consumer.poll(runContext.render(this.pollDuration).as(Duration.class).orElse(Duration.ofSeconds(5)));
                empty = records.isEmpty();
                total.addAndGet(processShareConsumerRecords(runContext, consumer, records, consumerRecord -> {
                    FileSerde.write(output, this.recordToMessage(consumerRecord));
                    count.compute(consumerRecord.topic(), (s, integer) -> integer == null ? 1 : integer + 1);
                }));
            }
            while (!this.ended(runContext, empty, total, started));

            output.flush();
            var uri = runContext.storage().putFile(tempFile);
            // Important - always commit the consumer offsets after
            // records are fully written to Kestra's internal storage
            consumer.commitSync();

            count.forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));

            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(uri)
                .build();
        }
    }

    private Output handleSerdeError(RunContext runContext, File tempFile, KafkaException e) throws Exception {
        if (onSerdeError == null || !(e.getCause() instanceof PluginKafkaSerdeException err)) {
            throw e;
        }

        var onSerdeErrorBehavior = runContext.render(this.onSerdeError.getType()).as(OnSerdeErrorBehavior.class).orElseThrow();
        runContext.logger().warn("Skipping consume on serde error: '{}'", e.getMessage());

        return switch (onSerdeErrorBehavior) {
            case SKIPPED -> Output.builder().build();
            case STORE -> {
                Files.writeString(tempFile.toPath(), err.getData(), StandardCharsets.UTF_8);
                yield Output.builder()
                    .uri(runContext.storage().putFile(tempFile))
                    .build();
            }
            case DLQ -> {
                Produce.builder()
                    .topic(onSerdeError.getTopic())
                    .properties(this.getProperties())
                    .serdeProperties(this.getSerdeProperties())
                    .keySerializer(this.getKeyDeserializer())
                    .valueSerializer(this.getValueDeserializer())
                    .from(err.getData())
                    .build()
                    .run(runContext);
                yield Output.builder().build();
            }
        };
    }

    int processConsumerRecords(RunContext runContext,
                               ConsumerRecords<Object, Object> records,
                               RecordHandler onMatchingRecord) throws Exception {
        var rHeaderFilters = runContext.render(headerFilters).asMap(String.class, String.class);
        var matchedCount = 0;

        for (var consumerRecord : records) {
            if (!matchHeaders(consumerRecord.headers(), rHeaderFilters)) {
                continue;
            }

            onMatchingRecord.accept(consumerRecord);
            matchedCount++;
        }

        return matchedCount;
    }

    int processShareConsumerRecords(RunContext runContext,
                                    ShareConsumer<Object, Object> consumer,
                                    ConsumerRecords<Object, Object> records,
                                    RecordHandler onMatchingRecord) throws Exception {
        var rHeaderFilters = runContext.render(headerFilters).asMap(String.class, String.class);
        var rAcknowledgeType = resolveAcknowledgeType(runContext).toKafkaType();
        var matchedCount = 0;

        for (var consumerRecord : records) {
            if (!matchHeaders(consumerRecord.headers(), rHeaderFilters)) {
                consumer.acknowledge(consumerRecord, AcknowledgeType.ACCEPT);
                continue;
            }

            onMatchingRecord.accept(consumerRecord);
            consumer.acknowledge(consumerRecord, rAcknowledgeType);
            matchedCount++;
        }

        return matchedCount;
    }

    @FunctionalInterface
    interface RecordHandler {
        void accept(ConsumerRecord<Object, Object> consumerRecord) throws Exception;
    }

    public Message recordToMessage(ConsumerRecord<Object, Object> consumerRecord) {
        return Message.builder()
            .key(consumerRecord.key())
            .value(consumerRecord.value())
            .headers(processHeaders(consumerRecord.headers()))
            .topic(consumerRecord.topic())
            .partition(consumerRecord.partition())
            .timestamp(Instant.ofEpochMilli(consumerRecord.timestamp()))
            .offset(consumerRecord.offset())
            .build();
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean ended(RunContext runContext, Boolean empty, AtomicInteger count, ZonedDateTime start) throws IllegalVariableEvaluationException {
        if (Boolean.TRUE.equals(empty)) {
            return true;
        }

        final Optional<Integer> renderedMaxRecords = runContext.render(this.maxRecords).as(Integer.class);
        if (renderedMaxRecords.isPresent() && count.get() > renderedMaxRecords.get()) {
            return true;
        }

        final Optional<Duration> renderedPollDuration = runContext.render(this.pollDuration).as(Duration.class);
        if (renderedPollDuration.isPresent() && ZonedDateTime.now().toEpochSecond() > start.plus(renderedPollDuration.get()).toEpochSecond()) {
            return true;
        }

        return false;
    }

     boolean matchHeaders(Headers headers, Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, String> entry : filters.entrySet()) {
            var header = headers.lastHeader(entry.getKey());
            if (header == null) {
                return false;
            }
            String value = new String(header.value(), StandardCharsets.UTF_8);
            if (!value.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public void shareSubscribe(RunContext runContext, ShareConsumer<Object, Object> consumer) throws IllegalVariableEvaluationException {
        consumer.subscribe(evaluateTopics(runContext));
    }

    @VisibleForTesting
    void validateShareConfiguration() {
        if (this.topic == null) {
            throw new IllegalArgumentException("Invalid Configuration: You must configure `topic` when `groupType` is `SHARE`.");
        }

        if (this.topicPattern != null) {
            throw new IllegalArgumentException("Invalid Configuration: `topicPattern` is not supported when `groupType` is `SHARE`.");
        }

        if (this.partitions != null) {
            throw new IllegalArgumentException("Invalid Configuration: `partitions` is not supported when `groupType` is `SHARE`.");
        }

        if (this.since != null) {
            throw new IllegalArgumentException("Invalid Configuration: `since` is not supported when `groupType` is `SHARE`.");
        }
    }


    public ConsumerSubscription topicSubscription(final RunContext runContext) throws IllegalVariableEvaluationException {
        validateConfiguration();

        final Optional<String> renderedGroupId = runContext.render(groupId).as(String.class);
        final List<Integer> renderedPartitions = runContext.render(partitions).asList(String.class);

        if (this.topic != null && !renderedPartitions.isEmpty()) {
            List<TopicPartition> topicPartitions = getTopicPartitions(runContext);
            return TopicPartitionsSubscription.forTopicPartitions(renderedGroupId.orElse(null), topicPartitions, evaluateSince(runContext));
        }

        if (this.topic != null && renderedGroupId.isEmpty()) {
            return TopicPartitionsSubscription.forTopics(null, evaluateTopics(runContext), evaluateSince(runContext));
        }

        if (this.topic != null) {
            return new TopicListSubscription(renderedGroupId.get(), evaluateTopics(runContext));
        }

        final Optional<String> renderedPattern = runContext.render(topicPattern).as(String.class);
        if (renderedPattern.isPresent()) {
            try {
                return new TopicPatternSubscription(renderedGroupId.orElse(null), Pattern.compile(renderedPattern.get()));
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex for `topicPattern`: " + renderedPattern.get());
            }
        }
        throw new IllegalArgumentException("Failed to create KafkaConsumer subscription");
    }

    private List<TopicPartition> getTopicPartitions(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> topics = evaluateTopics(runContext);
        final List<Integer> renderedPartitions = runContext.render(partitions).asList(String.class);
        return topics.stream()
            .flatMap(topic -> renderedPartitions.stream().map(partition -> new TopicPartition(topic, partition)))
            .toList();
    }

    /**
     * @return the configured `topic` list.
     */
    @SuppressWarnings("unchecked")
    private List<String> evaluateTopics(final RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> topics;
        if (this.topic instanceof String topicString) {
            topics = List.of(runContext.render(topicString));
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
        return runContext.render(this.since).as(String.class)
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
            .toList();
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

        void subscribe(RunContext runContext, Consumer<Object, Object> consumer, ConsumeInterface consumeInterface) throws IllegalVariableEvaluationException;

        default void waitForSubscription(RunContext runContext,
                                         final Consumer<Object, Object> consumer,
                                         final ConsumeInterface consumeInterface) throws IllegalVariableEvaluationException {
            var timeout = consumeInterface.getMaxDuration() != null ?
                runContext.render(consumeInterface.getMaxDuration()).as(Duration.class).orElse(null) :
                runContext.render(consumeInterface.getPollDuration()).as(Duration.class).orElse(null);
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
        public void subscribe(RunContext runContext, final Consumer<Object, Object> consumer,
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
        public void subscribe(RunContext runContext, final Consumer<Object, Object> consumer, final ConsumeInterface consumeInterface) throws IllegalVariableEvaluationException {
            consumer.subscribe(topics);
            waitForSubscription(runContext, consumer, consumeInterface);
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
        public void subscribe(RunContext runContext, final Consumer<Object, Object> consumer, final ConsumeInterface consumeInterface) {
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
                .forEach((tp, offsetAndTimestamp) ->
                    consumer.seek(tp, offsetAndTimestamp.timestamp())
                );
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

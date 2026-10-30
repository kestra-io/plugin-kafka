package io.kestra.plugin.kafka;


import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class ConsumeTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    @Test
    void shouldThrowIllegalGivenNoTopicAnNoPattern() {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume.builder().build();
        // When/Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldThrowIllegalGivenBothTopicAnNoPattern() {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume
            .builder()
            .topic("topic")
            .topicPattern(Property.ofValue(".*"))
            .build();
        // When/Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldGetTopicPartitionSubscriptionGivenPatternAndNoGroupId() throws IllegalVariableEvaluationException {
        // Given
        var runContext = runContextFactory.of(Map.of());
        var task = Consume
            .builder()
            .topicPattern(Property.ofValue("orders-.*"))
            .build();
        var consumer = new MockConsumer<Object, Object>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions("orders-eu", List.of(new PartitionInfo("orders-eu", 0, null, null, null)));
        consumer.updatePartitions("payments", List.of(new PartitionInfo("payments", 0, null, null, null)));

        // When
        var subscription = task.topicSubscription(runContext);
        subscription.subscribe(runContext, consumer, task);

        // Then
        Assertions.assertInstanceOf(Consume.TopicPartitionsSubscription.class, subscription);
        Assertions.assertEquals(List.of(new TopicPartition("orders-eu", 0)), ((Consume.TopicPartitionsSubscription) subscription).topicPartitions());
    }

    @Test
    void shouldAssignNothingGivenPatternMatchingNoTopic() throws IllegalVariableEvaluationException {
        // Given
        var runContext = runContextFactory.of(Map.of());
        var task = Consume
            .builder()
            .topicPattern(Property.ofValue("nomatch-.*"))
            .build();
        var consumer = new MockConsumer<Object, Object>(OffsetResetStrategy.EARLIEST);

        // When
        var subscription = task.topicSubscription(runContext);
        subscription.subscribe(runContext, consumer, task);

        // Then
        Assertions.assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    void shouldHaveNothingToConsumeGivenPatternMatchingNoTopic() throws IllegalVariableEvaluationException {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder().topicPattern(Property.ofValue("nomatch-.*")).build();
        var consumer = new MockConsumer<Object, Object>(OffsetResetStrategy.EARLIEST);

        var subscription = task.topicSubscription(runContext);
        subscription.subscribe(runContext, consumer, task);

        Assertions.assertTrue(subscription.hasNothingToConsume());
    }

    @Test
    void shouldReturnZeroMessagesGivenPatternMatchingNoTopic() throws Exception {
        var task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topicPattern(Property.ofValue("tu_nomatch_" + IdUtils.create() + "_.*"))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();

        var output = task.run(runContextFactory.of(Map.of()));

        assertThat(output.getMessagesCount(), is(0));
    }

    @Test
    void shouldReturnZeroMessagesGivenSinceAfterAllRecords() throws Exception {
        var topic = "tu_since_future_" + IdUtils.create();
        produceRecords(topic, 2);

        for (var usePattern : List.of(false, true)) {
            var builder = Consume.builder()
                .id(IdUtils.create())
                .type(Consume.class.getName())
                .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
                .keyDeserializer(Property.ofValue(SerdeType.STRING))
                .valueDeserializer(Property.ofValue(SerdeType.STRING))
                .since(Property.ofValue(Instant.now().plusSeconds(60).toString()))
                .pollDuration(Property.ofValue(Duration.ofSeconds(2)));
            if (usePattern) {
                builder.topicPattern(Property.ofValue(topic));
            } else {
                builder.topic(topic);
            }

            var output = builder.build().run(runContextFactory.of(Map.of()));

            assertThat("usePattern=" + usePattern, output.getMessagesCount(), is(0));
        }
    }

    @Test
    void shouldPickUpRecordProducedAfterSubscribeGivenSinceAfterAllRecords() throws Exception {
        var topic = "tu_since_new_" + IdUtils.create();
        produceRecords(topic, 2);

        var task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topicPattern(Property.ofValue(topic))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .since(Property.ofValue(Instant.now().plusSeconds(60).toString()))
            .pollDuration(Property.ofValue(Duration.ofSeconds(2)))
            .build();
        var runContext = runContextFactory.of(Map.of());

        try (var consumer = task.consumer(runContext)) {
            task.topicSubscription(runContext).subscribe(runContext, consumer, task);
            assertThat(consumer.poll(Duration.ofSeconds(2)).count(), is(0));

            produceRecords(topic, 1);

            var received = 0;
            var deadline = System.currentTimeMillis() + 15_000;
            while (received == 0 && System.currentTimeMillis() < deadline) {
                received += consumer.poll(Duration.ofSeconds(1)).count();
            }
            assertThat(received, is(1));
        }
    }

    @Test
    void shouldThrowGivenMissingTopicAndNoGroupId() throws IllegalVariableEvaluationException {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder().topic("does-not-exist").build();
        var consumer = new MockConsumer<Object, Object>(OffsetResetStrategy.EARLIEST);

        var subscription = task.topicSubscription(runContext);

        Assertions.assertThrows(IllegalArgumentException.class, () -> subscription.subscribe(runContext, consumer, task));
    }

    @Test
    void shouldThrowGivenInvalidPatternAndNoGroupId() {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder().topicPattern(Property.ofValue("[")).build();
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldConsumeMatchingTopicsGivenPatternAndNoGroupId() throws Exception {
        var prefix = "tu_pattern_" + IdUtils.create();
        var matching1 = prefix + "_a";
        var matching2 = prefix + "_b";
        var other = "tu_other_" + IdUtils.create();
        produceRecords(matching1, 2);
        produceRecords(matching2, 2);
        produceRecords(other, 2);

        var task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topicPattern(Property.ofValue(prefix + "_.*"))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var runContext = runContextFactory.of(Map.of());
        var output = task.run(runContext);

        assertThat(output.getMessagesCount(), is(4));
        String content;
        try (var in = runContext.storage().getFile(output.getUri())) {
            content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertThat(content, Matchers.containsString(matching1));
        assertThat(content, Matchers.containsString(matching2));
        assertThat(content, Matchers.not(Matchers.containsString(other)));

        try (var adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap))) {
            for (var group : adminClient.listConsumerGroups().all().get(15, TimeUnit.SECONDS)) {
                var offsets = adminClient.listConsumerGroupOffsets(group.groupId())
                    .partitionsToOffsetAndMetadata()
                    .get(15, TimeUnit.SECONDS);
                assertThat(
                    "No offsets must be committed when no group is configured",
                    offsets.keySet().stream().noneMatch(tp -> tp.topic().startsWith(prefix)),
                    is(true)
                );
            }
        }
    }

    @Test
    void shouldGetTopicPartitionSubscriptionGivenTopicAndNoGroupId() throws IllegalVariableEvaluationException {
        // Given
        var runContext = runContextFactory.of(Map.of());
        Consume task = Consume
            .builder()
            .topic("topic")
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);
        var consumer = new MockConsumer<Object, Object>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions("topic", List.of(new PartitionInfo("topic", 0, null, null, null)));
        subscription.subscribe(runContext, consumer, task);

        // Then
        Assertions.assertInstanceOf(Consume.TopicPartitionsSubscription.class, subscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicPartitionsSubscription) subscription).topics());
    }

    @Test
    void shouldGetTopicPartitionSubscriptionWithTimestamp() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Instant now = Instant.now();
        Consume task = Consume
            .builder()
            .topic("topic")
            .since(Property.ofValue(now.toString()))
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertInstanceOf(Consume.TopicPartitionsSubscription.class, subscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicPartitionsSubscription) subscription).topics());
        Assertions.assertEquals(now.toEpochMilli(), ((Consume.TopicPartitionsSubscription) subscription).fromTimestamp());
    }

    @Test
    void shouldSeekToEndOffsetSnapshotTakenBeforeOffsetsForTimes() throws IllegalVariableEvaluationException {
        // Given
        var runContext = runContextFactory.of(Map.of());
        var tp = new TopicPartition("topic", 0);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var consumer = new MockConsumer<Object, Object>(OffsetResetStrategy.EARLIEST) {
            @Override
            public Map<TopicPartition, Long> endOffsets(java.util.Collection<TopicPartition> partitions) {
                // a record is appended after the first call
                var end = calls.getAndIncrement() == 0 ? 5L : 6L;
                return partitions.stream().collect(java.util.stream.Collectors.toMap(p -> p, p -> end));
            }

            @Override
            public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
                var result = new HashMap<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp>();
                timestampsToSearch.keySet().forEach(p -> result.put(p, null));
                return result;
            }
        };
        consumer.updatePartitions("topic", List.of(new PartitionInfo("topic", 0, null, null, null)));
        Consume task = Consume.builder()
            .topic("topic")
            .since(Property.ofValue(Instant.now().toString()))
            .build();

        // When
        task.topicSubscription(runContext).subscribe(runContext, consumer, task);

        // Then
        Assertions.assertEquals(5L, consumer.position(tp));
    }

    @Test
    void shouldGetTopicPartitionSubscriptionGivenPartition() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Instant now = Instant.now();
        Consume task = Consume
            .builder()
            .topic("topic")
            .partitions(Property.ofValue(List.of(0)))
            .since(Property.ofValue(now.toString()))
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertInstanceOf(Consume.TopicPartitionsSubscription.class, subscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicPartitionsSubscription) subscription).topics());
        Assertions.assertEquals(List.of(new TopicPartition("topic", 0)), ((Consume.TopicPartitionsSubscription) subscription).topicPartitions());
        Assertions.assertEquals(now.toEpochMilli(), ((Consume.TopicPartitionsSubscription) subscription).fromTimestamp());
    }

    @Test
    void shouldGetTopicListSubscriptionGivenTopicAndGroupId() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume
            .builder()
            .groupId(Property.ofValue("groupId"))
            .topic("topic")
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertDoesNotThrow(() -> subscription.subscribe(runContext, new MockConsumer<>(OffsetResetStrategy.EARLIEST), task));
        Assertions.assertInstanceOf(Consume.TopicListSubscription.class, subscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicListSubscription) subscription).topics());
    }

    @Test
    void shouldGetPatternSubscriptionGivenPattern() throws IllegalVariableEvaluationException {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions("topic", List.of(new PartitionInfo("topic", 0, null, null, null)));

        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume
            .builder()
            .groupId(Property.ofValue("groupId"))
            .topicPattern(Property.ofValue(".*"))
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertDoesNotThrow(() -> subscription.subscribe(runContext, consumer, task));
        Assertions.assertInstanceOf(Consume.TopicPatternSubscription.class, subscription);
        Assertions.assertEquals(".*", ((Consume.TopicPatternSubscription) subscription).pattern().pattern());
    }

    @Test
    void shouldGetRecordHeadersAsPairs() {
        // Given
        List<Pair<String, String>> inputs = List.of(
            Pair.of("test-header-key-1", "test-header-value-1"),
            Pair.of("test-header-key-2", "test-header-value-2")
        );

        Headers headers = new RecordHeaders();
        inputs.forEach(pair -> headers.add(pair.getKey(), pair.getValue().getBytes(StandardCharsets.UTF_8)));

        // When
        List<Pair<String, String>> outputs = Consume.processHeaders(headers);
        // Then
        assertThat(inputs, Matchers.containsInAnyOrder(outputs.toArray()));
    }

    @Test
    void shouldAcceptAllWhenNoHeaderFilters() {
        Consume task = Consume.builder().build();

        Headers headers = new RecordHeaders()
            .add("eventType", "order.created".getBytes(StandardCharsets.UTF_8));

        Assertions.assertTrue(task.matchHeaders(headers, null));
        Assertions.assertTrue(task.matchHeaders(headers, Map.of()));
    }

    @Test
    void shouldAcceptWhenAllHeadersMatch() {
        Consume task = Consume.builder().build();

        Headers headers = new RecordHeaders()
            .add("eventType", "order.created".getBytes(StandardCharsets.UTF_8))
            .add("version", "v1".getBytes(StandardCharsets.UTF_8));

        Map<String, String> filters = Map.of(
            "eventType", "order.created",
            "version", "v1"
        );

        Assertions.assertTrue(task.matchHeaders(headers, filters));
    }

    @Test
    void shouldDeduplicateRecordsByTopicPartitionAndOffset() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume.builder().build();
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        ConsumerRecords<Object, Object> records = new ConsumerRecords<>(Map.of(
            topicPartition,
            List.of(
                new ConsumerRecord<>("topic", 0, 10L, "key-10", "value-10"),
                new ConsumerRecord<>("topic", 0, 10L, "key-10-duplicate", "value-10-duplicate"),
                new ConsumerRecord<>("topic", 0, 11L, "key-11", "value-11")
            )
        ));
        List<ConsumerRecord<Object, Object>> matchedRecords = new ArrayList<>();

        int matchedCount = task.processConsumerRecords(
            runContext,
            records,
            true,
            new HashMap<>(),
            matchedRecords::add,
            null
        );

        Assertions.assertEquals(2, matchedCount);
        Assertions.assertEquals(List.of(10L, 11L), matchedRecords.stream().map(ConsumerRecord::offset).toList());
    }

    @Test
    void shouldKeepSameOffsetFromDifferentPartitionsWhenDeduplicating() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume.builder().build();
        ConsumerRecords<Object, Object> records = new ConsumerRecords<>(Map.of(
            new TopicPartition("topic", 0), List.of(new ConsumerRecord<>("topic", 0, 10L, "key-0", "value-0")),
            new TopicPartition("topic", 1), List.of(new ConsumerRecord<>("topic", 1, 10L, "key-1", "value-1"))
        ));
        List<ConsumerRecord<Object, Object>> matchedRecords = new ArrayList<>();

        int matchedCount = task.processConsumerRecords(
            runContext,
            records,
            true,
            new HashMap<>(),
            matchedRecords::add,
            null
        );

        Assertions.assertEquals(2, matchedCount);
        assertThat(
            matchedRecords.stream().map(ConsumerRecord::partition).toList(),
            Matchers.containsInAnyOrder(0, 1)
        );
    }

    @Test
    void shouldRejectWhenHeaderIsMissing() {
        Consume task = Consume.builder().build();

        Headers headers = new RecordHeaders()
            .add("eventType", "order.created".getBytes(StandardCharsets.UTF_8));

        Map<String, String> filters = Map.of(
            "eventType", "order.created",
            "version", "v1"
        );

        Assertions.assertFalse(task.matchHeaders(headers, filters));
    }

    @Test
    void shouldThrowGivenShareGroupTypeWithTopicPattern() {
        Consume task = Consume.builder()
            .groupType(Property.ofValue(GroupType.SHARE))
            .topicPattern(Property.ofValue(".*"))
            .build();

        Assertions.assertThrows(IllegalArgumentException.class, task::validateShareConfiguration);
    }

    @Test
    void shouldThrowGivenShareGroupTypeWithPartitions() {
        Consume task = Consume.builder()
            .groupType(Property.ofValue(GroupType.SHARE))
            .topic("orders")
            .partitions(Property.ofValue(List.of(0)))
            .build();

        Assertions.assertThrows(IllegalArgumentException.class, task::validateShareConfiguration);
    }

    @Test
    void shouldMapShareAcknowledgeType() {
        Assertions.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.ACCEPT, QueueAcknowledgeType.ACCEPT.toKafkaType());
        Assertions.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.RELEASE, QueueAcknowledgeType.RELEASE.toKafkaType());
        Assertions.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.REJECT, QueueAcknowledgeType.REJECT.toKafkaType());
    }

    @Test
    void shouldMapOrFailGracefullyGivenRenewAcknowledgeType() {
        var kafkaAcknowledgeTypeNames = Arrays.stream(org.apache.kafka.clients.consumer.AcknowledgeType.values())
            .map(Enum::name)
            .toList();

        if (kafkaAcknowledgeTypeNames.contains("RENEW")) {
            Assertions.assertEquals(org.apache.kafka.clients.consumer.AcknowledgeType.valueOf("RENEW"), QueueAcknowledgeType.RENEW.toKafkaType());
            return;
        }

        var exception = Assertions.assertThrows(IllegalStateException.class, () -> QueueAcknowledgeType.RENEW.toKafkaType());
        Assertions.assertTrue(exception.getMessage().contains("not supported"));
    }

    @Test
    void shouldEndWhenMaxDurationExceeded() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var startInPast = ZonedDateTime.now().minusSeconds(10);
        Assertions.assertTrue(task.ended(runContext, false, new AtomicInteger(1), startInPast));
    }

    @Test
    void shouldNotEndWhenMaxDurationNotExceeded() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .maxDuration(Property.ofValue(Duration.ofSeconds(60)))
            .build();

        // 5 seconds elapsed: pollDuration (1s) has passed, but maxDuration (60s) has not
        var start = ZonedDateTime.now().minusSeconds(5);
        Assertions.assertFalse(task.ended(runContext, false, new AtomicInteger(1), start));
    }

    @Test
    void shouldRenderDynamicMaxDurationInEnded() throws Exception {
        var runContext = runContextFactory.of(Map.of("maxDuration", "PT10S"));
        var task = Consume.builder()
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .maxDuration(Property.ofExpression("{{ maxDuration }}"))
            .build();

        var startExceeded = ZonedDateTime.now().minusSeconds(15);
        Assertions.assertTrue(task.ended(runContext, false, new AtomicInteger(1), startExceeded));

        var startNotExceeded = ZonedDateTime.now().minusSeconds(2);
        Assertions.assertFalse(task.ended(runContext, false, new AtomicInteger(1), startNotExceeded));
    }

    @Test
    void shouldEndWhenPollDurationExceededAndMaxDurationNotSet() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .pollDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var startInPast = ZonedDateTime.now().minusSeconds(10);
        Assertions.assertTrue(task.ended(runContext, false, new AtomicInteger(1), startInPast));
    }

    @Test
    void shouldNotEndWhenPollDurationNotExceededAndMaxDurationNotSet() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .pollDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var start = ZonedDateTime.now();
        Assertions.assertFalse(task.ended(runContext, false, new AtomicInteger(1), start));
    }

    @Test
    void shouldEndWhenEmptyRecords() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder().build();

        Assertions.assertTrue(task.ended(runContext, true, new AtomicInteger(0), ZonedDateTime.now()));
    }

    @Test
    void shouldEndWhenMaxRecordsExceeded() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .maxRecords(Property.ofValue(5))
            .build();

        Assertions.assertTrue(task.ended(runContext, false, new AtomicInteger(6), ZonedDateTime.now()));
        Assertions.assertFalse(task.ended(runContext, false, new AtomicInteger(5), ZonedDateTime.now()));
    }

    @Test
    void shouldEndWhenTaskInactive() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder().build();
        task.stop();

        Assertions.assertTrue(task.ended(runContext, false, new AtomicInteger(0), ZonedDateTime.now()));
    }

    @Test
    void shouldKeepConsumingPastPollDurationWhenMaxDurationIsLonger() throws Exception {
        var topic = "tu_max_dur_" + IdUtils.create();
        var groupId = "tu_max_dur_group_" + IdUtils.create();
        var pollDuration = Duration.ofSeconds(5);
        var maxDuration = Duration.ofSeconds(12);

        // Produce initial record so topic exists and consumer can read immediately
        produceRecords(topic, 1);

        var producing = new AtomicBoolean(true);
        var producerStarted = new CountDownLatch(1);
        var producedDuringPollDuration = new AtomicInteger(1);
        var producedAfterPollDuration = new AtomicInteger(0);
        var taskStartTime = new AtomicReference<Instant>();

        var producerThread = new Thread(() -> {
            producerStarted.countDown();
            try {
                while (producing.get()) {
                    Thread.sleep(300);
                    produceRecords(topic, 1);
                    var start = taskStartTime.get();
                    if (start == null || Duration.between(start, Instant.now()).toMillis() < pollDuration.toMillis()) {
                        producedDuringPollDuration.incrementAndGet();
                    } else {
                        producedAfterPollDuration.incrementAndGet();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception ignored) {
            }
        });
        producerThread.start();
        producerStarted.await(5, TimeUnit.SECONDS);

        var task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of(
                "bootstrap.servers", this.bootstrap,
                "auto.offset.reset", "earliest"
            )))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(pollDuration))
            .maxDuration(Property.ofValue(maxDuration))
            .build();

        var runContext = runContextFactory.of(Map.of());
        var start = Instant.now();
        taskStartTime.set(start);
        try {
            var output = task.run(runContext);
            var elapsed = Duration.between(start, Instant.now()).toMillis();

            assertThat("Run must last longer than pollDuration", elapsed, greaterThan(pollDuration.toMillis()));
            assertThat("Records must have been produced after pollDuration", producedAfterPollDuration.get(), greaterThan(0));
            assertThat(
                "Messages count must include records produced after pollDuration",
                output.getMessagesCount(),
                greaterThan(producedDuringPollDuration.get())
            );
        } finally {
            producing.set(false);
            producerThread.interrupt();
            producerThread.join(5000);
        }
    }

    @Test
    void shouldTerminatePromptlyOnKill() throws Exception {
        var topic = "tu_kill_" + IdUtils.create();
        var groupId = "tu_kill_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        RunContext runContext = runContextFactory.of(Map.of());
        Thread runner = new Thread(() -> {
            try {
                task.run(runContext);
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        // Give the consumer time to join the group and reach the blocking poll() before producing.
        Thread.sleep(Duration.ofSeconds(3).toMillis());
        produceRecords(topic, 5);
        // Let the first (non-blocking) poll pick up the records and write them to internal storage
        // before the second poll blocks waiting for more, which is the call kill() must interrupt.
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        long killStart = System.currentTimeMillis();
        task.kill();
        long killElapsedMs = System.currentTimeMillis() - killStart;

        assertThat("kill() must not block for the full pollDuration", killElapsedMs, lessThan(15000L));
        assertThat("Consume task must terminate after kill()", completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A killed run() must not be reported as a clean success", thrown.get(), notNullValue());
    }

    @Test
    void shouldTerminatePromptlyOnKillIssuedBeforeConsumerIsRegistered() throws Exception {
        // Races kill() against the window between run() starting and the consumer being published
        // to consumerRef: if kill() lands in that window, stop() finds no consumer to wake up, and
        // only the isActive check now guards the first poll() from running for the full pollDuration.
        var topic = "tu_kill_early_" + IdUtils.create();
        var groupId = "tu_kill_early_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        RunContext runContext = runContextFactory.of(Map.of());
        Thread runner = new Thread(() -> {
            try {
                task.run(runContext);
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        // No sleep here on purpose: kill() must race the consumer's construction/subscription.
        task.kill();

        assertThat("Consume task must terminate promptly even when killed before the consumer is constructed",
            completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A killed run() must not be reported as a clean success", thrown.get(), notNullValue());
    }

    @Test
    void shouldNotCommitOffsetsOnKill() throws Exception {
        var topic = "tu_kill_offset_" + IdUtils.create();
        var groupId = "tu_kill_offset_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var completed = new CountDownLatch(1);
        RunContext runContext = runContextFactory.of(Map.of());
        Thread runner = new Thread(() -> {
            try {
                task.run(runContext);
            } catch (Throwable ignored) {
                // expected: a killed run surfaces as a failure rather than a clean success
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        Thread.sleep(Duration.ofSeconds(3).toMillis());
        produceRecords(topic, 5);
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        task.kill();
        assertThat("Consume task must terminate after kill()", completed.await(15, TimeUnit.SECONDS), is(true));

        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap))) {
            var offsets = adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(15, TimeUnit.SECONDS);

            assertThat("A killed consume task must not commit offsets (at-least-once redelivery)", offsets.isEmpty(), is(true));
        }
    }

    @Test
    void shouldTerminatePromptlyOnKillGivenShareGroup() throws Exception {
        var topic = "tu_kill_share_" + IdUtils.create();
        var groupId = "tu_kill_share_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .groupType(Property.ofValue(GroupType.SHARE))
            .acknowledgeType(Property.ofValue(QueueAcknowledgeType.ACCEPT))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        RunContext runContext = runContextFactory.of(Map.of());
        Thread runner = new Thread(() -> {
            try {
                task.run(runContext);
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        Thread.sleep(Duration.ofSeconds(3).toMillis());
        produceRecords(topic, 5);
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        long killStart = System.currentTimeMillis();
        task.kill();
        long killElapsedMs = System.currentTimeMillis() - killStart;

        assertThat("SHARE consume task kill() must not block for the full pollDuration", killElapsedMs, lessThan(15000L));
        assertThat("SHARE consume task must terminate after kill()", completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A killed SHARE run() must not be reported as a clean success", thrown.get(), notNullValue());
    }

    @Test
    void shouldCompleteGracefullyOnStop() throws Exception {
        // Unlike kill(), a graceful stop() (e.g. worker restart) must not fail the task: it should
        // commit what was already consumed and return a partial Output instead of throwing.
        var topic = "tu_stop_" + IdUtils.create();
        var groupId = "tu_stop_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(IdUtils.create())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        var output = new AtomicReference<Consume.Output>();
        RunContext runContext = runContextFactory.of(Map.of());
        Thread runner = new Thread(() -> {
            try {
                output.set(task.run(runContext));
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        Thread.sleep(Duration.ofSeconds(3).toMillis());
        produceRecords(topic, 5);
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        task.stop();
        assertThat("Consume task must terminate after stop()", completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A graceful stop() must not fail the task", thrown.get(), nullValue());
        assertThat("A graceful stop() must still return the already-consumed records", output.get(), notNullValue());
        assertThat(output.get().getMessagesCount(), is(5));

        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrap))) {
            var offsets = adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(15, TimeUnit.SECONDS);

            assertThat("A gracefully stopped consume task must commit offsets for already-consumed records", offsets.isEmpty(), is(false));
        }
    }

    private void produceRecords(String topic, int count) throws Exception {
        var records = new ArrayList<Map<String, String>>();
        for (int i = 0; i < count; i++) {
            records.add(ImmutableMap.of("key", "key" + i, "value", "value" + i));
        }

        Produce produce = Produce.builder()
            .id(ConsumeTest.class.getSimpleName())
            .type(Produce.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keySerializer(Property.ofValue(SerdeType.STRING))
            .valueSerializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue(topic))
            .from(records)
            .build();

        produce.run(runContextFactory.of(Map.of()));
    }
}

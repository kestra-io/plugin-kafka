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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

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
    void shouldThrowIllegalGivenPatternAndNoGroupId() {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume
            .builder()
            .topicPattern(Property.ofValue(".*"))
            .build();
        // When/Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldGetTopicPartitionSubscriptionGivenTopicAndNoGroupId() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(Map.of());
        Consume task = Consume
            .builder()
            .topic("topic")
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);
        subscription.subscribe(runContext, new MockConsumer<>(OffsetResetStrategy.EARLIEST), task);

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
            matchedRecords::add
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
            matchedRecords::add
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
    void shouldTerminatePromptlyOnKill() throws Exception {
        var topic = "tu_kill_" + IdUtils.create();
        var groupId = "tu_kill_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(ConsumeTest.class.getSimpleName())
            .type(Consume.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
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
    void shouldNotCommitOffsetsOnKill() throws Exception {
        var topic = "tu_kill_offset_" + IdUtils.create();
        var groupId = "tu_kill_offset_group_" + IdUtils.create();

        Consume task = Consume.builder()
            .id(ConsumeTest.class.getSimpleName())
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
            .id(ConsumeTest.class.getSimpleName())
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
        var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
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

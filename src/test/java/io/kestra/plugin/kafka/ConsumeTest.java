package io.kestra.plugin.kafka;


import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

@KestraTest
class ConsumeTest {

    @Inject
    private RunContextFactory runContextFactory;

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
}

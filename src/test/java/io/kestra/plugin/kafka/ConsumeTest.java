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
            .topicPattern(Property.of(".*"))
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
            .topicPattern(Property.of(".*"))
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
            .since(Property.of(now.toString()))
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
            .partitions(Property.of(List.of(0)))
            .since(Property.of(now.toString()))
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
            .groupId(Property.of("groupId"))
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
            .groupId(Property.of("groupId"))
            .topicPattern(Property.of(".*"))
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
}
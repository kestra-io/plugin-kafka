package io.kestra.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

@MicronautTest
class ConsumeTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldThrowIllegalGivenNoTopicAnNoPattern() {
        // Given
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Consume task = Consume.builder().build();
        // When/Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldThrowIllegalGivenBothTopicAnNoPattern() {
        // Given
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Consume task = Consume
            .builder()
            .topic("topic")
            .topicPattern(".*")
            .build();
        // When/Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldThrowIllegalGivenPatternAndNoGroupId() {
        // Given
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Consume task = Consume
            .builder()
            .topicPattern(".*")
            .build();
        // When/Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> task.topicSubscription(runContext));
    }

    @Test
    void shouldGetTopicPartitionSubscriptionGivenTopicAndNoGroupId() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Consume task = Consume
            .builder()
            .topic("topic")
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);
        subscription.subscribe(new MockConsumer<>(OffsetResetStrategy.EARLIEST), task);

        // Then
        Assertions.assertTrue(subscription instanceof Consume.TopicPartitionsSubscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicPartitionsSubscription)subscription).topics());
    }

    @Test
    void shouldGetTopicPartitionSubscriptionWithTimestamp() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Instant now = Instant.now();
        Consume task = Consume
            .builder()
            .topic("topic")
            .since(now.toString())
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertTrue(subscription instanceof Consume.TopicPartitionsSubscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicPartitionsSubscription)subscription).topics());
        Assertions.assertEquals(now.toEpochMilli(), ((Consume.TopicPartitionsSubscription)subscription).fromTimestamp());
    }

    @Test
    void shouldGetTopicListSubscriptionGivenTopicAndGroupId() throws IllegalVariableEvaluationException {
        // Given
        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Consume task = Consume
            .builder()
            .groupId("groupId")
            .topic("topic")
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertDoesNotThrow(() -> subscription.subscribe(new MockConsumer<>(OffsetResetStrategy.EARLIEST), task));
        Assertions.assertTrue(subscription instanceof Consume.TopicListSubscription);
        Assertions.assertEquals(List.of("topic"), ((Consume.TopicListSubscription)subscription).topics());
    }

    @Test
    void shouldGetPatternSubscriptionGivenPattern() throws IllegalVariableEvaluationException {
        // Given
        MockConsumer<Object, Object> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions("topic", List.of(new PartitionInfo("topic", 0, null, null, null)));

        RunContext runContext = runContextFactory.of(ImmutableMap.of());
        Consume task = Consume
            .builder()
            .groupId("groupId")
            .topicPattern(".*")
            .build();

        // When
        Consume.ConsumerSubscription subscription = task.topicSubscription(runContext);

        // Then
        Assertions.assertDoesNotThrow(() -> subscription.subscribe(consumer, task));
        Assertions.assertTrue(subscription instanceof Consume.PatternTopicSubscription);
        Assertions.assertEquals(".*", ((Consume.PatternTopicSubscription)subscription).pattern().pattern());
    }
}
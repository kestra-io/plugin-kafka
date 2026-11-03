package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

@KestraTest
class QueueModeTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldResolveDefaultQueueModeValues() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .topic("orders")
            .build();

        assertThat(task.resolveGroupType(runContext), is(GroupType.CONSUMER));
        assertThat(task.resolveAcknowledgeType(runContext), is(QueueAcknowledgeType.ACCEPT));
    }

    @Test
    void shouldRejectSinceInShareMode() {
        var task = Consume.builder()
            .topic("orders")
            .groupType(Property.ofValue(GroupType.SHARE))
            .since(Property.ofValue(Instant.now().toString()))
            .build();

        Assertions.assertThrows(IllegalArgumentException.class, task::validateShareConfiguration);
    }

    @Test
    void shouldSubscribeShareConsumerOnTopic() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder()
            .topic("orders")
            .build();

        var shareConsumer = new MockShareConsumer<Object, Object>();
        task.shareSubscribe(runContext, shareConsumer);

        assertThat(shareConsumer.subscription(), contains("orders"));
    }

    @Test
    void shouldPropagateQueueModeOnTriggerConsumeTask() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var trigger = io.kestra.plugin.kafka.Trigger.builder()
            .id("queue-trigger")
            .type(io.kestra.plugin.kafka.Trigger.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", "localhost:9092")))
            .topic("orders")
            .groupId(Property.ofValue("orders-share-group"))
            .groupType(Property.ofValue(GroupType.SHARE))
            .acknowledgeType(Property.ofValue(QueueAcknowledgeType.RELEASE))
            .deduplicate(Property.ofValue(true))
            .build();

        var consumeTask = trigger.consumeTask();

        assertThat(runContext.render(consumeTask.getGroupType()).as(GroupType.class).orElseThrow(), is(GroupType.SHARE));
        assertThat(runContext.render(consumeTask.getAcknowledgeType()).as(QueueAcknowledgeType.class).orElseThrow(), is(QueueAcknowledgeType.RELEASE));
        assertThat(runContext.render(consumeTask.getDeduplicate()).as(Boolean.class).orElseThrow(), is(true));
    }

    @Test
    void shouldPropagateQueueModeOnRealtimeTriggerConsumeTask() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var trigger = io.kestra.plugin.kafka.RealtimeTrigger.builder()
            .id("queue-realtime-trigger")
            .type(io.kestra.plugin.kafka.RealtimeTrigger.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", "localhost:9092")))
            .topic("orders")
            .groupId(Property.ofValue("orders-share-group"))
            .groupType(Property.ofValue(GroupType.SHARE))
            .acknowledgeType(Property.ofValue(QueueAcknowledgeType.REJECT))
            .deduplicate(Property.ofValue(true))
            .build();

        var consumeTask = trigger.consumeTask();

        assertThat(runContext.render(consumeTask.getGroupType()).as(GroupType.class).orElseThrow(), is(GroupType.SHARE));
        assertThat(runContext.render(consumeTask.getAcknowledgeType()).as(QueueAcknowledgeType.class).orElseThrow(), is(QueueAcknowledgeType.REJECT));
        assertThat(runContext.render(consumeTask.getDeduplicate()).as(Boolean.class).orElseThrow(), is(true));
    }

    @Test
    void shouldAcceptRecordsRejectedByFilterInShareMode() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        var task = Consume.builder().topic("orders").build();
        var records = new ConsumerRecords<Object, Object>(Map.of(
            new TopicPartition("orders", 0), List.of(
                new ConsumerRecord<>("orders", 0, 0L, "k1", "approved"),
                new ConsumerRecord<>("orders", 0, 1L, "k2", "rejected")
            )
        ));
        var shareConsumer = new MockShareConsumer<Object, Object>();
        task.shareSubscribe(runContext, shareConsumer);
        var emitted = new ArrayList<Object>();

        var matched = task.processShareConsumerRecords(runContext, shareConsumer, records, record -> "approved".equals(record.value()), record -> emitted.add(record.key()));

        assertThat(matched, is(1));
        assertThat(emitted, contains("k1"));
    }
}

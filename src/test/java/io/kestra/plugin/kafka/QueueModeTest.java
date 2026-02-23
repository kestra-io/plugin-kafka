package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.MockShareConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
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
            .build();

        var consumeTask = trigger.consumeTask();

        assertThat(runContext.render(consumeTask.getGroupType()).as(GroupType.class).orElseThrow(), is(GroupType.SHARE));
        assertThat(runContext.render(consumeTask.getAcknowledgeType()).as(QueueAcknowledgeType.class).orElseThrow(), is(QueueAcknowledgeType.RELEASE));
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
            .build();

        var consumeTask = trigger.consumeTask();

        assertThat(runContext.render(consumeTask.getGroupType()).as(GroupType.class).orElseThrow(), is(GroupType.SHARE));
        assertThat(runContext.render(consumeTask.getAcknowledgeType()).as(QueueAcknowledgeType.class).orElseThrow(), is(QueueAcknowledgeType.REJECT));
    }
}

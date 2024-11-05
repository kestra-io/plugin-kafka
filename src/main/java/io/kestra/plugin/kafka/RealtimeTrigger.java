package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume a message in real-time from a Kafka topic and create one execution per message.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.kafka.Trigger](https://kestra.io/plugins/plugin-kafka/triggers/io.kestra.plugin.kafka.trigger) instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume a message from a Kafka topic in real time.",
            full = true,
            code = """
                id: kafka_realtime_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.value }}"

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.kafka.RealtimeTrigger
                    topic: test_kestra
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                      keyDeserializer: STRING
                      valueDeserializer: AVRO
                    groupId: kafkaConsumerGroupId"""
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, KafkaConnectionInterface, KafkaConsumerInterface {
    private Property<Map<String, String>> properties;

    @Builder.Default
    private Property<Map<String, String>> serdeProperties = Property.of(new HashMap<>());

    private Object topic;

    private Property<List<Integer>> partitions;

    private Property<String> topicPattern;

    @NotNull
    private Property<String> groupId;

    @Builder.Default
    private Property<SerdeType> keyDeserializer = Property.of(SerdeType.STRING);

    @Builder.Default
    private Property<SerdeType> valueDeserializer = Property.of(SerdeType.STRING);

    private Property<String> since;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<Consumer<Object, Object>> consumer = new AtomicReference<>();

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
        RunContext runContext = conditionContext.getRunContext();

        Consume task = Consume.builder()
            .id(this.id)
            .type(Consume.class.getName())
            .properties(this.properties)
            .serdeProperties(this.serdeProperties)
            .topic(this.topic)
            .topicPattern(this.topicPattern)
            .partitions(this.partitions)
            .groupId(this.groupId)
            .keyDeserializer(this.keyDeserializer)
            .valueDeserializer(this.valueDeserializer)
            .since(this.since)
            .build();

        return Flux.from(publisher(task, runContext))
            .map((record) -> TriggerService.generateRealtimeExecution(this, conditionContext, context, task.recordToMessage(record)));
    }

    public Publisher<ConsumerRecord<Object, Object>> publisher(final Consume task,
                                                               final RunContext runContext) {
        return Flux.create(fluxSink -> {
            try (KafkaConsumer<Object, Object> consumer = task.consumer(runContext)) {
                this.consumer.set(consumer);
                task.topicSubscription(runContext).subscribe(runContext, consumer, task);
                while (isActive.get()) {
                    try {
                        consumer.poll(Duration.ofMillis(Long.MAX_VALUE)).forEach(fluxSink::next);
                        consumer.commitSync();
                    } catch (org.apache.kafka.common.errors.InterruptException e) {
                        // ignore, this case is handle by next lines
                    }
                    // Check if the current thread has been interrupted before next poll.
                    if (Thread.currentThread().isInterrupted()) {
                        isActive.set(false); // proactively stop polling
                    }
                }
            } catch (WakeupException e) {
                // ignore and stop
            } catch (Exception e) {
                fluxSink.error(e);
            } finally {
                fluxSink.complete();
                this.waitForTermination.countDown();
            }
        });
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        Optional.ofNullable(consumer.get()).ifPresent(consumer -> {
            consumer.wakeup();
            if (wait) {
                try {
                    this.waitForTermination.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}

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
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
    title = "Start a Flow for each Kafka record",
    description = """
        Consumes Kafka messages as they arrive and starts one Execution per record.
        In `groupType: CONSUMER` (default), behavior is classic consumer groups with manual offset commits (auto-commit disabled) and STRING deserializers by default.
        Configure `groupId`, `serdeProperties`, or `since` to control offsets and schema handling.
        In `groupType: SHARE`, behavior is queue semantics with share groups and explicit acknowledgements.
        In `SHARE` mode, use `topic` with `groupId`; `topicPattern`, `partitions`, and `since` are not supported.
        Use header filters to drop unmatched records. Prefer the batch [Kafka Trigger](https://kestra.io/plugins/plugin-kafka/triggers/io.kestra.plugin.kafka.trigger) for interval-based pulls.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Consume a message from a Kafka topic in real time.",
            code = """
                id: kafka_realtime_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.value ?? '' }}"

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
        ),
        @Example(
            full = true,
            title = "Use Kafka Realtime Trigger to push events into MongoDB",
            code = """
                id: kafka_realtime_trigger
                namespace: company.team

                tasks:
                  - id: insert_into_mongodb
                    type: io.kestra.plugin.mongodb.InsertOne
                    connection:
                      uri: mongodb://mongoadmin:secret@localhost:27017/?authSource=admin
                    database: kestra
                    collection: products
                    document: |
                      {
                        "product_id": "{{ trigger.value ?? '' | jq('.product_id') | first }}",
                        "product_name": "{{ trigger.value ?? '' | jq('.product_name') | first }}",
                        "category": "{{ trigger.value ?? '' | jq('.product_category') | first }}",
                        "brand": "{{ trigger.value ?? '' | jq('.brand') | first }}"
                      }

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.kafka.RealtimeTrigger
                    topic: products
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      valueDeserializer: JSON
                    groupId: kestraConsumer
                """
        ),
        @Example(
            full = true,
            title = "Use Kafka share group queue semantics in realtime",
            code = """
                id: kafka_realtime_share_group
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.value ?? '' }}"

                triggers:
                  - id: realtime_trigger
                    type: io.kestra.plugin.kafka.RealtimeTrigger
                    topic: orders
                    properties:
                      bootstrap.servers: localhost:9092
                    groupId: orders-share-group
                    groupType: SHARE
                    acknowledgeType: ACCEPT
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, KafkaConnectionInterface, KafkaConsumerInterface {
    private Property<Map<String, String>> properties;

    @Builder.Default
    private Property<Map<String, String>> serdeProperties = Property.ofValue(new HashMap<>());

    private Object topic;

    private Property<List<Integer>> partitions;

    private Property<String> topicPattern;

    @NotNull
    private Property<String> groupId;

    @Schema(
        title = "Group protocol to consume with",
        description = """
            `CONSUMER` (default) streams with classic consumer-group behavior.
            `SHARE` streams with Kafka share-group queue semantics and explicit acknowledgements.
            In `SHARE` mode, use `topic` with `groupId`; `topicPattern`, `partitions`, and `since` are not supported.
            """
    )
    @Builder.Default
    private Property<GroupType> groupType = Property.ofValue(GroupType.CONSUMER);

    @Schema(
        title = "Acknowledgement action for SHARE group type",
        description = """
            Used only when `groupType` is `SHARE`.
            `ACCEPT` (default) acknowledges processed records, `RELEASE` returns records to the queue, `REJECT` negatively acknowledges records,
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
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<Consumer<Object, Object>> consumer = new AtomicReference<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<ShareConsumer<Object, Object>> shareConsumer = new AtomicReference<>();

    @Schema(
        title = "Filter messages by Kafka headers",
        description = "Consume records only when all header key/value pairs match exactly (last header wins, UTF-8 comparison)"
    )
    private Property<Map<String, String>> headerFilters;

    protected Consume consumeTask() {
        return Consume.builder()
            .id(this.id)
            .type(Consume.class.getName())
            .properties(this.properties)
            .serdeProperties(this.serdeProperties)
            .topic(this.topic)
            .topicPattern(this.topicPattern)
            .partitions(this.partitions)
            .groupId(this.groupId)
            .groupType(this.groupType)
            .acknowledgeType(this.acknowledgeType)
            .keyDeserializer(this.keyDeserializer)
            .valueDeserializer(this.valueDeserializer)
            .onSerdeError(this.onSerdeError)
            .since(this.since)
            .headerFilters(this.headerFilters)
            .build();
    }

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) {
        RunContext runContext = conditionContext.getRunContext();

        Consume task = consumeTask();

        return Flux.from(publisher(task, runContext))
            .map((record) -> TriggerService.generateRealtimeExecution(this, conditionContext, context, task.recordToMessage(record)));
    }

    public Publisher<ConsumerRecord<Object, Object>> publisher(final Consume task,
                                                               final RunContext runContext) {
        return Flux.create(fluxSink -> {
            try {
                if (task.resolveGroupType(runContext) == GroupType.SHARE) {
                    runWithShareConsumer(task, runContext, fluxSink);
                } else {
                    runWithConsumer(task, runContext, fluxSink);
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

    private void runWithConsumer(Consume task,
                                 RunContext runContext,
                                 FluxSink<ConsumerRecord<Object, Object>> fluxSink) throws Exception {
        try (KafkaConsumer<Object, Object> consumer = task.consumer(runContext)) {
            this.consumer.set(consumer);
            task.topicSubscription(runContext).subscribe(runContext, consumer, task);
            runPollingLoop(() -> {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                task.processConsumerRecords(runContext, records, fluxSink::next);
                consumer.commitSync();
            });
        }
    }

    private void runWithShareConsumer(Consume task,
                                      RunContext runContext,
                                      FluxSink<ConsumerRecord<Object, Object>> fluxSink) throws Exception {
        task.validateShareConfiguration();
        try (var consumer = task.shareConsumer(runContext)) {
            this.shareConsumer.set(consumer);
            task.shareSubscribe(runContext, consumer);
            runPollingLoop(() -> {
                var records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                task.processShareConsumerRecords(runContext, consumer, records, fluxSink::next);
                consumer.commitSync();
            });
        }
    }

    private void runPollingLoop(PollAction pollAction) throws Exception {
        while (isActive.get()) {
            try {
                pollAction.run();
            } catch (org.apache.kafka.common.errors.InterruptException e) {
                // ignore, this case is handle by next lines
            }
            // Check if the current thread has been interrupted before next poll.
            if (Thread.currentThread().isInterrupted()) {
                isActive.set(false); // proactively stop polling
            }
        }
    }

    @FunctionalInterface
    private interface PollAction {
        void run() throws Exception;
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

        var hasConsumer = consumer.get() != null || shareConsumer.get() != null;
        Optional.ofNullable(consumer.get()).ifPresent(Consumer::wakeup);
        Optional.ofNullable(shareConsumer.get()).ifPresent(ShareConsumer::wakeup);

        if (wait && hasConsumer) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

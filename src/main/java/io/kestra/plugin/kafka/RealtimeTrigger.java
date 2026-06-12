package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.registry.SchemaRegistryVendor;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import org.slf4j.Logger;

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
        Emits DEBUG logs on startup, subscription, connection confirmation, and shutdown — grep by triggerId to verify Kafka connectivity.
        When the broker is unreachable, the trigger backs off exponentially instead of busy-looping,
        and emits a WARN log on each backoff escalation.
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

    // How long a single consumer.poll() call may block. A finite value lets the loop detect
    // failure spins: a poll that returns in well under this duration with zero records indicates
    // the broker is unreachable (connection refused, timeout, etc.).
    private static final Duration POLL_DURATION = Duration.ofSeconds(2);

    // Exponential backoff parameters (internal, not user-configurable).
    private static final long BACKOFF_INITIAL_MS = 1_000L;
    private static final long BACKOFF_CAP_MS = 30_000L;

    // A poll that completes in less than this fraction of POLL_DURATION with zero records is
    // treated as a failure spin (broker unreachable, returned immediately).
    private static final double FAST_EMPTY_POLL_THRESHOLD = 0.5;

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
    @PluginProperty(group = "advanced")
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
    @PluginProperty(group = "advanced")
    private Property<QueueAcknowledgeType> acknowledgeType = Property.ofValue(QueueAcknowledgeType.ACCEPT);

    @Builder.Default
    private Property<SerdeType> keyDeserializer = Property.ofValue(SerdeType.STRING);

    @Builder.Default
    private Property<SerdeType> valueDeserializer = Property.ofValue(SerdeType.STRING);

    @Schema(
        title = "Schema registry vendor."
    )
    @PluginProperty
    private SchemaRegistryVendor schemaRegistryVendor;

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

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicReference<Thread> pollThread = new AtomicReference<>();

    @Schema(
        title = "Filter messages by Kafka headers",
        description = "Consume records only when all header key/value pairs match exactly (last header wins, UTF-8 comparison)"
    )
    @PluginProperty(group = "advanced")
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
            .schemaRegistryVendor(this.schemaRegistryVendor)
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
            var logger = runContext.logger();
            try {
                var rProperties = runContext.render(this.properties).asMap(String.class, String.class);
                var rGroupId = runContext.render(this.groupId).as(String.class).orElse(null);
                var rGroupType = task.resolveGroupType(runContext);
                var rKeyDeserializer = runContext.render(this.keyDeserializer).as(SerdeType.class).orElse(SerdeType.STRING);
                var rValueDeserializer = runContext.render(this.valueDeserializer).as(SerdeType.class).orElse(SerdeType.STRING);
                var bootstrapServers = rProperties.get("bootstrap.servers");

                logger.debug(
                    "Starting Kafka trigger triggerId={} bootstrap.servers={} groupId={} groupType={} topics={} topicPattern={} partitions={} keyDeserializer={} valueDeserializer={}",
                    this.id, bootstrapServers, rGroupId, rGroupType,
                    this.topic, this.topicPattern, this.partitions,
                    rKeyDeserializer, rValueDeserializer
                );

                if (rGroupType == GroupType.SHARE) {
                    runWithShareConsumer(task, runContext, fluxSink);
                } else {
                    runWithConsumer(task, runContext, fluxSink);
                }
            } catch (WakeupException e) {
                logger.debug("Kafka trigger triggerId={} woken up; stopping poll loop", this.id);
            } catch (Exception e) {
                logger.error("Kafka trigger triggerId={} failed with error: {}", this.id, e.getMessage());
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
        Logger logger = runContext.logger();
        try (KafkaConsumer<Object, Object> consumer = task.consumer(runContext)) {
            this.consumer.set(consumer);
            logger.debug("Kafka consumer created for triggerId={} groupId={} groupType=CONSUMER", this.id, runContext.render(this.groupId).as(String.class).orElse(null));

            var subscription = task.topicSubscription(runContext);
            var firstAssignment = new AtomicBoolean(false);
            var rebalanceListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (firstAssignment.compareAndSet(false, true)) {
                        logger.debug("Kafka connection established for triggerId={}: partitions assigned = {}", RealtimeTrigger.this.id, partitions);
                    }
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // intentionally left blank — revocation does not change connection state
                }
            };
            subscription.subscribe(runContext, consumer, task, rebalanceListener);

            logger.debug(
                "Subscribed for triggerId={}: topics={} topicPattern={} partitions={}",
                this.id,
                this.topic,
                this.topicPattern,
                this.partitions
            );

            runPollingLoop(logger, fluxSink, () -> {
                long start = System.nanoTime();
                var records = consumer.poll(POLL_DURATION);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                task.processConsumerRecords(runContext, records, fluxSink::next);
                consumer.commitSync();
                return records.isEmpty() ? (isFailureSpin(elapsedMs) ? -1 : 0) : records.count();
            });
        }
    }

    private void runWithShareConsumer(Consume task,
                                      RunContext runContext,
                                      FluxSink<ConsumerRecord<Object, Object>> fluxSink) throws Exception {
        Logger logger = runContext.logger();
        task.validateShareConfiguration();
        try (var consumer = task.shareConsumer(runContext)) {
            this.shareConsumer.set(consumer);
            logger.debug("Kafka consumer created for triggerId={} groupId={} groupType=SHARE", this.id, runContext.render(this.groupId).as(String.class).orElse(null));

            task.shareSubscribe(runContext, consumer);
            logger.debug(
                "Subscribed for triggerId={}: topics={} topicPattern={} partitions={}",
                this.id,
                this.topic,
                this.topicPattern,
                this.partitions
            );

            var firstShareBatch = new AtomicBoolean(false);
            runPollingLoop(logger, fluxSink, () -> {
                long start = System.nanoTime();
                var records = consumer.poll(POLL_DURATION);
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                // compareAndSet flips the flag and returns true only on the first non-empty batch, so this logs once
                if (!records.isEmpty() && firstShareBatch.compareAndSet(false, true)) {
                    logger.debug("Kafka SHARE connection confirmed for triggerId={}: first batch received ({} records)", this.id, records.count());
                }
                task.processShareConsumerRecords(runContext, consumer, records, fluxSink::next);
                consumer.commitSync();
                return records.isEmpty() ? (isFailureSpin(elapsedMs) ? -1 : 0) : records.count();
            });
        }
    }

    /**
     * Returns true when a poll that returned zero records completed suspiciously fast,
     * indicating the broker is unreachable rather than the topic simply being idle.
     * A healthy idle poll blocks for approximately POLL_DURATION; a refused-connection
     * or immediate timeout returns in a tiny fraction of that.
     */
    private static boolean isFailureSpin(long elapsedMs) {
        return elapsedMs < (long) (POLL_DURATION.toMillis() * FAST_EMPTY_POLL_THRESHOLD);
    }

    /**
     * Runs the poll loop with exponential backoff when the broker is unreachable.
     *
     * A poll iteration is a "failure spin" when it either:
     * - throws any exception other than InterruptException, or
     * - returns zero records in well under POLL_DURATION (fast-empty heuristic).
     *
     * When Kafka cannot connect to any broker (connection refused, TLS error, invalid protocol
     * response, etc.) it will throw after its internal retry window, or return immediately with
     * zero records. On a failure spin the loop sleeps with exponential backoff before the next
     * attempt. Backoff resets to zero as soon as a poll succeeds or blocks normally (idle topic).
     * The backoff sleep is interruptible so stop()/kill() terminate promptly.
     */
    private void runPollingLoop(Logger logger,
                                FluxSink<ConsumerRecord<Object, Object>> fluxSink,
                                PollAction pollAction) throws Exception {
        long currentBackoffMs = 0;
        int failureSpins = 0;
        boolean inRetryState = false;

        pollThread.set(Thread.currentThread());

        while (isActive.get()) {
            boolean pollFailed = false;

            int recordCount;
            try {
                recordCount = pollAction.run();
                // negative sentinel: fast-empty heuristic fired (broker unreachable, no exception)
                if (recordCount < 0) {
                    pollFailed = true;
                    if (!inRetryState) {
                        logger.warn(
                            "Kafka trigger triggerId={} poll returned immediately with no records; broker may be unreachable — will retry with backoff",
                            this.id
                        );
                    }
                }
            } catch (org.apache.kafka.common.errors.InterruptException e) {
                // ignore, handled by isInterrupted check below
            } catch (Exception e) {
                pollFailed = true;
                if (!inRetryState) {
                    logger.warn(
                        "Kafka trigger triggerId={} broker unreachable ({}); will retry with backoff",
                        this.id, e.getMessage()
                    );
                }
            }

            if (Thread.currentThread().isInterrupted()) {
                isActive.set(false);
                break;
            }

            if (pollFailed) {
                failureSpins++;

                long nextBackoffMs = currentBackoffMs == 0
                    ? BACKOFF_INITIAL_MS
                    : Math.min(currentBackoffMs * 2, BACKOFF_CAP_MS);

                if (nextBackoffMs != currentBackoffMs || !inRetryState) {
                    logger.warn(
                        "Kafka trigger triggerId={} retrying (attempt #{}) after {}ms backoff",
                        this.id, failureSpins, nextBackoffMs
                    );
                }

                currentBackoffMs = nextBackoffMs;
                inRetryState = true;

                // interruptible sleep — wakeup()/stop()/kill() break out immediately
                try {
                    Thread.sleep(currentBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    isActive.set(false);
                    break;
                }
            } else {
                if (inRetryState) {
                    logger.debug("Kafka trigger triggerId={} reconnected; resetting backoff", this.id);
                }
                currentBackoffMs = 0;
                failureSpins = 0;
                inRetryState = false;
            }
        }
    }

    @FunctionalInterface
    private interface PollAction {
        /**
         * Executes one poll cycle and returns the number of records processed.
         * Returns a negative value when the poll completed suspiciously fast with zero records
         * (fast-empty heuristic, indicating broker unreachability without an exception).
         */
        int run() throws Exception;
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

        // Logging here requires a stable logger reference since RunContext may not be available
        org.slf4j.LoggerFactory.getLogger(RealtimeTrigger.class)
            .debug("Stopping Kafka trigger triggerId={} (wait={})", this.id, wait);

        var hasConsumer = consumer.get() != null || shareConsumer.get() != null;
        Optional.ofNullable(consumer.get()).ifPresent(Consumer::wakeup);
        Optional.ofNullable(shareConsumer.get()).ifPresent(ShareConsumer::wakeup);
        // interrupt the poll thread so that backoff sleeps wake up immediately
        Optional.ofNullable(pollThread.get()).ifPresent(Thread::interrupt);

        if (wait && hasConsumer) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

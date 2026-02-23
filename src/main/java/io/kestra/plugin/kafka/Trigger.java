package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Flow on scheduled Kafka pulls",
    description = """
        Polls Kafka on a fixed interval (default PT1M, pollDuration PT5S) to batch records into one Execution.
        In `groupType: CONSUMER` (default), behavior is classic consumer groups with manual offset commits and committed-only reads.
        In `groupType: SHARE`, behavior is queue semantics with share groups and explicit acknowledgements.
        Records are stored in internal storage at `{{ trigger.uri }}`; defaults use STRING deserializers.
        Use header filters to drop mismatching records or switch to [RealtimeTrigger](https://kestra.io/plugins/plugin-kafka/triggers/io.kestra.plugin.kafka.realtimetrigger) for one-execution-per-record.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: kafka_trigger
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.value ?? '' }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.kafka.Trigger
                    topic: test_kestra
                    properties:
                      bootstrap.servers: localhost:9092
                    serdeProperties:
                      schema.registry.url: http://localhost:8085
                    keyDeserializer: STRING
                    valueDeserializer: AVRO
                    interval: PT30S
                    maxRecords: 5
                    groupId: kafkaConsumerGroupId
                """
        ),
        @Example(
            full = true,
            title = "Use a Kafka share group for queue semantics",
            code = """
                id: kafka_trigger_share_group
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.messagesCount ?? 0 }}"

                triggers:
                  - id: trigger
                    type: io.kestra.plugin.kafka.Trigger
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, KafkaConnectionInterface, ConsumeInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

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
            `CONSUMER` (default) polls with classic consumer-group behavior.
            `SHARE` polls with Kafka share-group queue semantics and explicit acknowledgements.
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
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(5));

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

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
            .pollDuration(this.pollDuration)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .headerFilters(this.headerFilters)
            .build();
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        Consume task = consumeTask();
        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages for: '{}'", run.getMessagesCount(), task.getSubscription());
        }

        if (run.getMessagesCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}

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
    title = "Trigger a flow on message consumption periodically from Kafka topics.",
    description = "Note that you don't need an extra task to consume the message from the event trigger. The trigger will automatically consume messages and you can retrieve their content in your flow using the `{{ trigger.uri }}` variable. If you would like to consume each message from a Kafka topic in real-time and create one execution per message, you can use the [io.kestra.plugin.kafka.RealtimeTrigger](https://kestra.io/plugins/plugin-kafka/triggers/io.kestra.plugin.kafka.realtimetrigger) instead."
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
                    message: "{{ trigger.value }}"

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
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, KafkaConnectionInterface, ConsumeInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

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

    private OnSerdeError onSerdeError;

    private Property<String> since;

    @Builder.Default
    private Property<Duration> pollDuration = Property.of(Duration.ofSeconds(5));

    private Property<Integer> maxRecords;

    private Property<Duration> maxDuration;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

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
            .onSerdeError(this.onSerdeError)
            .since(this.since)
            .pollDuration(this.pollDuration)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .build();
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

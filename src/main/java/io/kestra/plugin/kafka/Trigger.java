package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = """
    React to and consume messages from one or more Kafka topics. Note that you don't need an extra task to consume the message from the event trigger. The trigger will automatically consume messages and you can retrieve their content in your flow using the `"{{ trigger.uri }}"` variable."""
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topic: test_kestra",
                "properties:",
                "  bootstrap.servers: localhost:9092",
                "serdeProperties:",
                "  schema.registry.url: http://localhost:8085",
                "keyDeserializer: STRING",
                "valueDeserializer: AVRO",
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, KafkaConnectionInterface, ConsumeInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Map<String, String> properties;

    @Builder.Default
    private Map<String, String> serdeProperties = Collections.emptyMap();

    private Object topic;

    private String groupId;

    @Builder.Default
    private SerdeType keyDeserializer = SerdeType.STRING;

    @Builder.Default
    private SerdeType valueDeserializer = SerdeType.STRING;

    private String since;

    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(2);

    private Integer maxRecords;

    private Duration maxDuration;

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
            .groupId(this.groupId)
            .keyDeserializer(this.keyDeserializer)
            .valueDeserializer(this.valueDeserializer)
            .since(this.since)
            .pollDuration(this.pollDuration)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .build();
        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages from '{}'", run.getMessagesCount(), task.topics(runContext));
        }

        if (run.getMessagesCount() == 0) {
            return Optional.empty();
        }

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            run
        );

        Execution execution = Execution.builder()
            .id(runContext.getTriggerExecutionId())
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}

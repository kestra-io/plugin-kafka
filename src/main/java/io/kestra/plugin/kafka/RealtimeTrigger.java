package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React to and consume messages from one or more Kafka topics creating one executions for each message."
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
    },
    beta = true
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Message>, KafkaConnectionInterface, ConsumeInterface {
    private Map<String, String> properties;

    @Builder.Default
    private Map<String, String> serdeProperties = Collections.emptyMap();

    private Object topic;

    private List<Integer> partitions;

    private String topicPattern;

    @NotNull
    private String groupId;

    @Builder.Default
    private SerdeType keyDeserializer = SerdeType.STRING;

    @Builder.Default
    private SerdeType valueDeserializer = SerdeType.STRING;

    private String since;

    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(5);

    private Integer maxRecords;

    private Duration maxDuration;

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
            .pollDuration(this.pollDuration)
            .maxRecords(this.maxRecords)
            .maxDuration(this.maxDuration)
            .build();

        return Flux.from(task.stream(runContext))
            .map((record) -> TriggerService.generateRealtimeExecution(this, context, task.recordToMessage(record)));
    }
}

package io.kestra.plugin.kafka.admin;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Increase the partition count of a Kafka topic",
    description = """
        Grows a topic to a new total partition count using the Kafka AdminClient. Partition counts can only be increased, never decreased.
        Fails with `UnknownTopicOrPartitionException` if the topic does not exist, or `InvalidPartitionsException` if `totalPartitionCount` is not greater than the current count.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Scale out a tenant topic to 12 partitions",
            full = true,
            code = """
                id: kafka_topic_create_partitions
                namespace: company.team

                tasks:
                  - id: create_partitions
                    type: io.kestra.plugin.kafka.admin.TopicCreatePartitions
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: tenant_acme_orders
                    totalPartitionCount: 12
                """
        )
    }
)
public class TopicCreatePartitions extends AbstractKafkaAdminTask implements RunnableTask<TopicCreatePartitions.Output> {

    @Schema(title = "Topic name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> topic;

    @Schema(title = "New total partition count", description = "Must be greater than the topic's current partition count.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Integer> totalPartitionCount;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTopic = requireRendered(runContext, this.topic, String.class, "topic");
        var rTotalPartitionCount = requireRendered(runContext, this.totalPartitionCount, Integer.class, "totalPartitionCount");
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.createPartitions(Map.of(rTopic, NewPartitions.increaseTo(rTotalPartitionCount))).all(), timeout);
        }

        return Output.builder().topic(rTopic).totalPartitionCount(rTotalPartitionCount).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Topic name")
        private final String topic;

        @Schema(title = "New total partition count")
        private final Integer totalPartitionCount;
    }
}

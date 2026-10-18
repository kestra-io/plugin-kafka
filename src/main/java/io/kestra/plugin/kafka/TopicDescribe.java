import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;
import java.util.stream.Collectors;
package io.kestra.plugin.kafka;

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
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe a Kafka topic",
    description = """
        Returns a topic's partition layout (leader, replicas, in-sync replicas) and its effective configuration.
        Fails with `UnknownTopicOrPartitionException` if the topic does not exist.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Inspect a tenant topic's retention config",
            full = true,
            code = """
                id: kafka_topic_describe
                namespace: company.team

                tasks:
                  - id: topic_describe
                    type: io.kestra.plugin.kafka.TopicDescribe
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: tenant_acme_orders

                  - id: log_retention
                    type: io.kestra.plugin.core.log.Log
                    message: "retention.ms = {{ outputs.topic_describe.configs['retention.ms'] }}"
                """
        )
    }
)
public class TopicDescribe extends AbstractKafkaAdminTask implements RunnableTask<TopicDescribe.Output> {

    @Schema(title = "Topic name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> topic;

    @Schema(
        title = "Include partition offsets",
        description = "When true, queries the broker for the earliest and latest offset of each partition."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> includeOffsets = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTopic = requireRendered(runContext, this.topic, String.class, "topic");
        var rIncludeOffsets = runContext.render(this.includeOffsets).as(Boolean.class).orElse(false);
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var description = get(admin.describeTopics(List.of(rTopic)).allTopicNames(), timeout).get(rTopic);

            var configResource = new ConfigResource(ConfigResource.Type.TOPIC, rTopic);
            var config = get(admin.describeConfigs(List.of(configResource)).all(), timeout).get(configResource);

            var configs = new LinkedHashMap<String, String>();
            for (ConfigEntry entry : config.entries()) {
                configs.put(entry.name(), entry.value());
            }

            Map<TopicPartition, Long> earliestOffsets = new HashMap<>();
            Map<TopicPartition, Long> latestOffsets = new HashMap<>();

            if (rIncludeOffsets && !description.partitions().isEmpty()) {
                var earliestQuery = description.partitions().stream()
                    .collect(Collectors.toMap(
                        p -> new TopicPartition(rTopic, p.partition()),
                        p -> OffsetSpec.earliest()
                    ));
                var latestQuery = description.partitions().stream()
                    .collect(Collectors.toMap(
                        p -> new TopicPartition(rTopic, p.partition()),
                        p -> OffsetSpec.latest()
                    ));

                var earliestResult = get(admin.listOffsets(earliestQuery).all(), timeout);
                var latestResult = get(admin.listOffsets(latestQuery).all(), timeout);

                earliestResult.forEach((tp, info) -> earliestOffsets.put(tp, info.offset()));
                latestResult.forEach((tp, info) -> latestOffsets.put(tp, info.offset()));
            }

            var partitions = description.partitions().stream()
                .map(p -> partitionInfo(
                    p,
                    earliestOffsets.get(new TopicPartition(rTopic, p.partition())),
                    latestOffsets.get(new TopicPartition(rTopic, p.partition()))
                ))
                .toList();

            var replicationFactor = description.partitions().isEmpty() ? 0 : description.partitions().getFirst().replicas().size();

            return Output.builder()
                .topic(rTopic)
                .partitionCount(description.partitions().size())
                .replicationFactor(replicationFactor)
                .partitions(partitions)
                .configs(configs)
                .build();
        }
    }

    private static Map<String, Object> partitionInfo(TopicPartitionInfo info, Long earliest, Long latest) {
        var partition = new LinkedHashMap<String, Object>();
        partition.put("partition", info.partition());
        partition.put("leader", info.leader() != null ? info.leader().idString() : null);
        partition.put("replicas", info.replicas().stream().map(Node::idString).toList());
        partition.put("isr", info.isr().stream().map(Node::idString).toList());
        if (earliest != null) {
            partition.put("earliestOffset", earliest);
        }
        if (latest != null) {
            partition.put("latestOffset", latest);
        }
        return partition;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Topic name")
        private final String topic;

        @Schema(title = "Number of partitions")
        private final Integer partitionCount;

        @Schema(title = "Replication factor")
        private final Integer replicationFactor;

        @Schema(title = "Per-partition layout", description = "Each entry contains `partition`, `leader`, `replicas`, `isr` (in-sync replicas broker IDs), and optionally `earliestOffset` and `latestOffset` when `includeOffsets` is true.")
        private final List<Map<String, Object>> partitions;

        @Schema(title = "Effective topic configuration", description = "For example `retention.ms`, `cleanup.policy`.")
        private final Map<String, String> configs;
    }
}

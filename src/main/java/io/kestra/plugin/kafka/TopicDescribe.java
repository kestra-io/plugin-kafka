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
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe a Kafka topic",
    description = """
        Returns a topic's partition layout (leader, replicas, in-sync replicas) and its effective configuration.
        Set `includeOffsets` to `true` to also return each partition's log start and log end offsets.
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
        ),
        @Example(
            title = "Inspect partition offsets after retention",
            full = true,
            code = """
                id: kafka_topic_offsets
                namespace: company.team

                tasks:
                  - id: describe_offsets
                    type: io.kestra.plugin.kafka.TopicDescribe
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: orders
                    includeOffsets: true

                  - id: log_offsets
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.describe_offsets.partitions }}"
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
        title = "Include per-partition earliest and latest offsets",
        description = "When enabled, fetches the log start offset (`earliestOffset`) and log end offset (`latestOffset`, the next offset to be written) for each partition. " +
            "Offsets are fetched in separate requests and can change while the task runs. Defaults to `false`, which omits these fields and skips offset requests."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
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

            var topicPartitions = description.partitions().stream()
                .map(info -> new TopicPartition(rTopic, info.partition()))
                .toList();
            var earliestOffsets = rIncludeOffsets ? fetchOffsets(admin, topicPartitions, OffsetSpec.earliest(), timeout) : Map.<TopicPartition, Long>of();
            var latestOffsets = rIncludeOffsets ? fetchOffsets(admin, topicPartitions, OffsetSpec.latest(), timeout) : Map.<TopicPartition, Long>of();

            var partitions = description.partitions().stream()
                .map(info -> {
                    var partition = partitionInfo(info);
                    if (rIncludeOffsets) {
                        var topicPartition = new TopicPartition(rTopic, info.partition());
                        partition.put("earliestOffset", earliestOffsets.get(topicPartition));
                        partition.put("latestOffset", latestOffsets.get(topicPartition));
                    }
                    return partition;
                })
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

    private static Map<TopicPartition, Long> fetchOffsets(AdminClient admin, List<TopicPartition> partitions, OffsetSpec offsetSpec, Duration timeout) throws Exception {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        var specs = partitions.stream().collect(Collectors.toMap(topicPartition -> topicPartition, topicPartition -> offsetSpec));
        return get(admin.listOffsets(specs).all(), timeout).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    private static Map<String, Object> partitionInfo(TopicPartitionInfo info) {
        var partition = new LinkedHashMap<String, Object>();
        partition.put("partition", info.partition());
        partition.put("leader", info.leader() != null ? info.leader().idString() : null);
        partition.put("replicas", info.replicas().stream().map(Node::idString).toList());
        partition.put("isr", info.isr().stream().map(Node::idString).toList());
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

        @Schema(title = "Per-partition layout", description = "Each entry contains `partition`, `leader`, `replicas` and `isr` (in-sync replicas broker IDs), plus `earliestOffset` and `latestOffset` when `includeOffsets` is enabled.")
        private final List<Map<String, Object>> partitions;

        @Schema(title = "Effective topic configuration", description = "For example `retention.ms`, `cleanup.policy`.")
        private final Map<String, String> configs;
    }
}

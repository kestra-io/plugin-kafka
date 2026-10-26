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
        Set `includeOffsets: true` to also return the earliest and latest offset of each partition.
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
            title = "Check the offset range retained in each partition",
            full = true,
            code = """
                id: kafka_topic_offsets
                namespace: company.team

                tasks:
                  - id: topic_describe
                    type: io.kestra.plugin.kafka.TopicDescribe
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: tenant_acme_orders
                    includeOffsets: true

                  - id: log_offsets
                    type: io.kestra.plugin.core.log.Log
                    message: |
                      {% for p in outputs.topic_describe.partitions %}
                      partition {{ p.partition }}: offsets {{ p.earliestOffset }} to {{ p.latestOffset }}
                      {% endfor %}
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
        description = """
            When true, each partition entry also contains `earliestOffset` (log start offset) and `latestOffset` (high watermark, the offset of the next record a consumer can read), fetched with two batched `listOffsets` requests.
            Transaction markers take offsets too, so `latestOffset - earliestOffset` can be larger than the record count. Both are null for a partition without a leader.
            Defaults to false, with no extra broker request and no offset keys.
            """
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
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

            // A partition without a leader cannot answer ListOffsets and would fail the whole request after callTimeout
            var ledPartitions = rIncludeOffsets
                ? description.partitions().stream()
                    .filter(info -> info.leader() != null)
                    .map(info -> new TopicPartition(rTopic, info.partition()))
                    .toList()
                : List.<TopicPartition>of();
            var earliestOffsets = listOffsets(admin, ledPartitions, OffsetSpec.earliest(), timeout);
            var latestOffsets = listOffsets(admin, ledPartitions, OffsetSpec.latest(), timeout);

            var partitions = description.partitions().stream()
                .map(info -> partitionInfo(info, rIncludeOffsets, new TopicPartition(rTopic, info.partition()), earliestOffsets, latestOffsets))
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

    private static Map<String, Object> partitionInfo(TopicPartitionInfo info, boolean includeOffsets, TopicPartition topicPartition, Map<TopicPartition, Long> earliestOffsets, Map<TopicPartition, Long> latestOffsets) {
        var partition = new LinkedHashMap<String, Object>();
        partition.put("partition", info.partition());
        partition.put("leader", info.leader() != null ? info.leader().idString() : null);
        partition.put("replicas", info.replicas().stream().map(Node::idString).toList());
        partition.put("isr", info.isr().stream().map(Node::idString).toList());
        // Keys are always present with includeOffsets, null for a partition without a leader, so Pebble can read them in strict mode
        if (includeOffsets) {
            partition.put("earliestOffset", earliestOffsets.get(topicPartition));
            partition.put("latestOffset", latestOffsets.get(topicPartition));
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

        @Schema(
            title = "Per-partition layout",
            description = """
                Each entry contains `partition`, `leader`, `replicas` and `isr` (in-sync replicas broker IDs).
                With `includeOffsets: true` it also contains `earliestOffset` and `latestOffset`.
                """
        )
        private final List<Map<String, Object>> partitions;

        @Schema(title = "Effective topic configuration", description = "For example `retention.ms`, `cleanup.policy`.")
        private final Map<String, String> configs;
    }
}

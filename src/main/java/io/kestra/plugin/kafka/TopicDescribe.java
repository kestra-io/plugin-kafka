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
import java.util.ArrayList;
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
            title = "Check how many records are retained in each partition",
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
                      partition {{ p.partition }}: {{ p.latestOffset - p.earliestOffset }} records retained
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
            When true, each partition entry also contains `earliestOffset` (log start offset) and `latestOffset`
            (log end offset, the next offset to be written), fetched with two batched `listOffsets` requests.
            Defaults to false, which keeps the output and the broker requests unchanged.
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

            Map<TopicPartition, Long> earliestOffsets = Map.of();
            Map<TopicPartition, Long> latestOffsets = Map.of();
            if (rIncludeOffsets) {
                var topicPartitions = description.partitions().stream()
                    .map(info -> new TopicPartition(rTopic, info.partition()))
                    .toList();
                earliestOffsets = listOffsets(admin, topicPartitions, OffsetSpec.earliest(), timeout);
                latestOffsets = listOffsets(admin, topicPartitions, OffsetSpec.latest(), timeout);
            }

            var partitions = new ArrayList<Map<String, Object>>(description.partitions().size());
            for (TopicPartitionInfo info : description.partitions()) {
                var topicPartition = new TopicPartition(rTopic, info.partition());
                partitions.add(partitionInfo(info, earliestOffsets.get(topicPartition), latestOffsets.get(topicPartition)));
            }

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

    private Map<TopicPartition, Long> listOffsets(AdminClient admin, List<TopicPartition> topicPartitions, OffsetSpec spec, Duration timeout) throws Exception {
        var request = topicPartitions.stream().collect(Collectors.toMap(tp -> tp, tp -> spec));
        return get(admin.listOffsets(request).all(), timeout).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    static Map<String, Object> partitionInfo(TopicPartitionInfo info, Long earliestOffset, Long latestOffset) {
        var partition = new LinkedHashMap<String, Object>();
        partition.put("partition", info.partition());
        partition.put("leader", info.leader() != null ? info.leader().idString() : null);
        partition.put("replicas", info.replicas().stream().map(Node::idString).toList());
        partition.put("isr", info.isr().stream().map(Node::idString).toList());
        // offsets are only present when includeOffsets is true, so the default output stays unchanged
        if (earliestOffset != null) {
            partition.put("earliestOffset", earliestOffset);
        }
        if (latestOffset != null) {
            partition.put("latestOffset", latestOffset);
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
            description = "Each entry contains `partition`, `leader`, `replicas` and `isr` (in-sync replicas broker IDs). With `includeOffsets: true` it also contains `earliestOffset` and `latestOffset`."
        )
        private final List<Map<String, Object>> partitions;

        @Schema(title = "Effective topic configuration", description = "For example `retention.ms`, `cleanup.policy`.")
        private final Map<String, String> configs;
    }
}

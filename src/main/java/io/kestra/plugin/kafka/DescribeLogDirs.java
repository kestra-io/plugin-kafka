package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe Kafka broker log directories",
    description = "Reports per-partition on-disk size and replica lag per broker log directory using the Kafka AdminClient, useful for metering per-tenant topic storage usage."
)
@Plugin(
    examples = {
        @Example(
            title = "Meter storage usage across the cluster",
            full = true,
            code = """
                id: kafka_describe_log_dirs
                namespace: company.team

                tasks:
                  - id: describe_log_dirs
                    type: io.kestra.plugin.kafka.DescribeLogDirs
                    properties:
                      bootstrap.servers: localhost:9092
                """
        )
    }
)
public class DescribeLogDirs extends AbstractKafkaAdminTask implements RunnableTask<DescribeLogDirs.Output> {

    @Schema(title = "Broker IDs to describe", description = "Describes every broker in the cluster when unset.")
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<List<Integer>> brokerIds = Property.ofValue(List.of());

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rBrokerIds = runContext.render(this.brokerIds).asList(Integer.class);
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            List<Integer> targetBrokers = rBrokerIds.isEmpty()
                ? get(admin.describeCluster().nodes(), timeout).stream().map(Node::id).toList()
                : rBrokerIds;

            var descriptions = get(admin.describeLogDirs(targetBrokers).allDescriptions(), timeout);

            List<Map<String, Object>> logDirs = new ArrayList<>();
            Map<String, Long> topicSizes = new LinkedHashMap<>();

            descriptions.forEach((brokerId, dirs) -> dirs.forEach((path, description) -> {
                var partitions = description.replicaInfos().entrySet().stream()
                    .map(entry -> {
                        var topicPartition = entry.getKey();
                        var replicaInfo = entry.getValue();
                        topicSizes.merge(topicPartition.topic(), replicaInfo.size(), Long::sum);

                        Map<String, Object> partition = new LinkedHashMap<>();
                        partition.put("topic", topicPartition.topic());
                        partition.put("partition", topicPartition.partition());
                        partition.put("size", replicaInfo.size());
                        partition.put("offsetLag", replicaInfo.offsetLag());
                        return (Map<String, Object>) partition;
                    })
                    .toList();

                Map<String, Object> logDir = new LinkedHashMap<>();
                logDir.put("brokerId", brokerId);
                logDir.put("path", path);
                logDir.put("partitions", partitions);
                logDirs.add(logDir);
            }));

            return Output.builder().logDirs(logDirs).topicSizes(topicSizes).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Per broker log directories",
            description = "Each entry contains `brokerId`, `path` and `partitions` (topic, partition, size in bytes, offsetLag)."
        )
        private final List<Map<String, Object>> logDirs;

        @Schema(title = "Total on-disk size per topic, in bytes", description = "Summed across all partitions and brokers.")
        private final Map<String, Long> topicSizes;
    }
}

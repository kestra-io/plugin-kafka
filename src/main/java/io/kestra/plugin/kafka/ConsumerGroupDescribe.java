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
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Describe Kafka consumer groups",
    description = """
        Returns state, members and per-partition committed offset/lag for the given consumer groups using the Kafka AdminClient.
        Fails with `GroupIdNotFoundException` if a group does not exist.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Check the lag of a tenant's processing consumer group",
            full = true,
            code = """
                id: kafka_consumer_group_describe
                namespace: company.team

                tasks:
                  - id: describe_groups
                    type: io.kestra.plugin.kafka.ConsumerGroupDescribe
                    properties:
                      bootstrap.servers: localhost:9092
                    groupIds:
                      - tenant-acme-orders-processor
                """
        )
    }
)
public class ConsumerGroupDescribe extends AbstractKafkaAdminTask implements RunnableTask<ConsumerGroupDescribe.Output> {

    @Schema(title = "Consumer group IDs to describe")
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> groupIds;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rGroupIds = requireNonEmpty(runContext.render(this.groupIds).asList(String.class), "groupIds").stream().distinct().toList();
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var descriptions = get(admin.describeConsumerGroups(rGroupIds).all(), timeout);

            // batched over all groupIds in a single round trip instead of one listConsumerGroupOffsets call per group
            var groupSpecs = rGroupIds.stream()
                .collect(Collectors.toMap(groupId -> groupId, groupId -> new ListConsumerGroupOffsetsSpec(), (a, b) -> a));
            var committedOffsetsByGroup = get(admin.listConsumerGroupOffsets(groupSpecs).all(), timeout);

            var allPartitions = committedOffsetsByGroup.values().stream()
                .flatMap(offsets -> offsets.keySet().stream())
                .collect(Collectors.toSet());
            var endOffsets = fetchEndOffsets(admin, allPartitions, timeout);

            var groups = rGroupIds.stream()
                .map(groupId -> {
                    var description = descriptions.get(groupId);
                    var committedOffsets = committedOffsetsByGroup.getOrDefault(groupId, Map.of());

                    var offsets = committedOffsets.entrySet().stream()
                        .map(entry -> offsetEntry(entry.getKey(), entry.getValue().offset(), endOffsets))
                        .toList();

                    var members = description.members().stream()
                        .map(ConsumerGroupDescribe::memberEntry)
                        .toList();

                    Map<String, Object> group = new LinkedHashMap<>();
                    group.put("groupId", groupId);
                    group.put("state", description.groupState().toString());
                    group.put("members", members);
                    group.put("offsets", offsets);
                    return group;
                })
                .toList();

            return Output.builder().groups(groups).build();
        }
    }

    private static Map<TopicPartition, Long> fetchEndOffsets(AdminClient admin, Set<TopicPartition> partitions, Duration timeout) throws Exception {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        var offsetSpecs = partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        var results = get(admin.listOffsets(offsetSpecs).all(), timeout);
        return results.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private static Map<String, Object> offsetEntry(TopicPartition tp, long committedOffset, Map<TopicPartition, Long> endOffsets) {
        long endOffset = endOffsets.getOrDefault(tp, committedOffset);
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("topic", tp.topic());
        entry.put("partition", tp.partition());
        entry.put("currentOffset", committedOffset);
        entry.put("endOffset", endOffset);
        entry.put("lag", endOffset - committedOffset);
        return entry;
    }

    private static Map<String, Object> memberEntry(MemberDescription member) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("memberId", member.consumerId());
        entry.put("clientId", member.clientId());
        entry.put("host", member.host());
        entry.put("assignedPartitions", member.assignment().topicPartitions().stream()
            .map(tp -> tp.topic() + "-" + tp.partition())
            .toList());
        return entry;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Described consumer groups",
            description = "Each entry contains `groupId`, `state`, `members` (memberId, clientId, host, assignedPartitions) and `offsets` (topic, partition, currentOffset, endOffset, lag)."
        )
        private final List<Map<String, Object>> groups;
    }
}

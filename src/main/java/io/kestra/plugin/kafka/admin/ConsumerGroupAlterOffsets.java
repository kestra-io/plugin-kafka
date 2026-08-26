package io.kestra.plugin.kafka.admin;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Alter committed offsets of a Kafka consumer group",
    description = """
        Overwrites the committed offset for one or more topic partitions of a consumer group using the Kafka AdminClient, typically to replay or skip records.
        Fails with `GroupNotEmptyException` if the group has active members — stop its consumers first.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Rewind a stalled consumer group to reprocess from an earlier offset",
            full = true,
            code = """
                id: kafka_consumer_group_alter_offsets
                namespace: company.team

                tasks:
                  - id: alter_offsets
                    type: io.kestra.plugin.kafka.admin.ConsumerGroupAlterOffsets
                    properties:
                      bootstrap.servers: localhost:9092
                    groupId: tenant-acme-orders-processor
                    offsets:
                      - topic: tenant_acme_orders
                        partition: 0
                        offset: 1000
                """
        )
    }
)
public class ConsumerGroupAlterOffsets extends AbstractKafkaAdminTask implements RunnableTask<ConsumerGroupAlterOffsets.Output> {

    @Schema(title = "Consumer group ID")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> groupId;

    @Schema(title = "Offsets to set", description = "One entry per topic partition to overwrite.")
    @NotNull
    @PluginProperty(group = "main")
    private List<TopicPartitionOffset> offsets;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rGroupId = runContext.render(this.groupId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'groupId'"));

        if (this.offsets == null || this.offsets.isEmpty()) {
            throw new IllegalArgumentException("Missing required property 'offsets'");
        }

        Map<TopicPartition, OffsetAndMetadata> rOffsets = new LinkedHashMap<>();
        for (TopicPartitionOffset entry : this.offsets) {
            rOffsets.put(entry.toTopicPartition(runContext), entry.toOffsetAndMetadata(runContext));
        }

        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.alterConsumerGroupOffsets(rGroupId, rOffsets).all(), timeout);
        }

        var alteredOffsets = rOffsets.entrySet().stream()
            .map(entry -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("topic", entry.getKey().topic());
                item.put("partition", entry.getKey().partition());
                item.put("offset", entry.getValue().offset());
                return (Map<String, Object>) item;
            })
            .toList();

        return Output.builder().groupId(rGroupId).alteredOffsets(alteredOffsets).build();
    }

    @Builder
    @Getter
    public static class TopicPartitionOffset {
        @Schema(title = "Topic name")
        @NotNull
        private Property<String> topic;

        @Schema(title = "Partition number")
        @NotNull
        private Property<Integer> partition;

        @Schema(title = "Offset to set")
        @NotNull
        private Property<Long> offset;

        TopicPartition toTopicPartition(RunContext runContext) throws IllegalVariableEvaluationException {
            var rTopic = runContext.render(this.topic).as(String.class)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property 'topic' in 'offsets' entry"));
            var rPartition = runContext.render(this.partition).as(Integer.class)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property 'partition' in 'offsets' entry"));
            return new TopicPartition(rTopic, rPartition);
        }

        OffsetAndMetadata toOffsetAndMetadata(RunContext runContext) throws IllegalVariableEvaluationException {
            var rOffset = runContext.render(this.offset).as(Long.class)
                .orElseThrow(() -> new IllegalArgumentException("Missing required property 'offset' in 'offsets' entry"));
            return new OffsetAndMetadata(rOffset);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Consumer group ID")
        private final String groupId;

        @Schema(title = "Offsets that were set", description = "Each entry contains `topic`, `partition` and `offset`.")
        private final List<Map<String, Object>> alteredOffsets;
    }
}

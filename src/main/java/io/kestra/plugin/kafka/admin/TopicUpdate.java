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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update a Kafka topic's configuration",
    description = """
        Incrementally alters an existing topic's configuration, typically `retention.ms` or `retention.bytes` to enforce a per-tenant data retention policy.
        Only the provided configs are changed; other configs are left untouched. Fails with `UnknownTopicOrPartitionException` if the topic does not exist.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Shrink retention for a tenant topic to 3 days",
            full = true,
            code = """
                id: kafka_topic_update
                namespace: company.team

                tasks:
                  - id: update_topic
                    type: io.kestra.plugin.kafka.admin.TopicUpdate
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: tenant_acme_orders
                    retentionMs: 259200000
                """
        )
    }
)
public class TopicUpdate extends AbstractKafkaAdminTask implements RunnableTask<TopicUpdate.Output> {

    @Schema(title = "Topic name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> topic;

    @Schema(title = "Retention duration in milliseconds", description = "Maps to the topic-level `retention.ms` config.")
    @PluginProperty(group = "main")
    private Property<Long> retentionMs;

    @Schema(title = "Retention size in bytes", description = "Maps to the topic-level `retention.bytes` config.")
    @PluginProperty(group = "main")
    private Property<Long> retentionBytes;

    @Schema(
        title = "Additional topic-level configs to set",
        description = "Any other Kafka topic config, for example `cleanup.policy` or `min.insync.replicas`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> configs = Property.ofValue(Map.of());

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTopic = runContext.render(this.topic).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'topic'"));
        var rRetentionMs = runContext.render(this.retentionMs).as(Long.class);
        var rRetentionBytes = runContext.render(this.retentionBytes).as(Long.class);
        var rConfigs = runContext.render(this.configs).asMap(String.class, String.class);
        var timeout = renderTimeout(runContext);

        var appliedConfigs = new HashMap<>(rConfigs);
        rRetentionMs.ifPresent(v -> appliedConfigs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(v)));
        rRetentionBytes.ifPresent(v -> appliedConfigs.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(v)));

        if (appliedConfigs.isEmpty()) {
            throw new IllegalArgumentException("At least one of 'retentionMs', 'retentionBytes' or 'configs' must be set");
        }

        List<AlterConfigOp> ops = buildOps(appliedConfigs);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, rTopic);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.incrementalAlterConfigs(Map.of(resource, ops)).all(), timeout);
        }

        return Output.builder()
            .topic(rTopic)
            .updatedConfigs(appliedConfigs)
            .build();
    }

    private static List<AlterConfigOp> buildOps(Map<String, String> configs) {
        List<AlterConfigOp> ops = new ArrayList<>();
        configs.forEach((key, value) -> ops.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)));
        return ops;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Updated topic name")
        private final String topic;

        @Schema(title = "Configs that were set on the topic")
        private final Map<String, String> updatedConfigs;
    }
}

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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a Kafka topic",
    description = """
        Creates a topic with the given partition count and replication factor using the Kafka AdminClient.
        Fails with `TopicExistsException` if the topic already exists, unless `ifNotExists` is set to `true`.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Provision a topic for a tenant namespace",
            full = true,
            code = """
                id: kafka_topic_create
                namespace: company.team

                tasks:
                  - id: create_topic
                    type: io.kestra.plugin.kafka.TopicCreate
                    properties:
                      bootstrap.servers: localhost:9092
                    topic: tenant_acme_orders
                    partitions: 6
                    replicationFactor: 3
                    ifNotExists: true
                    configs:
                      retention.ms: "604800000"
                """
        )
    }
)
public class TopicCreate extends AbstractKafkaAdminTask implements RunnableTask<TopicCreate.Output> {

    @Schema(title = "Topic name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> topic;

    @Schema(
        title = "Number of partitions",
        description = "No default is provided: choose an explicit partition count based on expected throughput."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Integer> partitions;

    @Schema(
        title = "Replication factor",
        description = "Defaults to `1`, which is unsuitable for production — use at least `3` on a production cluster for durability."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Integer> replicationFactor = Property.ofValue(1);

    @Schema(
        title = "Additional topic-level configs",
        description = "For example `retention.ms`, `retention.bytes`, `cleanup.policy`, `min.insync.replicas`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> configs = Property.ofValue(Map.of());

    @Schema(
        title = "Don't fail when the topic already exists",
        description = "When `true`, an existing topic is left untouched and the task succeeds, matching `kafka-topics.sh --if-not-exists` semantics. Defaults to `false`."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> ifNotExists = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTopic = requireRendered(runContext, this.topic, String.class, "topic");
        var rPartitions = requireRendered(runContext, this.partitions, Integer.class, "partitions");
        var rReplicationFactor = runContext.render(this.replicationFactor).as(Integer.class).orElse(1);
        var rIfNotExists = runContext.render(this.ifNotExists).as(Boolean.class).orElse(false);
        var rConfigs = runContext.render(this.configs).asMap(String.class, String.class);
        var timeout = renderTimeout(runContext);

        var newTopic = new NewTopic(rTopic, rPartitions, rReplicationFactor.shortValue());
        if (!rConfigs.isEmpty()) {
            newTopic.configs(rConfigs);
        }

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.createTopics(List.of(newTopic)).all(), timeout);
        } catch (TopicExistsException e) {
            if (!rIfNotExists) {
                throw e;
            }
            runContext.logger().info("Topic '{}' already exists, skipping creation as 'ifNotExists' is true", rTopic);
        }

        return Output.builder()
            .topic(rTopic)
            .partitions(rPartitions)
            .replicationFactor(rReplicationFactor)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Created topic name")
        private final String topic;

        @Schema(title = "Number of partitions")
        private final Integer partitions;

        @Schema(title = "Replication factor")
        private final Integer replicationFactor;
    }
}

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

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete one or more Kafka topics",
    description = """
        Permanently deletes topics and all their data using the Kafka AdminClient. This operation is destructive and cannot be undone.
        Fails with `UnknownTopicOrPartitionException` if a topic does not exist.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Decommission tenant topics",
            full = true,
            code = """
                id: kafka_topic_delete
                namespace: company.team

                tasks:
                  - id: delete_topics
                    type: io.kestra.plugin.kafka.TopicDelete
                    properties:
                      bootstrap.servers: localhost:9092
                    topics:
                      - tenant_acme_orders
                      - tenant_acme_shipments
                """
        )
    }
)
public class TopicDelete extends AbstractKafkaAdminTask implements RunnableTask<TopicDelete.Output> {

    @Schema(title = "Topics to delete")
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> topics;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rTopics = requireNonEmpty(runContext.render(this.topics).asList(String.class), "topics");
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.deleteTopics(rTopics).all(), timeout);
        }

        return Output.builder().deletedTopics(rTopics).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Topics that were deleted")
        private final List<String> deletedTopics;
    }
}

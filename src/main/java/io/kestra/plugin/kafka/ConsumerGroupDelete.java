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
    title = "Delete Kafka consumer groups",
    description = """
        Permanently deletes consumer groups and their committed offsets using the Kafka AdminClient. This operation is destructive and cannot be undone.
        Fails with `GroupNotEmptyException` if a group has active members — stop its consumers first.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a decommissioned tenant's consumer groups",
            full = true,
            code = """
                id: kafka_consumer_group_delete
                namespace: company.team

                tasks:
                  - id: delete_groups
                    type: io.kestra.plugin.kafka.ConsumerGroupDelete
                    properties:
                      bootstrap.servers: localhost:9092
                    groupIds:
                      - tenant-acme-orders-processor
                """
        )
    }
)
public class ConsumerGroupDelete extends AbstractKafkaAdminTask implements RunnableTask<ConsumerGroupDelete.Output> {

    @Schema(title = "Consumer group IDs to delete")
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> groupIds;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rGroupIds = requireNonEmpty(runContext.render(this.groupIds).asList(String.class), "groupIds");
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.deleteConsumerGroups(rGroupIds).all(), timeout);
        }

        return Output.builder().deletedGroups(rGroupIds).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Consumer group IDs that were deleted")
        private final List<String> deletedGroups;
    }
}

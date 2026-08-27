package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import org.apache.kafka.clients.admin.ListGroupsOptions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Kafka consumer groups",
    description = "Lists every consumer group known to the cluster, with its state, using the Kafka AdminClient."
)
@Plugin(
    examples = {
        @Example(
            title = "List all consumer groups",
            full = true,
            code = """
                id: kafka_consumer_group_list
                namespace: company.team

                tasks:
                  - id: list_groups
                    type: io.kestra.plugin.kafka.ConsumerGroupList
                    properties:
                      bootstrap.servers: localhost:9092
                """
        )
    }
)
public class ConsumerGroupList extends AbstractKafkaAdminTask implements RunnableTask<ConsumerGroupList.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var listings = get(admin.listGroups(ListGroupsOptions.forConsumerGroups()).all(), timeout);

            var groups = listings.stream()
                .map(listing -> {
                    Map<String, Object> group = new LinkedHashMap<>();
                    group.put("groupId", listing.groupId());
                    group.put("isSimpleConsumerGroup", listing.isSimpleConsumerGroup());
                    group.put("state", listing.groupState().map(Enum::toString).orElse(null));
                    return group;
                })
                .toList();

            return Output.builder().groups(groups).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Consumer groups", description = "Each entry contains `groupId`, `isSimpleConsumerGroup` and `state`.")
        private final List<Map<String, Object>> groups;
    }
}

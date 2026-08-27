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
import org.apache.kafka.clients.admin.ListTopicsOptions;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Kafka topics",
    description = "Lists topic names visible to the AdminClient. Internal topics (e.g. `__consumer_offsets`) are excluded by default."
)
@Plugin(
    examples = {
        @Example(
            title = "List all topics on a cluster",
            full = true,
            code = """
                id: kafka_topic_list
                namespace: company.team

                tasks:
                  - id: list_topics
                    type: io.kestra.plugin.kafka.TopicList
                    properties:
                      bootstrap.servers: localhost:9092
                """
        )
    }
)
public class TopicList extends AbstractKafkaAdminTask implements RunnableTask<TopicList.Output> {

    @Schema(title = "Include internal topics", description = "Defaults to `false`.")
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> includeInternal = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rIncludeInternal = runContext.render(this.includeInternal).as(Boolean.class).orElse(false);
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var options = new ListTopicsOptions().listInternal(rIncludeInternal);
            var names = get(admin.listTopics(options).names(), timeout);
            return Output.builder().topics(names.stream().sorted().toList()).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Topic names")
        private final List<String> topics;
    }
}

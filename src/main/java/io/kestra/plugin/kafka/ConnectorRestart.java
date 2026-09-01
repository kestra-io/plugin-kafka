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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Restart a Kafka Connect connector",
    description = "By default restarts only the connector instance itself, leaving its tasks untouched. Set `includeTasks: true` to also restart its tasks, and `onlyFailed: true` to restrict the restart to failed tasks only."
)
@Plugin(
    examples = {
        @Example(
            title = "Restart only the failed tasks of a connector",
            full = true,
            code = """
                id: kafka_connector_restart
                namespace: company.team

                tasks:
                  - id: restart_connector
                    type: io.kestra.plugin.kafka.ConnectorRestart
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                    includeTasks: true
                    onlyFailed: true
                """
        )
    }
)
public class ConnectorRestart extends AbstractKafkaConnectTask implements RunnableTask<ConnectorRestart.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Schema(title = "Also restart the connector's tasks", description = "Defaults to `false`, which restarts only the connector instance.")
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Boolean> includeTasks = Property.ofValue(false);

    @Schema(title = "Restrict the restart to failed tasks only", description = "Only applied when `includeTasks` is `true`. Defaults to `false`.")
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Boolean> onlyFailed = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");
        var rIncludeTasks = runContext.render(this.includeTasks).as(Boolean.class).orElse(false);
        var rOnlyFailed = runContext.render(this.onlyFailed).as(Boolean.class).orElse(false);

        var path = "/connectors/" + encodePathSegment(rConnectorName) + "/restart"
            + "?includeTasks=" + rIncludeTasks + "&onlyFailed=" + rOnlyFailed;
        var request = requestBuilder(runContext, "POST", path).build();
        execute(runContext, request, rConnectorName);

        return Output.builder()
            .connectorName(rConnectorName)
            .includeTasks(rIncludeTasks)
            .onlyFailed(rOnlyFailed)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;

        @Schema(title = "Whether tasks were included in the restart")
        private final Boolean includeTasks;

        @Schema(title = "Whether the restart was restricted to failed tasks")
        private final Boolean onlyFailed;
    }
}

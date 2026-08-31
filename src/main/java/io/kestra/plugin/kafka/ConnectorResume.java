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
    title = "Resume a paused Kafka Connect connector",
    description = "Restarts processing on a connector previously paused with `ConnectorPause`. Asynchronous — poll `ConnectorGetStatus` to confirm the `RUNNING` state has taken effect."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: kafka_connector_resume
                namespace: company.team

                tasks:
                  - id: resume_connector
                    type: io.kestra.plugin.kafka.ConnectorResume
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                """
        )
    }
)
public class ConnectorResume extends AbstractKafkaConnectTask implements RunnableTask<ConnectorResume.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");

        var request = requestBuilder(runContext, "PUT", "/connectors/" + encodePathSegment(rConnectorName) + "/resume").build();
        execute(runContext, request, rConnectorName);

        return Output.builder().connectorName(rConnectorName).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;
    }
}

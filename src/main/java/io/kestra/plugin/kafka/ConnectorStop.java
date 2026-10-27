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
    title = "Stop a Kafka Connect connector",
    description = """
        Stops the connector and shuts down its tasks while preserving its configuration and offsets.
        Requires Kafka Connect 3.5+.
        This operation is asynchronous; poll `ConnectorGetStatus` to confirm the `STOPPED` state
        before using `ConnectorAlterOffsets` or `ConnectorResetOffsets`.
        Use `ConnectorResume` to restart processing.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: kafka_connector_stop
                namespace: company.team

                tasks:
                  - id: stop_connector
                    type: io.kestra.plugin.kafka.ConnectorStop
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                """
        )
    }
)
public class ConnectorStop extends AbstractKafkaConnectTask implements RunnableTask<ConnectorStop.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");

        var request = requestBuilder(runContext, "PUT", "/connectors/" + encodePathSegment(rConnectorName) + "/stop").build();
        try {
            execute(runContext, request, rConnectorName);
        } catch (KafkaConnectApiException e) {
            if (e.getStatusCode() != 404) {
                throw e;
            }
            throw new KafkaConnectApiException(
                "Cannot stop connector '%s' on the Kafka Connect worker at %s: the connector was not found or the Kafka Connect cluster is older than 3.5 and has no stop endpoint. Check the connector name and upgrade Kafka Connect to 3.5 or later if needed.".formatted(rConnectorName, renderConnectUrl(runContext)),
                e.getStatusCode(),
                e.getResponseBody(),
                e
            );
        }

        return Output.builder().connectorName(rConnectorName).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;
    }
}

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
    title = "Reset a Kafka Connect connector's offsets",
    description = """
        Deletes all of the connector's committed offsets, so it restarts from the beginning (source) or from the consumer group's default reset policy (sink) next time it runs.

        The connector **must be in the `STOPPED` state** for this call to succeed; resetting offsets via `DELETE /connectors/{name}/offsets` is a [KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875:+First-class+offsets+support+in+Kafka+Connect) concept only available on Kafka Connect clusters running Kafka 3.6+. This task does **not** pre-validate the connector's state client-side — it always sends the request and surfaces Connect's error body verbatim (HTTP 400) if the connector isn't `STOPPED`. On older clusters that don't support this, delete and recreate the connector instead.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: kafka_connector_reset_offsets
                namespace: company.team

                tasks:
                  - id: reset_offsets
                    type: io.kestra.plugin.kafka.ConnectorResetOffsets
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_source
                """
        )
    }
)
public class ConnectorResetOffsets extends AbstractKafkaConnectTask implements RunnableTask<ConnectorResetOffsets.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");

        var request = requestBuilder(runContext, "DELETE", "/connectors/" + encodePathSegment(rConnectorName) + "/offsets").build();
        var response = execute(runContext, request, rConnectorName);
        var responseBody = parseMap(response.getBody());

        return Output.builder()
            .connectorName(rConnectorName)
            .message((String) responseBody.get("message"))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;

        @Schema(title = "Confirmation message returned by the Connect API")
        private final String message;
    }
}

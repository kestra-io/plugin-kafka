package io.kestra.plugin.kafka;

import io.kestra.core.http.HttpRequest;
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

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Alter a Kafka Connect connector's offsets",
    description = """
        Overwrites the connector's offsets with the given `offsets` value — typically obtained from `ConnectorGetOffsets` and edited, or built from scratch to skip/replay records.

        The connector **must be in the `STOPPED` state** for this call to succeed; this is a [KIP-980](https://cwiki.apache.org/confluence/display/KAFKA/KIP-980%3A+Allow+Connect+RestartRequest+to+Restart+Tasks+with+Exponential+Backoff) concept only available on Kafka Connect clusters running Kafka 3.5+ (`STOPPED` did not exist before that). This task does **not** pre-validate the connector's state client-side — it always sends the request and surfaces Connect's error body verbatim (HTTP 400) if the connector isn't `STOPPED`. Stop the connector first, e.g. with `ConnectorUpdateConfig` toggling to a stopped state or the Connect `PUT /connectors/{name}/stop` endpoint. On older clusters that don't support `STOPPED`, delete and recreate the connector instead.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Rewind a source connector's offsets",
            full = true,
            code = """
                id: kafka_connector_alter_offsets
                namespace: company.team

                tasks:
                  - id: alter_offsets
                    type: io.kestra.plugin.kafka.ConnectorAlterOffsets
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_source
                    offsets:
                      - partition:
                          table: orders
                        offset:
                          incrementing: 0
                """
        )
    }
)
public class ConnectorAlterOffsets extends AbstractKafkaConnectTask implements RunnableTask<ConnectorAlterOffsets.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Schema(
        title = "Offsets to apply",
        description = "Same shape as `ConnectorGetOffsets`'s `offsets` output: a list of `{partition, offset}` entries. The connector must be `STOPPED` — see the task description."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<Map<String, Object>>> offsets;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");
        // explicit type needed here: `var` can't disambiguate the generic item type Map<String, Object> from `asList`
        List<Map<String, Object>> rOffsets = runContext.render(this.offsets).asList(Map.class);

        var body = Map.of("offsets", rOffsets);
        var request = requestBuilder(runContext, "PATCH", "/connectors/" + encodePathSegment(rConnectorName) + "/offsets")
            .body(HttpRequest.JsonRequestBody.of(body))
            .addHeader("Content-Type", "application/json")
            .build();

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

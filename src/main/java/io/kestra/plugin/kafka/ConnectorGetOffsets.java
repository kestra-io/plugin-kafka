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

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a Kafka Connect connector's offsets",
    description = "Returns the connector's committed offsets (source partitions/offsets for a source connector, consumer group offsets for a sink connector). The output `offsets` has the same shape expected by `ConnectorAlterOffsets`, so it can be piped directly between the two tasks."
)
@Plugin(
    examples = {
        @Example(
            title = "Inspect a connector's offsets",
            full = true,
            code = """
                id: kafka_connector_get_offsets
                namespace: company.team

                tasks:
                  - id: get_offsets
                    type: io.kestra.plugin.kafka.ConnectorGetOffsets
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                """
        )
    }
)
public class ConnectorGetOffsets extends AbstractKafkaConnectTask implements RunnableTask<ConnectorGetOffsets.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");

        var request = requestBuilder(runContext, "GET", "/connectors/" + encodePathSegment(rConnectorName) + "/offsets").build();
        var response = execute(runContext, request, rConnectorName);
        var body = parseMap(response.getBody());

        @SuppressWarnings("unchecked")
        var offsets = (List<Map<String, Object>>) body.getOrDefault("offsets", List.of());

        return Output.builder().connectorName(rConnectorName).offsets(offsets).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;

        @Schema(
            title = "Per-partition offsets",
            description = "Each entry has a `partition` map and an `offset` map; their exact keys depend on the connector type (source vs sink)."
        )
        private final List<Map<String, Object>> offsets;
    }
}

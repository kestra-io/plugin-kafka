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

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a Kafka Connect connector's configuration",
    description = "Returns the connector's current configuration. The output `config` has the same shape as `ConnectorCreate`'s `config` input, so it can be piped directly into `ConnectorCreate` or `ConnectorUpdateConfig` to clone or restore a connector."
)
@Plugin(
    examples = {
        @Example(
            title = "Clone a connector's config into a new connector",
            full = true,
            code = """
                id: kafka_connector_clone
                namespace: company.team

                tasks:
                  - id: get_config
                    type: io.kestra.plugin.kafka.ConnectorGetConfig
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink

                  - id: create_clone
                    type: io.kestra.plugin.kafka.ConnectorCreate
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink_clone
                    config: "{{ outputs.get_config.config }}"
                """
        )
    }
)
public class ConnectorGetConfig extends AbstractKafkaConnectTask implements RunnableTask<ConnectorGetConfig.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");

        var request = requestBuilder(runContext, "GET", "/connectors/" + encodePathSegment(rConnectorName) + "/config").build();
        var response = execute(runContext, request, rConnectorName);

        return Output.builder()
            .connectorName(rConnectorName)
            .config(Property.ofValue(parseStringMap(response.getBody())))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;

        @Schema(
            title = "Connector configuration",
            description = "Same shape as `ConnectorCreate`'s `config` input — pass directly to `ConnectorCreate.config` or `ConnectorUpdateConfig.config`."
        )
        private final Property<Map<String, String>> config;
    }
}

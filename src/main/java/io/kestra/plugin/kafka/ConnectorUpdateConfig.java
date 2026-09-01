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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Update a Kafka Connect connector's configuration",
    description = "Replaces the entire configuration of an existing connector, restarting its tasks with the new config. Connect creates the connector if it doesn't already exist. Fails with the API's error body verbatim if `config` fails the connector plugin's own validation."
)
@Plugin(
    examples = {
        @Example(
            title = "Bump the task count of an existing connector",
            full = true,
            code = """
                id: kafka_connector_update_config
                namespace: company.team

                tasks:
                  - id: update_connector
                    type: io.kestra.plugin.kafka.ConnectorUpdateConfig
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                    config:
                      connector.class: io.confluent.connect.jdbc.JdbcSinkConnector
                      tasks.max: "3"
                      topics: orders
                      connection.url: jdbc:postgresql://postgres:5432/orders
                      connection.user: "{{ secret('POSTGRES_USER') }}"
                      connection.password: "{{ secret('POSTGRES_PASSWORD') }}"
                      insert.mode: upsert
                      pk.mode: record_key
                """
        )
    }
)
public class ConnectorUpdateConfig extends AbstractKafkaConnectTask implements RunnableTask<ConnectorUpdateConfig.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Schema(
        title = "Connector configuration",
        description = "Replaces the connector's entire configuration; must include `connector.class` and any property required by that connector plugin."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, String>> config;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");
        var rConfig = new HashMap<>(runContext.render(this.config).asMap(String.class, String.class));
        // Same defensive overwrite as ConnectorCreate: keep the embedded "name" in sync with the
        // URL's connector name in case a piped-through config (e.g. from ConnectorGetConfig) carries a stale one.
        rConfig.put("name", rConnectorName);

        var request = requestBuilder(runContext, "PUT", "/connectors/" + encodePathSegment(rConnectorName) + "/config")
            .body(HttpRequest.JsonRequestBody.of(rConfig))
            .addHeader("Content-Type", "application/json")
            .build();

        var response = execute(runContext, request, rConnectorName, rConfig);
        var info = parse(response.getBody(), ConnectorInfoResponse.class);

        return Output.builder()
            .connectorName(info.name)
            .type(info.type)
            .config(info.config)
            .taskIds(info.tasks == null ? List.of() : info.tasks.stream().map(t -> t.task).toList())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;

        @Schema(title = "Connector type", description = "`source` or `sink`, as reported by the Connect API.")
        private final String type;

        @Schema(title = "Effective connector configuration, as accepted by Connect")
        private final Map<String, String> config;

        @Schema(title = "Task ids provisioned by Connect for this connector")
        private final List<Integer> taskIds;
    }
}

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
    title = "Create a Kafka Connect connector",
    description = "Submits a new connector to the Kafka Connect REST API. Fails with the API's error body verbatim (HTTP 409) if a connector with the same name already exists, and (HTTP 400/500) if `config` fails the connector plugin's own validation."
)
@Plugin(
    examples = {
        @Example(
            title = "Create a JDBC sink connector",
            full = true,
            code = """
                id: kafka_connector_create
                namespace: company.team

                tasks:
                  - id: create_connector
                    type: io.kestra.plugin.kafka.ConnectorCreate
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                    config:
                      connector.class: io.confluent.connect.jdbc.JdbcSinkConnector
                      tasks.max: "1"
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
public class ConnectorCreate extends AbstractKafkaConnectTask implements RunnableTask<ConnectorCreate.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Schema(
        title = "Connector configuration",
        description = "Must include `connector.class` and any property required by that connector plugin (e.g. `topics`, `tasks.max`, connection settings). Left empty, Connect rejects the request with its own validation error."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, String>> config;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");
        var rConfig = runContext.render(this.config).asMap(String.class, String.class);

        var body = Map.of("name", rConnectorName, "config", rConfig);
        var request = requestBuilder(runContext, "POST", "/connectors")
            .body(HttpRequest.JsonRequestBody.of(body))
            .addHeader("Content-Type", "application/json")
            .build();

        var response = execute(runContext, request, null);
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

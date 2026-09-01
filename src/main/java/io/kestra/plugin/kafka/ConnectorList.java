package io.kestra.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
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
    title = "List Kafka Connect connectors",
    description = "Returns every connector name on the worker. Set `expandStatus: true` to also fetch each connector's status in the same call, avoiding one `ConnectorGetStatus` call per connector."
)
@Plugin(
    examples = {
        @Example(
            title = "List connectors with their status",
            full = true,
            code = """
                id: kafka_connector_list
                namespace: company.team

                tasks:
                  - id: list_connectors
                    type: io.kestra.plugin.kafka.ConnectorList
                    connectUrl: http://connect:8083
                    expandStatus: true
                """
        )
    }
)
public class ConnectorList extends AbstractKafkaConnectTask implements RunnableTask<ConnectorList.Output> {

    @Schema(title = "Fetch each connector's status alongside its name", description = "Defaults to `false`.")
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<Boolean> expandStatus = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rExpandStatus = runContext.render(this.expandStatus).as(Boolean.class).orElse(false);

        var path = "/connectors" + (rExpandStatus ? "?expand=status" : "");
        var request = requestBuilder(runContext, "GET", path).build();
        var response = execute(runContext, request, null);

        if (!rExpandStatus) {
            return Output.builder()
                .connectorNames(parseListOfStrings(response.getBody()))
                .connectors(List.of())
                .build();
        }

        Map<String, ExpandedEntry> expanded = isBlank(response.getBody())
            ? Map.of()
            : JacksonMapper.ofJson().readValue(response.getBody(), new TypeReference<Map<String, ExpandedEntry>>() {});

        var connectors = expanded.entrySet().stream()
            .map(entry -> ConnectorGetStatus.toOutput(entry.getKey(), entry.getValue().status))
            .toList();

        return Output.builder()
            .connectorNames(expanded.keySet().stream().toList())
            .connectors(connectors)
            .build();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ExpandedEntry {
        public ConnectorStatusPayload status;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector names")
        private final List<String> connectorNames;

        @Schema(title = "Per-connector status", description = "Empty unless `expandStatus` was `true`.")
        private final List<ConnectorGetStatus.Output> connectors;
    }
}

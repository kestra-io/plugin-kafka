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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get the status of a Kafka Connect connector",
    description = "Returns the connector's own state and the state of each of its tasks, as typed fields — `outputs.x.connectorState` and `outputs.x.tasks[*].state` can be used directly in flow conditions without parsing a raw JSON blob."
)
@Plugin(
    examples = {
        @Example(
            title = "Fail the flow when a connector isn't running",
            full = true,
            code = """
                id: kafka_connector_get_status
                namespace: company.team

                tasks:
                  - id: get_status
                    type: io.kestra.plugin.kafka.ConnectorGetStatus
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink

                  - id: check_running
                    type: io.kestra.plugin.core.flow.If
                    condition: "{{ outputs.get_status.connectorState != 'RUNNING' }}"
                    then:
                      - id: fail
                        type: io.kestra.plugin.core.execution.Fail
                """
        )
    }
)
public class ConnectorGetStatus extends AbstractKafkaConnectTask implements RunnableTask<ConnectorGetStatus.Output> {

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rConnectorName = requireRendered(runContext, this.connectorName, String.class, "connectorName");

        var request = requestBuilder(runContext, "GET", "/connectors/" + encodePathSegment(rConnectorName) + "/status").build();
        var response = execute(runContext, request, rConnectorName);
        var status = parse(response.getBody(), ConnectorStatusPayload.class);

        return toOutput(rConnectorName, status);
    }

    protected static Output toOutput(String connectorName, ConnectorStatusPayload status) {
        return Output.builder()
            .connectorName(connectorName)
            .connectorState(status.connector == null ? null : status.connector.state)
            .workerId(status.connector == null ? null : status.connector.workerId)
            .tasks(status.tasks == null ? List.of() : status.tasks.stream()
                .map(t -> TaskStatus.builder().id(t.id).state(t.state).workerId(t.workerId).trace(t.trace).build())
                .toList())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Connector name")
        private final String connectorName;

        @Schema(title = "Connector state", description = "e.g. `RUNNING`, `PAUSED`, `STOPPED`, `FAILED`, `UNASSIGNED`.")
        private final String connectorState;

        @Schema(title = "Id of the worker running the connector instance")
        private final String workerId;

        @Schema(title = "Status of each task provisioned for this connector")
        private final List<TaskStatus> tasks;
    }

    @Builder
    @Getter
    public static class TaskStatus {
        @Schema(title = "Task id")
        private final Integer id;

        @Schema(title = "Task state", description = "e.g. `RUNNING`, `PAUSED`, `FAILED`, `UNASSIGNED`.")
        private final String state;

        @Schema(title = "Id of the worker running this task")
        private final String workerId;

        @Schema(title = "Stack trace, present only when the task is in `FAILED` state")
        private final String trace;
    }
}

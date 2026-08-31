package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Start a Flow when a Kafka Connect connector reaches a target state",
    description = """
        Polls a connector's status on the Kafka Connect REST API on a fixed interval (default PT1M) and fires one execution when the connector itself or any of its tasks matches `targetState` (case-insensitive), e.g. `RUNNING`, `FAILED`, `PAUSED`.
        Fires on every poll where the state still matches — pair with a flow-level condition or [Pause](https://kestra.io/plugins/core/tasks/flow/io.kestra.plugin.core.flow.pause) task if only the first match should act.
        A connector deleted mid-poll is treated as no match for that tick rather than a trigger failure.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Alert when a connector fails",
            full = true,
            code = """
                id: kafka_connector_status_trigger
                namespace: company.team

                tasks:
                  - id: notify
                    type: io.kestra.plugin.core.log.Log
                    message: "Connector {{ trigger.connectorName }} is {{ trigger.connectorState }}"

                triggers:
                  - id: connector_failed
                    type: io.kestra.plugin.kafka.ConnectorStatusTrigger
                    connectUrl: http://connect:8083
                    connectorName: orders_jdbc_sink
                    targetState: FAILED
                    interval: PT30S
                """
        )
    }
)
public class ConnectorStatusTrigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<ConnectorGetStatus.Output>, KafkaConnectConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    private Property<String> connectUrl;

    private Property<String> username;

    @ToString.Exclude
    private Property<String> password;

    private Property<Map<String, String>> headers;

    @Schema(title = "Connector name")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> connectorName;

    @Schema(
        title = "Target state to watch for",
        description = "Fires an execution when the connector's own state or any of its tasks' state matches this value (case-insensitive), e.g. `RUNNING`, `FAILED`, `PAUSED`, `UNASSIGNED`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> targetState;

    protected ConnectorGetStatus statusTask() {
        return ConnectorGetStatus.builder()
            .id(this.id)
            .type(ConnectorGetStatus.class.getName())
            .connectUrl(this.connectUrl)
            .username(this.username)
            .password(this.password)
            .headers(this.headers)
            .connectorName(this.connectorName)
            .build();
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        ConnectorGetStatus.Output output;
        try {
            output = statusTask().run(runContext);
        } catch (KafkaConnectApiException e) {
            if (e.getStatusCode() == 404) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connector not found during status poll, skipping this tick: {}", e.getMessage());
                }
                return Optional.empty();
            }
            throw e;
        }

        var rTargetState = runContext.render(this.targetState).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'targetState'"));

        boolean matches = rTargetState.equalsIgnoreCase(output.getConnectorState())
            || output.getTasks().stream().anyMatch(task -> rTargetState.equalsIgnoreCase(task.getState()));

        if (!matches) {
            return Optional.empty();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Connector '{}' matched target state '{}'", output.getConnectorName(), rTargetState);
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);

        return Optional.of(execution);
    }
}

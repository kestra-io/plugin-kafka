package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@org.junit.jupiter.api.parallel.Execution(ExecutionMode.SAME_THREAD)
class ConnectorStatusTriggerTest {

    private static final String CONNECTOR_NAME = "tu_connect_trigger_source";

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.connect.url}")
    private String connectUrl;

    @Value("${kafka.connect.data-dir}")
    private String dataDir;

    @BeforeEach
    void createConnector() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Path sourceFile = Path.of(dataDir, CONNECTOR_NAME + "_source.txt");
        Files.writeString(sourceFile, "line1\n");

        Map<String, String> config = Map.of(
            "connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max", "1",
            "file", "/data/" + sourceFile.getFileName(),
            "topic", CONNECTOR_NAME + "_topic"
        );

        ConnectorCreate.builder()
            .connectUrl(Property.ofValue(this.connectUrl))
            .connectorName(Property.ofValue(CONNECTOR_NAME))
            .config(Property.ofValue(config))
            .build()
            .run(runContext);

        var statusTask = ConnectorGetStatus.builder()
            .connectUrl(Property.ofValue(this.connectUrl))
            .connectorName(Property.ofValue(CONNECTOR_NAME))
            .build();
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                // right after create, the status endpoint can briefly 404 before the config change has propagated
                if ("RUNNING".equalsIgnoreCase(statusTask.run(runContext).getConnectorState())) {
                    return;
                }
            } catch (KafkaConnectApiException e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("Connector '" + CONNECTOR_NAME + "' never reached RUNNING state");
    }

    @AfterEach
    void deleteConnector() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        ConnectorDelete.builder()
            .connectUrl(Property.ofValue(this.connectUrl))
            .connectorName(Property.ofValue(CONNECTOR_NAME))
            .build()
            .run(runContext);
        Files.deleteIfExists(Path.of(dataDir, CONNECTOR_NAME + "_source.txt"));
    }

    @Test
    @EvaluateTrigger(flow = "flows/connector_status_trigger.yaml", triggerId = "watch")
    void flow(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));

        var execution = optionalExecution.get();
        assertThat(execution.getFlowId(), is("connector_status_trigger"));
        assertThat(execution.getTrigger().getVariables().get("connectorState"), is("RUNNING"));
    }

    @Test
    void shouldTreatMissingConnectorAsNoMatchInsteadOfFailing() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder().runContext(runContext).build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("connector_status_trigger")
            .triggerId("watch")
            .date(ZonedDateTime.now())
            .build();

        ConnectorStatusTrigger trigger = ConnectorStatusTrigger.builder()
            .id("watch")
            .type(ConnectorStatusTrigger.class.getName())
            .connectUrl(Property.ofValue(this.connectUrl))
            .connectorName(Property.ofValue("tu_connect_missing_" + IdUtils.create()))
            .targetState(Property.ofValue("RUNNING"))
            .build();

        Optional<Execution> result = trigger.evaluate(conditionContext, triggerContext);

        assertThat(result.isPresent(), is(false));
    }
}

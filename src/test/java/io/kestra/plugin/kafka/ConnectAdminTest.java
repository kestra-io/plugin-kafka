package io.kestra.plugin.kafka;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests against a real Kafka Connect worker (see {@code docker-compose-ci.yml} and
 * {@code .github/setup-unit.sh}), using the FileStream source/sink connectors bundled in the
 * {@code confluentinc/cp-kafka-connect} image. The container shares {@code kafka.connect.data-dir}
 * (host) / {@code /data} (container) as a bind mount so the test JVM and the worker can read/write
 * the same files.
 */
@KestraTest
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConnectAdminTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    @Value("${kafka.connect.url}")
    private String connectUrl;

    @Value("${kafka.connect.data-dir}")
    private String dataDir;

    private Property<String> connectUrl() {
        return Property.ofValue(this.connectUrl);
    }

    private ConnectorGetStatus.Output waitForState(RunContext runContext, String connectorName, String targetState) throws Exception {
        return waitForConnectorState(runContext, connectorName, targetState, true);
    }

    /**
     * @param requireTasks whether the connector's tasks list must also be non-empty to consider the target
     *                      state reached — false for {@code STOPPED}, where KIP-980 leaves the tasks list empty.
     */
    private ConnectorGetStatus.Output waitForConnectorState(RunContext runContext, String connectorName, String targetState, boolean requireTasks) throws Exception {
        var task = ConnectorGetStatus.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build();

        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                var output = task.run(runContext);
                // right after create/pause/resume/restart, the status endpoint can briefly 404 or report the
                // tasks list as still empty before the config change has fully propagated — keep polling
                if (targetState.equalsIgnoreCase(output.getConnectorState()) && (!requireTasks || !output.getTasks().isEmpty())) {
                    return output;
                }
            } catch (KafkaConnectApiException e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
            }
            Thread.sleep(1000);
        }

        throw new AssertionError("Connector '" + connectorName + "' did not reach state '" + targetState + "' in time");
    }

    /**
     * Stops a connector via Kafka Connect's KIP-980 {@code PUT /connectors/{name}/stop} endpoint — required for
     * {@code ConnectorAlterOffsets}/{@code ConnectorResetOffsets} to succeed, but not exposed by any task in this
     * plugin yet, so it's called directly here using Kestra's internal HTTP client.
     */
    private void stopConnector(RunContext runContext, String connectorName) throws Exception {
        var request = HttpRequest.builder()
            .uri(URI.create(this.connectUrl + "/connectors/" + connectorName + "/stop"))
            .method("PUT")
            .build();

        try (var client = HttpClient.builder().runContext(runContext).build()) {
            client.request(request, String.class);
        }
    }

    private String waitForFileContent(Path file, String expected) throws Exception {
        for (int attempt = 0; attempt < 30; attempt++) {
            if (Files.exists(file)) {
                var content = Files.readString(file, StandardCharsets.UTF_8);
                if (content.contains(expected)) {
                    return content;
                }
            }
            Thread.sleep(1000);
        }

        throw new AssertionError("File '" + file + "' never contained the expected content");
    }

    @Test
    @Order(1)
    void shouldListNoConnectorsInitially() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        ConnectorList.Output output = ConnectorList.builder().connectUrl(connectUrl()).build().run(runContext);

        assertThat(output.getConnectorNames(), is(empty()));
        assertThat(output.getConnectors(), is(empty()));
    }

    @Test
    @Order(2)
    void sourceConnectorLifecycle() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String connectorName = "tu_connect_src_" + IdUtils.create();
        String topic = connectorName + "_topic";
        Path sourceFile = Path.of(dataDir, connectorName + "_source.txt");
        Files.writeString(sourceFile, "line1\nline2\n");

        Map<String, String> config = Map.of(
            "connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max", "1",
            "file", "/data/" + sourceFile.getFileName(),
            "topic", topic
        );

        ConnectorCreate create = ConnectorCreate.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue(connectorName))
            .config(Property.ofValue(config))
            .build();
        ConnectorCreate.Output createOutput = create.run(runContext);

        assertThat(createOutput.getConnectorName(), is(connectorName));
        assertThat(createOutput.getType(), is("source"));
        assertThat(createOutput.getConfig().get("connector.class"), is("org.apache.kafka.connect.file.FileStreamSourceConnector"));

        try {
            var runningStatus = waitForState(runContext, connectorName, "RUNNING");
            assertThat(runningStatus.getTasks(), hasSize(1));
            assertThat(runningStatus.getTasks().getFirst().getState(), is("RUNNING"));

            ConnectorPause.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build().run(runContext);
            waitForState(runContext, connectorName, "PAUSED");

            ConnectorResume.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build().run(runContext);
            waitForState(runContext, connectorName, "RUNNING");

            ConnectorRestart.Output restartOutput = ConnectorRestart.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(connectorName))
                .includeTasks(Property.ofValue(true))
                .build()
                .run(runContext);
            assertThat(restartOutput.getConnectorName(), is(connectorName));
            assertThat(restartOutput.getIncludeTasks(), is(true));
            waitForState(runContext, connectorName, "RUNNING");

            ConnectorGetConfig.Output configOutput = ConnectorGetConfig.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(connectorName))
                .build()
                .run(runContext);
            Map<String, String> rConfig = runContext.render(configOutput.getConfig()).asMap(String.class, String.class);
            assertThat(rConfig.get("topic"), is(topic));

            var updatedConfig = new HashMap<>(rConfig);
            updatedConfig.put("tasks.max", "1");
            ConnectorUpdateConfig.Output updateOutput = ConnectorUpdateConfig.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(connectorName))
                .config(Property.ofValue(updatedConfig))
                .build()
                .run(runContext);
            assertThat(updateOutput.getConnectorName(), is(connectorName));
            waitForState(runContext, connectorName, "RUNNING");

            ConnectorList.Output listOutput = ConnectorList.builder()
                .connectUrl(connectUrl())
                .expandStatus(Property.ofValue(true))
                .build()
                .run(runContext);
            assertThat(listOutput.getConnectorNames(), hasItem(connectorName));
            assertThat(
                listOutput.getConnectors().stream().anyMatch(c -> c.getConnectorName().equals(connectorName)),
                is(true)
            );

            ConnectorGetOffsets.Output offsetsOutput = null;
            for (int attempt = 0; attempt < 15 && (offsetsOutput == null || offsetsOutput.getOffsets().isEmpty()); attempt++) {
                offsetsOutput = ConnectorGetOffsets.builder()
                    .connectUrl(connectUrl())
                    .connectorName(Property.ofValue(connectorName))
                    .build()
                    .run(runContext);
                if (offsetsOutput.getOffsets().isEmpty()) {
                    Thread.sleep(1000);
                }
            }
            assertThat(offsetsOutput.getOffsets(), not(empty()));

            // the connector is still RUNNING (not STOPPED), so Connect must reject the alter — verbatim, not pre-validated client-side
            var alterOffsets = ConnectorAlterOffsets.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(connectorName))
                .offsets(Property.ofValue(offsetsOutput.getOffsets()))
                .build();
            KafkaConnectApiException alterException = assertThrows(KafkaConnectApiException.class, () -> alterOffsets.run(runContext));
            assertThat(alterException.getStatusCode(), is(400));
            assertThat(alterException.getResponseBody(), containsString("STOPPED"));
        } finally {
            ConnectorDelete.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build().run(runContext);
            Files.deleteIfExists(sourceFile);
        }

        ConnectorList.Output afterDelete = ConnectorList.builder().connectUrl(connectUrl()).build().run(runContext);
        assertThat(afterDelete.getConnectorNames(), not(hasItem(connectorName)));
    }

    @Test
    @Order(3)
    void sinkConnectorRoundTrip() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String connectorName = "tu_connect_sink_" + IdUtils.create();
        String topic = connectorName + "_topic";
        Path sinkFile = Path.of(dataDir, connectorName + "_sink.txt");

        Produce.builder()
            .id("produce")
            .type(Produce.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keySerializer(Property.ofValue(SerdeType.STRING))
            .valueSerializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue(topic))
            .from(List.of(Map.of("value", "hello-connect")))
            .build()
            .run(runContext);

        Map<String, String> config = Map.of(
            "connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max", "1",
            "file", "/data/" + sinkFile.getFileName(),
            "topics", topic,
            "key.converter", "org.apache.kafka.connect.storage.StringConverter",
            "value.converter", "org.apache.kafka.connect.storage.StringConverter"
        );

        ConnectorCreate.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue(connectorName))
            .config(Property.ofValue(config))
            .build()
            .run(runContext);

        try {
            waitForState(runContext, connectorName, "RUNNING");
            var content = waitForFileContent(sinkFile, "hello-connect");
            assertThat(content, containsString("hello-connect"));
        } finally {
            ConnectorDelete.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build().run(runContext);
            Files.deleteIfExists(sinkFile);
        }
    }

    @Test
    @Order(4)
    void shouldFailToCreateDuplicateConnector() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String connectorName = "tu_connect_dup_" + IdUtils.create();

        Map<String, String> config = Map.of(
            "connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max", "1",
            "file", "/data/does-not-need-to-exist-for-create.txt",
            "topic", connectorName + "_topic"
        );

        ConnectorCreate create = ConnectorCreate.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue(connectorName))
            .config(Property.ofValue(config))
            .build();
        create.run(runContext);

        try {
            KafkaConnectApiException exception = assertThrows(KafkaConnectApiException.class, () -> create.run(runContext));
            assertThat(exception.getStatusCode(), is(409));
            assertThat(exception.getResponseBody(), containsString(connectorName));
        } finally {
            ConnectorDelete.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build().run(runContext);
        }
    }

    @Test
    @Order(5)
    void shouldRejectEmptyConfigOnCreate() {
        RunContext runContext = runContextFactory.of(Map.of());

        ConnectorCreate create = ConnectorCreate.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue("tu_connect_empty_" + IdUtils.create()))
            .config(Property.ofValue(Map.of()))
            .build();

        KafkaConnectApiException exception = assertThrows(KafkaConnectApiException.class, () -> create.run(runContext));
        assertThat(exception.getStatusCode(), is(400));
    }

    @Test
    @Order(6)
    void shouldReturn404ForUnknownConnector() {
        RunContext runContext = runContextFactory.of(Map.of());
        String connectorName = "tu_connect_missing_" + IdUtils.create();

        ConnectorGetStatus task = ConnectorGetStatus.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue(connectorName))
            .build();

        KafkaConnectApiException exception = assertThrows(KafkaConnectApiException.class, () -> task.run(runContext));
        assertThat(exception.getStatusCode(), is(404));
        assertThat(exception.getMessage(), containsString(connectorName));
    }

    @Test
    @Order(7)
    void connectorOffsetsLifecycleWhenStopped() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String connectorName = "tu_connect_offsets_" + IdUtils.create();
        Path sourceFile = Path.of(dataDir, connectorName + "_source.txt");
        Files.writeString(sourceFile, "line1\nline2\n");

        Map<String, String> config = Map.of(
            "connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max", "1",
            "file", "/data/" + sourceFile.getFileName(),
            "topic", connectorName + "_topic"
        );

        ConnectorCreate.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue(connectorName))
            .config(Property.ofValue(config))
            .build()
            .run(runContext);

        try {
            waitForState(runContext, connectorName, "RUNNING");

            ConnectorGetOffsets.Output offsetsOutput = null;
            for (int attempt = 0; attempt < 15 && (offsetsOutput == null || offsetsOutput.getOffsets().isEmpty()); attempt++) {
                offsetsOutput = ConnectorGetOffsets.builder()
                    .connectUrl(connectUrl())
                    .connectorName(Property.ofValue(connectorName))
                    .build()
                    .run(runContext);
                if (offsetsOutput.getOffsets().isEmpty()) {
                    Thread.sleep(1000);
                }
            }
            assertThat(offsetsOutput.getOffsets(), not(empty()));

            stopConnector(runContext, connectorName);
            waitForConnectorState(runContext, connectorName, "STOPPED", false);

            ConnectorAlterOffsets.Output alterOutput = ConnectorAlterOffsets.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(connectorName))
                .offsets(Property.ofValue(offsetsOutput.getOffsets()))
                .build()
                .run(runContext);
            assertThat(alterOutput.getConnectorName(), is(connectorName));

            ConnectorResetOffsets.Output resetOutput = ConnectorResetOffsets.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(connectorName))
                .build()
                .run(runContext);
            assertThat(resetOutput.getConnectorName(), is(connectorName));
        } finally {
            ConnectorDelete.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(connectorName)).build().run(runContext);
            Files.deleteIfExists(sourceFile);
        }
    }

    @Test
    @Order(8)
    void shouldCloneConnectorConfigIntoDifferentlyNamedConnector() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String originalName = "tu_connect_clone_src_" + IdUtils.create();
        String cloneName = "tu_connect_clone_dst_" + IdUtils.create();
        Path sourceFile = Path.of(dataDir, originalName + "_source.txt");
        Files.writeString(sourceFile, "line1\n");

        Map<String, String> config = Map.of(
            "connector.class", "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max", "1",
            "file", "/data/" + sourceFile.getFileName(),
            "topic", originalName + "_topic"
        );

        ConnectorCreate.builder()
            .connectUrl(connectUrl())
            .connectorName(Property.ofValue(originalName))
            .config(Property.ofValue(config))
            .build()
            .run(runContext);

        try {
            waitForState(runContext, originalName, "RUNNING");

            ConnectorGetConfig.Output configOutput = ConnectorGetConfig.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(originalName))
                .build()
                .run(runContext);

            // Connect's GET .../config response embeds the original connector's own "name" in the config
            // map — piping it straight into ConnectorCreate under a different connectorName must not 400.
            ConnectorCreate.Output cloneOutput = ConnectorCreate.builder()
                .connectUrl(connectUrl())
                .connectorName(Property.ofValue(cloneName))
                .config(configOutput.getConfig())
                .build()
                .run(runContext);

            assertThat(cloneOutput.getConnectorName(), is(cloneName));
            assertThat(cloneOutput.getConfig().get("name"), is(cloneName));
            waitForState(runContext, cloneName, "RUNNING");
        } finally {
            ConnectorDelete.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(originalName)).build().run(runContext);
            ConnectorDelete.builder().connectUrl(connectUrl()).connectorName(Property.ofValue(cloneName)).build().run(runContext);
            Files.deleteIfExists(sourceFile);
        }
    }

    @Test
    @Order(9)
    void shouldFailClearlyWhenWorkerUnreachable() {
        RunContext runContext = runContextFactory.of(Map.of());

        ConnectorList task = ConnectorList.builder().connectUrl(Property.ofValue("http://localhost:1")).build();

        KafkaConnectApiException exception = assertThrows(KafkaConnectApiException.class, () -> task.run(runContext));
        assertThat(exception.getStatusCode(), is(-1));
        assertThat(exception.getMessage(), containsString("Unable to reach the Kafka Connect worker"));
    }
}

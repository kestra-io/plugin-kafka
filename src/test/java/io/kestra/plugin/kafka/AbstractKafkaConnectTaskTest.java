package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pure unit tests for {@link AbstractKafkaConnectTask} logic that doesn't require a live Kafka Connect worker.
 */
@KestraTest
class AbstractKafkaConnectTaskTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldRedactSecretValueEmbeddedInFreeTextMessage() {
        var body = "{\"error_code\":400,\"message\":\"Connector configuration is invalid and contains the following 1 error(s):\\nInvalid value s3cr3t-p4ssw0rd for configuration connection.password: ...\"}";
        var submittedConfig = Map.of("connection.password", "s3cr3t-p4ssw0rd");

        var redacted = AbstractKafkaConnectTask.redactSecrets(body, submittedConfig);

        assertThat(redacted, not(containsString("s3cr3t-p4ssw0rd")));
        assertThat(redacted, containsString("***REDACTED***"));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenOnlyUsernameIsSet() {
        RunContext runContext = runContextFactory.of(Map.of());

        ConnectorList task = ConnectorList.builder()
            .connectUrl(Property.ofValue("http://localhost:8083"))
            .username(Property.ofValue("alice"))
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("password"));
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenParsingBlankBody() {
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> AbstractKafkaConnectTask.parse("", AbstractKafkaConnectTask.ConnectorInfoResponse.class)
        );

        assertThat(exception.getMessage(), containsString("ConnectorInfoResponse"));
    }
}

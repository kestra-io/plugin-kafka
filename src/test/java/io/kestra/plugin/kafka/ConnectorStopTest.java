package io.kestra.plugin.kafka;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ConnectorStopTest {

    @Inject
    private RunContextFactory runContextFactory;

    private WireMockServer connect;

    @BeforeEach
    void setUp() {
        connect = new WireMockServer(0);
        connect.start();
    }

    @AfterEach
    void tearDown() {
        connect.stop();
    }

    @Test
    void shouldAcceptEmptyResponseWithoutAuthentication() throws Exception {
        var runContext = runContextFactory.of(Map.of());
        connect.stubFor(put(urlEqualTo("/connectors/orders/stop"))
            .willReturn(aResponse().withStatus(202)));

        var output = ConnectorStop.builder()
            .connectUrl(Property.ofValue(connect.baseUrl()))
            .connectorName(Property.ofValue("orders"))
            .build()
            .run(runContext);

        assertThat(output.getConnectorName(), is("orders"));
        connect.verify(putRequestedFor(urlEqualTo("/connectors/orders/stop"))
            .withHeader("Authorization", absent()));
    }

    @Test
    void shouldRenderAndEncodeConnectorNameWithAuthenticationAndHeaders() throws Exception {
        var connectorName = "orders / east";
        var runContext = runContextFactory.of(Map.of("vars", Map.of("connectorName", connectorName)));
        connect.stubFor(put(urlEqualTo("/connectors/orders%20%2F%20east/stop"))
            .withBasicAuth("alice", "secret")
            .withHeader("X-Connect-Cluster", equalTo("east"))
            .willReturn(aResponse().withStatus(204)));

        var output = ConnectorStop.builder()
            .connectUrl(Property.ofValue(connect.baseUrl() + "/"))
            .connectorName(Property.ofExpression("{{ vars.connectorName }}"))
            .username(Property.ofValue("alice"))
            .password(Property.ofValue("secret"))
            .headers(Property.ofValue(Map.of("X-Connect-Cluster", "east")))
            .build()
            .run(runContext);

        assertThat(output.getConnectorName(), is(connectorName));
        connect.verify(putRequestedFor(urlEqualTo("/connectors/orders%20%2F%20east/stop")));
    }

    @ParameterizedTest
    @ValueSource(ints = {404, 409, 500})
    void shouldSurfaceConnectErrors(int status) {
        var runContext = runContextFactory.of(Map.of());
        var body = "{\"error_code\":" + status + ",\"message\":\"Cannot stop orders\"}";
        connect.stubFor(put(urlEqualTo("/connectors/orders/stop"))
            .willReturn(aResponse().withStatus(status).withBody(body)));

        var task = ConnectorStop.builder()
            .connectUrl(Property.ofValue(connect.baseUrl()))
            .connectorName(Property.ofValue("orders"))
            .build();

        var exception = assertThrows(KafkaConnectApiException.class, () -> task.run(runContext));
        assertThat(exception.getStatusCode(), is(status));
        assertThat(exception.getResponseBody(), is(body));
        assertThat(exception.getMessage(), containsString("orders"));
        if (status == 404) {
            assertThat(exception.getMessage(), containsString("upgrade Kafka Connect to 3.5 or later"));
            assertThat(exception.getMessage(), containsString(connect.baseUrl()));
            assertThat(exception.getCause() instanceof KafkaConnectApiException, is(true));
        }
    }
}

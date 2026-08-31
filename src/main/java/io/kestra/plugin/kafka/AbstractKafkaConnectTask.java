package io.kestra.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientRequestException;
import io.kestra.core.http.client.configurations.BasicAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractKafkaConnectTask extends Task implements KafkaConnectConnectionInterface {
    @NotNull
    @PluginProperty(group = "connection")
    protected Property<String> connectUrl;

    @PluginProperty(group = "connection")
    protected Property<String> username;

    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> password;

    @PluginProperty(group = "connection")
    protected Property<Map<String, String>> headers;

    protected String renderConnectUrl(RunContext runContext) throws IllegalVariableEvaluationException {
        var rConnectUrl = requireRendered(runContext, this.connectUrl, String.class, "connectUrl");
        return rConnectUrl.endsWith("/") ? rConnectUrl.substring(0, rConnectUrl.length() - 1) : rConnectUrl;
    }

    /**
     * Renders a required property, failing with a message naming the missing field instead of an opaque
     * {@code NoSuchElementException}.
     */
    protected static <T> T requireRendered(RunContext runContext, Property<T> property, Class<T> type, String fieldName) throws IllegalVariableEvaluationException {
        return runContext.render(property).as(type)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property '" + fieldName + "'"));
    }

    protected static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    protected HttpRequest.HttpRequestBuilder requestBuilder(RunContext runContext, String method, String path) throws IllegalVariableEvaluationException {
        var uri = URI.create(renderConnectUrl(runContext) + path);
        var builder = HttpRequest.builder().uri(uri).method(method);
        var rHeaders = runContext.render(this.headers).asMap(String.class, String.class);
        rHeaders.forEach(builder::addHeader);
        return builder;
    }

    /**
     * Executes a request against the Connect REST API and returns the raw response, mapping an unreachable
     * worker or a non-2xx status to a {@link KafkaConnectApiException} carrying the API's error body verbatim.
     * {@code connectorName} names the connector in a 404 message when the call is connector-scoped; pass
     * {@code null} for connector-agnostic calls (e.g. {@code ConnectorCreate}, {@code ConnectorList}).
     */
    protected HttpResponse<String> execute(RunContext runContext, HttpRequest request, String connectorName) throws Exception {
        try (var client = buildClient(runContext)) {
            var response = client.request(request, String.class);
            var status = response.getStatus().getCode();

            if (status == 404 && connectorName != null) {
                throw new KafkaConnectApiException(
                    "Connector '" + connectorName + "' was not found on the Kafka Connect worker at " + renderConnectUrl(runContext)
                        + " — check the connector name or that it hasn't already been deleted",
                    status,
                    response.getBody()
                );
            }

            if (status >= 300) {
                throw new KafkaConnectApiException(
                    "Kafka Connect API call " + request.getMethod() + " " + request.getUri() + " failed with status " + status
                        + (isBlank(response.getBody()) ? "" : ": " + response.getBody()),
                    status,
                    response.getBody()
                );
            }

            return response;
        } catch (HttpClientRequestException e) {
            throw new KafkaConnectApiException(
                "Unable to reach the Kafka Connect worker at " + renderConnectUrl(runContext)
                    + " — check the `connectUrl` property and that the worker is reachable: " + e.getMessage(),
                -1,
                null,
                e
            );
        }
    }

    private HttpClient buildClient(RunContext runContext) throws IllegalVariableEvaluationException {
        var configurationBuilder = HttpConfiguration.builder()
            // status codes are inspected manually in `execute` so the raw error body can be surfaced verbatim
            .allowFailed(Property.ofValue(true));

        var rUsername = runContext.render(this.username).as(String.class);
        var rPassword = runContext.render(this.password).as(String.class);
        if (rUsername.isPresent() && rPassword.isPresent()) {
            configurationBuilder.auth(BasicAuthConfiguration.builder()
                .username(Property.ofValue(rUsername.get()))
                .password(Property.ofValue(rPassword.get()))
                .build());
        }

        return HttpClient.builder().runContext(runContext).configuration(configurationBuilder.build()).build();
    }

    protected static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    protected static Map<String, String> parseStringMap(String body) throws JsonProcessingException {
        return parseOrDefault(body, new TypeReference<Map<String, String>>() {}, Map.of());
    }

    protected static Map<String, Object> parseMap(String body) throws JsonProcessingException {
        return parseOrDefault(body, new TypeReference<Map<String, Object>>() {}, Map.of());
    }

    protected static List<String> parseListOfStrings(String body) throws JsonProcessingException {
        return parseOrDefault(body, new TypeReference<List<String>>() {}, List.of());
    }

    private static <T> T parseOrDefault(String body, TypeReference<T> typeReference, T emptyDefault) throws JsonProcessingException {
        if (isBlank(body)) {
            return emptyDefault;
        }
        return JacksonMapper.ofJson().readValue(body, typeReference);
    }

    protected static <T> T parse(String body, Class<T> type) throws JsonProcessingException {
        return JacksonMapper.ofJson().readValue(body, type);
    }

    /**
     * Raw shape of a connector "info" response, returned by {@code POST /connectors} and {@code PUT /connectors/{name}/config}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class ConnectorInfoResponse {
        public String name;
        public Map<String, String> config;
        public List<ConnectorTaskReference> tasks;
        public String type;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class ConnectorTaskReference {
        public String connector;
        public Integer task;
    }

    /**
     * Raw shape of a connector "status" response, returned by {@code GET /connectors/{name}/status} and,
     * nested under each connector name, by {@code GET /connectors?expand=status}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class ConnectorStatusPayload {
        public String name;
        public ConnectorStateInfo connector;
        public List<ConnectorTaskState> tasks;
        public String type;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class ConnectorStateInfo {
        public String state;
        @JsonProperty("worker_id")
        public String workerId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    protected static class ConnectorTaskState {
        public Integer id;
        public String state;
        @JsonProperty("worker_id")
        public String workerId;
        public String trace;
    }
}

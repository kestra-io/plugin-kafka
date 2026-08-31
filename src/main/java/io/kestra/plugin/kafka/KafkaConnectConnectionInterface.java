package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public interface KafkaConnectConnectionInterface {
    @Schema(
        title = "Kafka Connect REST API base URL",
        description = "For example `http://connect:8083`. Kafka Connect has no dedicated Java admin client — every operation goes through this REST API."
    )
    @NotNull
    @PluginProperty(group = "connection")
    Property<String> getConnectUrl();

    @Schema(
        title = "Basic auth username",
        description = "Required together with `password` when the Connect REST API is protected with HTTP basic auth. Leave both unset to call an unauthenticated worker — no `Authorization` header is sent in that case."
    )
    @PluginProperty(group = "connection")
    Property<String> getUsername();

    @Schema(title = "Basic auth password")
    @PluginProperty(group = "connection", secret = true)
    Property<String> getPassword();

    @Schema(
        title = "Additional HTTP headers",
        description = "Sent on every request to the Connect REST API. Useful when the worker sits behind a reverse proxy or expects a bearer token, e.g. `Authorization: Bearer ...`."
    )
    @PluginProperty(group = "connection")
    Property<Map<String, String>> getHeaders();
}

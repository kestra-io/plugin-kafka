package io.kestra.plugin.kafka.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TraceContext {
    @Schema(
        title = "Whether to extract trace context from the Kafka message and propagate it to the execution",
        description = "Disabled by default. When enabled, W3C traceparent (and tracestate) will be extracted."
    )
    @Builder.Default
    @PluginProperty
    private Property<Boolean> enabled = Property.ofValue(false);

    @Schema(
        title = "Kafka header name for the W3C traceparent",
        description = "Defaults to `traceparent`."
    )
    @Builder.Default
    @PluginProperty
    private Property<String> traceparentHeader = Property.ofValue("traceparent");

    @Schema(
        title = "Kafka header name for the W3C tracestate",
        description = "Defaults to `tracestate`."
    )
    @Builder.Default
    @PluginProperty
    private Property<String> tracestateHeader = Property.ofValue("tracestate");

    @Schema(
        title = "Optional field path to extract the traceparent from the message body (e.g. `traceparent` or `trace.parent`)",
        description = "Supported when message value is a JSON string or a Map."
    )
    @PluginProperty
    private Property<String> bodyExtractor;
}

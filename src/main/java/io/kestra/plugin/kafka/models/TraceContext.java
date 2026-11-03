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
        description = """
            Disabled by default. When enabled, W3C traceparent will be extracted and propagated to the execution.
            Note: Propagation takes effect on Kestra 1.3.x; support on Kestra 2.0.x is pending a core update.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> enabled = Property.ofValue(false);

    @Schema(
        title = "Kafka header name for the W3C traceparent",
        description = """
            Defaults to `traceparent`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<String> traceparentHeader = Property.ofValue("traceparent");

    @Schema(
        title = "Optional field path to extract the traceparent from the message body (e.g. `traceparent` or `trace.parent`)",
        description = """
            Supported when message value is a JSON string or a Map.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> bodyExtractor;
}

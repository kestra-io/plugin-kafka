package io.kestra.plugin.kafka;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;
public interface KafkaConnectionInterface {
    @Schema(
        title = "Kafka client properties",
        description = """
            Must include `bootstrap.servers`; accepts any Kafka [consumer](https://kafka.apache.org/documentation/#consumerconfigs) or [producer](https://kafka.apache.org/documentation/#producerconfigs) config. Provide base64-encoded content for `ssl.keystore.location` and `ssl.truststore.location` when using SSL.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Map<String, String>> getProperties();

    @Schema(
        title="Serializer or deserializer properties",
        description = "Passed to serdes; `avro.use.logical.type.converters` is forced to true by default."
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, String>> getSerdeProperties();
}

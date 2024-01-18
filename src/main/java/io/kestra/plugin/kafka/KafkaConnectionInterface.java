package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;
import javax.validation.constraints.NotNull;
public interface KafkaConnectionInterface {
    @Schema(
        title = "Kafka connection properties.",
        description = "The `bootstrap.servers` property is a minimal required configuration to connect to a Kafka topic.\n" +
            "This property can reference any valid [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs) or " +
            "[Producer Configs\n](https://kafka.apache.org/documentation/#producerconfigs) as key-value pairs.\n\n" +
            "If you want to pass a truststore or a keystore, you must provide a base64 encoded string for `ssl.keystore.location` and `ssl.truststore.location`."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    Map<String, String> getProperties();

    @Schema(
        title="Serializer configuration",
        description = "Configuration that will be passed to serializer or deserializer. The `avro.use.logical.type.converters` is always passed when you have any values set to `true`."
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getSerdeProperties();
}

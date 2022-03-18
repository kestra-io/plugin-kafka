package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Map;
import javax.validation.constraints.NotNull;
public interface KafkaConnectionInterface {
    @Schema(
        title = "Connection properties",
        description = "`bootstrap.servers` is a minimal required configuration.\n" +
            "Can be any valid [Consumer Configs](https://kafka.apache.org/documentation/#consumerconfigs) or " +
            "[Producer Configs\n](https://kafka.apache.org/documentation/#producerconfigs) "
    )
    @PluginProperty(dynamic = true)
    @NotNull
    Map<String, String> getProperties();

    @Schema(
        title="Serializer configuration",
        description = "Configuration that will be passed to serializer or deserializer, you typically may need to use ``\n" +
            "`avro.use.logical.type.converters` is always passed with `true` value."
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getSerdeProperties();
}

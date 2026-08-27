package io.kestra.plugin.kafka;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import org.apache.kafka.common.config.SslConfigs;

import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

/**
 * Shared builder for Kafka client {@link Properties} (producer, consumer or admin), used by both
 * the data-plane tasks ({@link AbstractKafkaConnection}) and the control-plane admin tasks.
 */
public final class KafkaClientProperties {

    private KafkaClientProperties() {
    }

    public static Properties create(Property<Map<String, String>> mapProperties, RunContext runContext) throws Exception {
        Properties properties = new Properties();
        final Map<String, String> renderedMapProperties = runContext.render(mapProperties).asMap(String.class, String.class);
        renderedMapProperties
            .forEach(throwBiConsumer((key, value) -> {
                if (key.equals(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) || key.equals(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)) {
                    Path path = runContext.workingDir().createTempFile(Base64.getDecoder().decode(value.replace("\n", "")));
                    properties.put(key, path.toAbsolutePath().toString());
                } else {
                    properties.put(key, value);
                }
            }));

        return properties;
    }
}

package io.kestra.plugin.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.serialization.*;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractKafkaConnection extends Task {
    @Schema(
        title = "Connection properties"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected Map<String, String> properties;

    @Schema(
        title="Serializer configuration"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    protected Map<String, String> serdeProperties = Collections.emptyMap();

    protected Properties createProperties(Map<String, String> mapProperties, RunContext runContext) throws Exception {
        Properties properties = new Properties();

        mapProperties
            .forEach(throwBiConsumer((key, value) -> properties.put(key, runContext.render(value))));

        return properties;
    }

    protected Serializer<?> getTypedSerializer(SerdeType s) throws Exception {
        switch (s) {
            case STRING:
                return new StringSerializer();
            case INTEGER:
                return new IntegerSerializer();
            case FLOAT:
                return new FloatSerializer();
            case DOUBLE:
                return new DoubleSerializer();
            case LONG:
                return new LongSerializer();
            case SHORT:
                return new ShortSerializer();
            case BYTE_ARRAY:
                return new ByteArraySerializer();
            case BYTE_BUFFER:
                return new ByteBufferSerializer();
            case BYTES:
                return new BytesSerializer();
            case UUID:
                return new UUIDSerializer();
            case VOID:
                return new VoidSerializer();
            case AVRO:
                return new KafkaAvroSerializer();
            case JSON:
                return new KafkaJsonSerializer<>();
            default:
                throw new Exception();
        }
    }
}

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

import javax.validation.constraints.NotNull;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractKafkaConnection extends Task {
        @Schema(
            title="Connection properties"
        )
        @PluginProperty(dynamic = true)
        @NotNull
        protected Map<String, String> properties;

        @Schema(
            title="Serializer configuration"
        )
        @PluginProperty(dynamic = true)
        @Builder.Default
        protected Map<String, String> serializerConfig = Collections.emptyMap();


        protected enum SerializerType {
            String,
            Integer,
            Float,
            Double,
            Long,
            Short,
            ByteArray,
            ByteBuffer,
            Bytes,
            UUID,
            Void,
            AVRO,
            JSON
        }

    protected Properties createProperties(Map<String,String> mapProperties, RunContext runContext) throws Exception {
        Properties properties = new Properties();

        mapProperties
            .forEach(throwBiConsumer((key, value) -> properties.put(key, runContext.render(value))));

        return properties;
    }

    protected Serializer getTypedSerializer(SerializerType s) throws Exception{
        switch(s){
            case String:
                return new StringSerializer();
            case Integer:
                return new IntegerSerializer();
            case Float:
                return new FloatSerializer();
            case Double:
                return new DoubleSerializer();
            case Long:
                return new LongSerializer();
            case Short:
                return new ShortSerializer();
            case ByteArray:
                return new ByteArraySerializer();
            case ByteBuffer:
                return new ByteBufferSerializer();
            case Bytes:
                return new BytesSerializer();
            case UUID:
                return new UUIDSerializer();
            case Void:
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
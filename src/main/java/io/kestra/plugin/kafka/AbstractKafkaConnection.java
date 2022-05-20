package io.kestra.plugin.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.GenericRecordToMapDeserializer;
import io.kestra.plugin.kafka.serdes.KafkaAvroSerializer;
import io.kestra.plugin.kafka.serdes.SerdeType;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.*;

import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractKafkaConnection extends Task implements KafkaConnectionInterface {
    protected Map<String, String> properties;

    @Builder.Default
    protected Map<String, String> serdeProperties = Collections.emptyMap();

    protected static Properties createProperties(Map<String, String> mapProperties, RunContext runContext) throws Exception {
        Properties properties = new Properties();

        mapProperties
            .forEach(throwBiConsumer((key, value) -> {
                if (key.equals(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG) || key.equals(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)) {
                    Path path = runContext.tempFile(Base64.getDecoder().decode(runContext.render(value).replace("\n", "")));
                    properties.put(key, path.toAbsolutePath().toString());
                } else {
                    properties.put(key, runContext.render(value));
                }
            }));

        return properties;
    }

    protected static Serializer<?> getTypedSerializer(SerdeType s) throws Exception {
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

    protected static Deserializer<?> getTypedDeserializer(SerdeType s) throws Exception {
        switch (s) {
            case STRING:
                return new StringDeserializer();
            case INTEGER:
                return new IntegerDeserializer();
            case FLOAT:
                return new FloatDeserializer();
            case DOUBLE:
                return new DoubleDeserializer();
            case LONG:
                return new LongDeserializer();
            case SHORT:
                return new ShortDeserializer();
            case BYTE_ARRAY:
                return new ByteArrayDeserializer();
            case BYTE_BUFFER:
                return new ByteBufferDeserializer();
            case BYTES:
                return new BytesDeserializer();
            case UUID:
                return new UUIDDeserializer();
            case VOID:
                return new VoidDeserializer();
            case AVRO:
                return new GenericRecordToMapDeserializer(new KafkaAvroDeserializer());
            case JSON:
                return new KafkaJsonDeserializer<>();
            default:
                throw new Exception();
        }
    }
}

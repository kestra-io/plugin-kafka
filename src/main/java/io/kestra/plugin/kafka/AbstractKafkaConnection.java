package io.kestra.plugin.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.GenericRecordToMapDeserializer;
import io.kestra.plugin.kafka.serdes.KafkaAvroSerializer;
import io.kestra.plugin.kafka.serdes.MapToGenericRecordSerializer;
import io.kestra.plugin.kafka.serdes.SerdeType;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.utils.Rethrow.throwBiConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractKafkaConnection extends Task implements KafkaConnectionInterface {
    @NotNull
    protected Property<Map<String, String>> properties;

    @Builder.Default
    protected Property<Map<String, String>> serdeProperties = Property.ofValue(new HashMap<>());

    @Getter(AccessLevel.NONE)
    protected AtomicReference<Object> dataOnSerdeError = new AtomicReference<>();

    protected static Properties createProperties(Property<Map<String, String>> mapProperties, RunContext runContext) throws Exception {
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

    protected static Serializer<?> getTypedSerializer(SerdeType s, @Nullable AvroSchema avroSchema) {
        return switch (s) {
            case STRING -> new StringSerializer();
            case INTEGER -> new IntegerSerializer();
            case FLOAT -> new FloatSerializer();
            case DOUBLE -> new DoubleSerializer();
            case LONG -> new LongSerializer();
            case SHORT -> new ShortSerializer();
            case BYTE_ARRAY -> new ByteArraySerializer();
            case BYTE_BUFFER -> new ByteBufferSerializer();
            case BYTES -> new BytesSerializer();
            case UUID -> new UUIDSerializer();
            case VOID -> new VoidSerializer();
            case AVRO -> new MapToGenericRecordSerializer(new KafkaAvroSerializer(), avroSchema);
            case JSON -> new KafkaJsonSerializer<>();
        };
    }

    protected static Deserializer<?> getTypedDeserializer(SerdeType s) {
        return switch (s) {
            case STRING -> new StringDeserializer();
            case INTEGER -> new IntegerDeserializer();
            case FLOAT -> new FloatDeserializer();
            case DOUBLE -> new DoubleDeserializer();
            case LONG -> new LongDeserializer();
            case SHORT -> new ShortDeserializer();
            case BYTE_ARRAY -> new ByteArrayDeserializer();
            case BYTE_BUFFER -> new ByteBufferDeserializer();
            case BYTES -> new BytesDeserializer();
            case UUID -> new UUIDDeserializer();
            case VOID -> new VoidDeserializer();
            case AVRO -> new GenericRecordToMapDeserializer(new KafkaAvroDeserializer());
            case JSON -> new KafkaJsonDeserializer<>() {
                @Override
                public Object deserialize(String ignored, byte[] bytes) {
                    try {
                        return super.deserialize(ignored, bytes);
                    }  catch (SerializationException e) {
                        throw new PluginKafkaSerdeException(e, new String(bytes));
                    }
                }
            };
        };
    }

    @Getter
    protected static class PluginKafkaSerdeException extends SerializationException {
        private final String data;

        public PluginKafkaSerdeException(SerializationException e, String data) {
            super(e);
            this.data = data;
        }
    }
}

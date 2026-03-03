package io.kestra.plugin.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
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
    @Builder.Default
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
            case PROTOBUF -> new KafkaProtobufSerializer<>();
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
            case PROTOBUF -> new KafkaProtobufDeserializer<>();
        };
    }

    protected static void enrichAwsGlueSerdeProperties(Map<String, Object> serdeProperties) {
        if (!serdeProperties.containsKey("endpoint") && serdeProperties.containsKey("schema.registry.url")) {
            serdeProperties.put("endpoint", serdeProperties.get("schema.registry.url"));
        }

        if (!serdeProperties.containsKey("schema.registry.url") && serdeProperties.containsKey("endpoint")) {
            serdeProperties.put("schema.registry.url", serdeProperties.get("endpoint"));
        }

        // Backward compatibility with existing AWS-prefixed keys.
        if (!serdeProperties.containsKey("endpoint") && serdeProperties.containsKey("aws.glue.endpoint")) {
            serdeProperties.put("endpoint", serdeProperties.get("aws.glue.endpoint"));
        }

        if (!serdeProperties.containsKey("aws.glue.endpoint") && serdeProperties.containsKey("endpoint")) {
            serdeProperties.put("aws.glue.endpoint", serdeProperties.get("endpoint"));
        }

        if (!serdeProperties.containsKey("region") && serdeProperties.containsKey("aws.region")) {
            serdeProperties.put("region", serdeProperties.get("aws.region"));
        }

        if (!serdeProperties.containsKey("aws.region") && serdeProperties.containsKey("region")) {
            serdeProperties.put("aws.region", serdeProperties.get("region"));
        }

        var awsRegion = resolveAwsConfigValue("aws.region", "AWS_REGION");
        awsRegion.ifPresent(value -> {
            serdeProperties.putIfAbsent("aws.region", value);
            serdeProperties.putIfAbsent("region", value);
            System.setProperty("aws.region", value);
        });

        var awsEndpoint = resolveAwsConfigValue("aws.glue.endpoint", "AWS_GLUE_ENDPOINT");
        awsEndpoint.ifPresent(value -> {
            serdeProperties.putIfAbsent("aws.glue.endpoint", value);
            serdeProperties.putIfAbsent("endpoint", value);
        });

        if (!serdeProperties.containsKey("aws.accessKeyId") && serdeProperties.containsKey("aws.access.key.id")) {
            serdeProperties.put("aws.accessKeyId", serdeProperties.get("aws.access.key.id"));
        }

        if (!serdeProperties.containsKey("aws.access.key.id") && serdeProperties.containsKey("aws.accessKeyId")) {
            serdeProperties.put("aws.access.key.id", serdeProperties.get("aws.accessKeyId"));
        }

        var awsAccessKeyId = resolveAwsConfigValue("aws.accessKeyId", "AWS_ACCESS_KEY_ID")
            .or(() -> resolveAwsConfigValue("aws.access.key.id", "AWS_ACCESS_KEY_ID"));
        awsAccessKeyId.ifPresent(value -> {
            serdeProperties.putIfAbsent("aws.accessKeyId", value);
            serdeProperties.putIfAbsent("aws.access.key.id", value);
            System.setProperty("aws.accessKeyId", value);
        });

        if (!serdeProperties.containsKey("aws.secretAccessKey") && serdeProperties.containsKey("aws.secret.access.key")) {
            serdeProperties.put("aws.secretAccessKey", serdeProperties.get("aws.secret.access.key"));
        }

        if (!serdeProperties.containsKey("aws.secret.access.key") && serdeProperties.containsKey("aws.secretAccessKey")) {
            serdeProperties.put("aws.secret.access.key", serdeProperties.get("aws.secretAccessKey"));
        }

        var awsSecretAccessKey = resolveAwsConfigValue("aws.secretAccessKey", "AWS_SECRET_ACCESS_KEY")
            .or(() -> resolveAwsConfigValue("aws.secret.access.key", "AWS_SECRET_ACCESS_KEY"));
        awsSecretAccessKey.ifPresent(value -> {
            serdeProperties.putIfAbsent("aws.secretAccessKey", value);
            serdeProperties.putIfAbsent("aws.secret.access.key", value);
            System.setProperty("aws.secretAccessKey", value);
        });

        // Required by AWS Kafka Avro deserializer for generic records.
        serdeProperties.putIfAbsent("avroRecordType", "GENERIC_RECORD");
    }

    private static Optional<String> resolveAwsConfigValue(String propertyName, String environmentVariableName) {
        var systemProperty = System.getProperty(propertyName);
        if (systemProperty != null && !systemProperty.isBlank()) {
            return Optional.of(systemProperty);
        }

        var systemEnvironmentValue = System.getenv(environmentVariableName);
        if (systemEnvironmentValue != null && !systemEnvironmentValue.isBlank()) {
            return Optional.of(systemEnvironmentValue);
        }

        var uppercaseSystemProperty = System.getProperty(environmentVariableName);
        if (uppercaseSystemProperty != null && !uppercaseSystemProperty.isBlank()) {
            return Optional.of(uppercaseSystemProperty);
        }

        return Optional.empty();
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

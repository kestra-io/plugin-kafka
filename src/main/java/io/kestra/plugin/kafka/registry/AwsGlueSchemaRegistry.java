package io.kestra.plugin.kafka.registry;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.AwsGlueGenericRecordToMapDeserializer;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

@Getter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonDeserialize
@Schema(
    title = "AWS Glue Schema Registry"
)
@Plugin(
    examples = {
        @Example(
            title = "Consume AVRO data from a Kafka topic using AWS Glue Schema Registry",
            full = true,
            code = """
                id: consume-kafka-messages
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.kafka.Consume
                    topic: topic_test
                    properties:
                      bootstrap.servers: localhost:9093
                      auto.offset.reset: earliest
                    pollDuration: PT20S
                    maxRecords: 50
                    keyDeserializer: STRING
                    valueDeserializer: AVRO
                    schemaRegistryVendor:
                      type: io.kestra.plugin.kafka.registry.AwsGlueSchemaRegistry
                      region: us-east-1
                      # endpoint, accessKey, secretKey are optional when using IAM roles
                      accessKey: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                      secretKey: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
                """
        )
    }
)
public class AwsGlueSchemaRegistry extends SchemaRegistryVendor {

    @Schema(title = "The AWS region where the schema registry is located.")
    @NotNull
    private Property<String> region;

    @Schema(title = "The AWS Glue Schema Registry endpoint. Optional when using IAM roles or standard regional endpoints.")
    @Nullable
    private Property<String> endpoint;

    @Schema(title = "The AWS access key ID. Optional when using IAM roles or environment credentials.")
    @Nullable
    private Property<String> accessKey;

    @Schema(title = "The AWS secret access key. Optional when using IAM roles or environment credentials.")
    @Nullable
    private Property<String> secretKey;

    @Override
    public Serializer<?> getSerializer(RunContext runContext, SerdeType valueSerdeType)
        throws IllegalVariableEvaluationException {

        var awsConfig = buildConfig(runContext);
        var credentialsProvider = buildCredentialsProvider(runContext);

        return switch (valueSerdeType) {
            case AVRO -> {
                // Construct with credentials so they are preserved across subsequent configure() calls.
                // The framework will call configure(serdesMap, false) — the delegate merges awsConfig in.
                var delegate = new AWSKafkaAvroSerializer(credentialsProvider, null);
                yield new Serializer<Object>() {
                    @Override
                    public void configure(Map<String, ?> configs, boolean isKey) {
                        // Merge user serdeProperties with our AWS config; AWS config takes precedence
                        var merged = new HashMap<String, Object>(configs);
                        merged.putAll(awsConfig);
                        delegate.configure(merged, isKey);
                    }

                    @Override
                    public byte[] serialize(String topic, Object data) {
                        return delegate.serialize(topic, data);
                    }

                    @Override
                    public byte[] serialize(String topic, Headers headers, Object data) {
                        return delegate.serialize(topic, headers, data);
                    }

                    @Override
                    public void close() {
                        delegate.close();
                    }
                };
            }
            default -> throw new IllegalArgumentException("SerdeType not supported by AWS Glue Schema Registry serializer: " + valueSerdeType + ". Only AVRO is currently supported.");
        };
    }

    @Override
    public Deserializer<?> getDeserializer(RunContext runContext, SerdeType valueSerdeType)
        throws IllegalVariableEvaluationException {

        var awsConfig = buildConfig(runContext);
        var credentialsProvider = buildCredentialsProvider(runContext);

        return switch (valueSerdeType) {
            case AVRO -> {
                // Construct with credentials so they are preserved across subsequent configure() calls.
                var inner = new AWSKafkaAvroDeserializer(credentialsProvider, null);
                yield new AwsGlueGenericRecordToMapDeserializer(inner) {
                    @Override
                    public void configure(Map<String, ?> configs, boolean isKey) {
                        // Merge user serdeProperties with our AWS config; AWS config takes precedence
                        var merged = new HashMap<String, Object>(configs);
                        merged.putAll(awsConfig);
                        super.configure(merged, isKey);
                    }
                };
            }
            default -> throw new IllegalArgumentException("SerdeType not supported by AWS Glue Schema Registry deserializer: " + valueSerdeType + ". Only AVRO is currently supported.");
        };
    }

    private AwsCredentialsProvider buildCredentialsProvider(RunContext runContext)
        throws IllegalVariableEvaluationException {

        var rAccessKey = this.accessKey != null
            ? runContext.render(this.accessKey).as(String.class).orElse(null)
            : null;
        var rSecretKey = this.secretKey != null
            ? runContext.render(this.secretKey).as(String.class).orElse(null)
            : null;

        if (rAccessKey != null && rSecretKey != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(rAccessKey, rSecretKey));
        }

        // Fall back to the default chain (env vars, instance profile, etc.)
        return DefaultCredentialsProvider.create();
    }

    private Map<String, Object> buildConfig(RunContext runContext)
        throws IllegalVariableEvaluationException {

        var rRegion = runContext.render(this.region).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("AWS region must be defined for Glue Schema Registry"));

        Map<String, Object> config = new HashMap<>();
        config.put("region", rRegion);
        config.put("aws.region", rRegion);
        // Required by AWSKafkaAvroDeserializer to return GenericRecord instead of SpecificRecord
        config.put("avroRecordType", "GENERIC_RECORD");

        if (this.endpoint != null) {
            runContext.render(this.endpoint).as(String.class).ifPresent(v -> {
                config.put("endpoint", v);
                config.put("aws.glue.endpoint", v);
            });
        }

        return config;
    }
}

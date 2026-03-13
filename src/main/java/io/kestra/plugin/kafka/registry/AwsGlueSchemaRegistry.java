package io.kestra.plugin.kafka.registry;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

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
            title = "Consume data from a Kafka topic with AWS Glue Schema Registry",
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
                      endpoint: https://glue.us-east-1.amazonaws.com
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

    @Schema(title = "The AWS Glue Schema Registry endpoint.")
    @NotNull
    private Property<String> endpoint;

    @Schema(title = "The AWS secret key to use when connecting to the schema registry.")
    @NotNull
    private Property<String> secretKey;

    @Schema(title = "The AWS access key to use when connecting to the schema registry.")
    @NotNull
    private Property<String> accessKey;

    @Override
    public Serializer<?> getSerializer(RunContext runContext, SerdeType valueSerdeType)
        throws IllegalVariableEvaluationException {

        GlueSchemaRegistryConfiguration configuration = new GlueSchemaRegistryConfiguration(buildConfig(runContext));

        return switch (valueSerdeType) {
            case AVRO -> {
                AWSKafkaAvroSerializer serializer = new AWSKafkaAvroSerializer();
                serializer.configure(configurationToMap(configuration), false);
                yield serializer;
            }
            default -> throw new IllegalArgumentException("SerdeType not supported by AWS Glue: " + valueSerdeType);
        };
    }

    @Override
    public Deserializer<?> getDeserializer(RunContext runContext, SerdeType valueSerdeType)
        throws IllegalVariableEvaluationException {

        GlueSchemaRegistryConfiguration configuration = new GlueSchemaRegistryConfiguration(buildConfig(runContext));

        return switch (valueSerdeType) {
            case AVRO -> {
                AWSKafkaAvroDeserializer deserializer = new AWSKafkaAvroDeserializer();
                deserializer.configure(configurationToMap(configuration), false);
                yield deserializer;
            }
            default -> throw new IllegalArgumentException("SerdeType not supported by AWS Glue: " + valueSerdeType);
        };
    }

    private Map<String, Object> buildConfig(RunContext runContext)
        throws IllegalVariableEvaluationException {

        String rRegion = runContext.render(this.region).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("AWS region must be defined for Glue Schema Registry"));

        String rEndpoint = runContext.render(this.endpoint).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("AWS Glue endpoint must be defined for Glue Schema Registry"));

        String rAccessKey = runContext.render(this.accessKey).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("AWS access key must be defined for Glue Schema Registry"));

        String rSecretKey = runContext.render(this.secretKey).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("AWS secret key must be defined for Glue Schema Registry"));

        Map<String, Object> config = new HashMap<>();
        config.put("region", rRegion);
        config.put("endpoint", rEndpoint);
        // Keep compatibility with previous key names.
        config.put("aws.region", rRegion);
        config.put("aws.glue.endpoint", rEndpoint);
        config.put("aws.access.key.id", rAccessKey);
        config.put("aws.secret.access.key", rSecretKey);

        return config;
    }

    /**
     * AWSKafkaAvroSerializer and AWSKafkaAvroDeserializer need a Map<String, Object>
     * even though Json/Protobuf serializers accept GlueSchemaRegistryConfiguration directly.
     */
    private Map<String, Object> configurationToMap(GlueSchemaRegistryConfiguration configuration) {
        Map<String, Object> map = new HashMap<>();
        map.put("region", configuration.getRegion());
        map.put("endpoint", configuration.getEndPoint());
        // Keep compatibility with previous key names.
        map.put("aws.region", configuration.getRegion());
        map.put("aws.glue.endpoint", configuration.getEndPoint());
        return map;
    }
}

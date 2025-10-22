package io.kestra.plugin.kafka.registry;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.protobuf.Message;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
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
    title = "Confluent Schema Registry"
)
@Plugin(
    examples = {
        @Example(
            title = "Consume data from a Kafka topic with Confluent Schema Registry",
            full = true,
            code = """
                id: consume-kafka-messages
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.kafka.Consume
                    topic: topic_test
                    properties:
                      bootstrap.servers: localhost:9092
                    pollDuration: PT20S
                    maxRecords: 50
                    keyDeserializer: STRING
                    valueDeserializer: AVRO
                    schemaRegistryVendor:
                      type: io.kestra.plugin.kafka.registry.ConfluentSchemaRegistry
                      schemaRegistryUrl: http://localhost:8081
                """
        )
    }
)
public class ConfluentSchemaRegistry extends SchemaRegistryVendor {

    @Schema(title = "The Confluent Schema Registry URL.")
    @NotNull
    private Property<String> schemaRegistryUrl;

    @Override
    public Serializer<?> getSerializer(RunContext runContext, SerdeType serdeType) throws IllegalVariableEvaluationException {
        String rSchemaRegistryUrl = runContext.render(this.schemaRegistryUrl).as(String.class).orElseThrow();

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", rSchemaRegistryUrl);

        return switch (serdeType) {
            case AVRO -> {
                KafkaAvroSerializer serializer = new KafkaAvroSerializer();
                serializer.configure(config, false);
                yield serializer;
            }
            case JSON -> {
                KafkaJsonSerializer<Object> serializer = new KafkaJsonSerializer<>();
                serializer.configure(config, false);
                yield serializer;
            }
            case PROTOBUF -> {
                KafkaProtobufSerializer<? extends Message> serializer = new KafkaProtobufSerializer<>();
                serializer.configure(config, false);
                yield serializer;
            }
            default -> throw new IllegalArgumentException("SerdeType not supported by Confluent: " + serdeType);
        };
    }

    @Override
    public Deserializer<?> getDeserializer(RunContext runContext, SerdeType serdeType) throws IllegalVariableEvaluationException {
        String rUrl = runContext.render(this.schemaRegistryUrl).as(String.class).orElseThrow();

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", rUrl);

        return switch (serdeType) {
            case AVRO -> {
                KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
                deserializer.configure(config, false);
                yield deserializer;
            }
            case JSON -> {
                KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();
                deserializer.configure(config, false);
                yield deserializer;
            }
            case PROTOBUF -> {
                KafkaProtobufDeserializer<? extends Message> deserializer = new KafkaProtobufDeserializer<>();
                deserializer.configure(config, false);
                yield deserializer;
            }
            default -> throw new IllegalArgumentException("SerdeType not supported by Confluent: " + serdeType);
        };
    }
}

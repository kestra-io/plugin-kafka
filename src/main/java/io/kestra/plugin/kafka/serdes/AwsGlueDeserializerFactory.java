package io.kestra.plugin.kafka.serdes;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import static io.kestra.plugin.kafka.serdes.SerdeType.AVRO;

public class AwsGlueDeserializerFactory {
    public static Deserializer<?> getDeserializer(SerdeType serdeType) {
        if (serdeType == AVRO) {
            return new GlueSchemaRegistryKafkaDeserializer();
        } else {
            throw new IllegalArgumentException("Unsupported SerdeType for AWS Glue: " + serdeType);
        }
    }
}

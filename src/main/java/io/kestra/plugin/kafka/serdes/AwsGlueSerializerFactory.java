package io.kestra.plugin.kafka.serdes;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import org.apache.kafka.common.serialization.Serializer;

import static io.kestra.plugin.kafka.serdes.SerdeType.AVRO;

public class AwsGlueSerializerFactory {
    public static Serializer<?> getSerializer(SerdeType serdeType) {
        if (serdeType == AVRO) {
            return new GlueSchemaRegistryKafkaSerializer();
        } else {
            throw new IllegalArgumentException("Unsupported SerdeType for AWS Glue: " + serdeType);
        }
    }
}

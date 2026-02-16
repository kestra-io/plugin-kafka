package io.kestra.plugin.kafka.serdes;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class AwsGlueGenericRecordToMapDeserializer implements Deserializer<Object> {
    private final AWSKafkaAvroDeserializer deserializer;

    public AwsGlueGenericRecordToMapDeserializer(AWSKafkaAvroDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return mapGenericRecord(this.deserializer.deserialize(topic, data));
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return mapGenericRecord(this.deserializer.deserialize(topic, headers, data));
    }

    @Override
    public void close() {
        this.deserializer.close();
    }

    private static Object mapGenericRecord(Object value) {
        if (value instanceof GenericRecord genericRecord) {
            return GenericRecordToMapDeserializer.recordDeserializer(genericRecord);
        }

        return value;
    }
}

package io.kestra.plugin.kafka.serdes;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

public class MapToGenericRecordSerializer implements Serializer<Object> {

    private final KafkaAvroSerializer serializer;
    private final AvroSchema schema;

    public MapToGenericRecordSerializer(KafkaAvroSerializer serializer, AvroSchema schema) {
        this.serializer = serializer;
        this.schema = schema;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.serializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return serializer.serialize(topic, buildValue(schema.rawSchema(), data));
    }

    @Override
    public void close() {
        this.serializer.close();
    }

    private static GenericRecord buildRecordValue(Schema schema, Map<String, ?> data) {
        final var record = new org.apache.avro.generic.GenericData.Record(schema);
        data.forEach((key, value) -> record.put(key, buildValue(schema.getField(key).schema(), value)));
        return record;
    }

    private static GenericEnumSymbol<?> buildEnumValue(Schema schema, String data) {
        return new org.apache.avro.generic.GenericData.EnumSymbol(schema, data);
    }

    private static GenericArray<?> buildArrayValue(Schema schema, Collection<?> data) {
        final var values = data.stream().map(value -> buildValue(schema.getElementType(), value)).toList();
        return new org.apache.avro.generic.GenericData.Array<>(schema, values);
    }

    private static Object buildUnionValue(Schema schema, Object value) {
        // TODO
        for (Schema s : schema.getTypes()) {
            if (!s.getType().equals(Schema.Type.NULL)) {
                return buildValue(s, value);
            }
        }
        throw new IllegalArgumentException();
    }

    private static GenericFixed buildFixedValue(Schema schema, byte[] data) {
        // TODO
        return new org.apache.avro.generic.GenericData.Fixed(schema, data);
    }

    private static Object buildValue(Schema schema, Object data) {
        return switch (schema.getType()) {
            case UNION -> buildUnionValue(schema, data);
            case RECORD -> buildRecordValue(schema, (Map<String, ?>) data);
            case ARRAY -> buildArrayValue(schema, (Collection<?>) data);
            case ENUM -> buildEnumValue(schema, (String) data);
            case FIXED -> buildFixedValue(schema, (byte[]) data);
            case MAP, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL -> data;
        };
    }
}

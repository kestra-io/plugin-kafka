package io.kestra.plugin.kafka.serdes;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class GenericRecordToMapDeserializer implements Deserializer<Object> {
    private final KafkaAvroDeserializer deserializer;

    public GenericRecordToMapDeserializer(KafkaAvroDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        GenericRecord deserialize = (GenericRecord) this.deserializer.deserialize(topic, data);

        return GenericRecordToMapDeserializer.recordDeserializer(deserialize);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        GenericRecord deserialize = (GenericRecord) this.deserializer.deserialize(topic, headers, data);

        return GenericRecordToMapDeserializer.recordDeserializer(deserialize);
    }

    @Override
    public void close() {
        this.deserializer.close();
    }

    public static Map<String, Object> recordDeserializer(GenericRecord record) {
        return record
            .getSchema()
            .getFields()
            .stream()
            .collect(
                LinkedHashMap::new, // preserve schema field order
                (m, v) -> m.put(
                    v.name(),
                    GenericRecordToMapDeserializer.objectDeserializer(record.get(v.name()), v.schema())
                ),
                HashMap::putAll
            );
    }

    @SuppressWarnings("unchecked")
    private static Object objectDeserializer(Object value, Schema schema) {
        LogicalType logicalType = schema.getLogicalType();
        Type primitiveType = schema.getType();
        if (logicalType != null) {
            return value;
        } else {
            switch (primitiveType) {
                case UNION:
                    return GenericRecordToMapDeserializer.unionDeserializer(value, schema);
                case MAP:
                    return GenericRecordToMapDeserializer.mapDeserializer((Map<String, ?>) value, schema);
                case RECORD:
                    return GenericRecordToMapDeserializer.recordDeserializer((GenericRecord) value);
                case ENUM:
                    return value.toString();
                case ARRAY:
                    return arrayDeserializer((Collection<?>) value, schema);
                case FIXED:
                    return ((GenericFixed) value).bytes();
                case STRING:
                    return ((CharSequence) value).toString();
                case BYTES:
                    return ((ByteBuffer) value).array();
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                case NULL:
                    return value;
                default:
                    throw new IllegalStateException("Unexpected value: " + primitiveType);
            }
        }
    }

    private static Object unionDeserializer(Object value, Schema schema) {
        return GenericRecordToMapDeserializer.objectDeserializer(value, schema
            .getTypes()
            .stream()
            .filter(type -> {
                try {
                    // logical type is already converted and validated
                    if (type.getLogicalType() != null) {
                        return true;
                    } else {
                        return GenericData.get().validate(type, value);
                    }
                } catch (Exception e) {
                    return  false;
                }
            })
            .findFirst()
            .orElseThrow());
    }

    private static Map<String, ?> mapDeserializer(Map<String, ?> value, Schema schema) {
        return value
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> GenericRecordToMapDeserializer.objectDeserializer(e.getValue(), schema.getValueType()))
            );
    }

    private static Collection<?> arrayDeserializer(Collection<?> value, Schema schema) {
        return value
            .stream()
            .map(e -> GenericRecordToMapDeserializer.objectDeserializer(e, schema.getElementType()))
            .collect(Collectors.toList());
    }
}

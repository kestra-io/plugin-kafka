package io.kestra.plugin.kafka.serdes;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

public class KafkaAvroSerializer extends io.confluent.kafka.serializers.KafkaAvroSerializer {
    @Override
    protected DatumWriter<?> getDatumWriter(Object value, Schema schema) {
        return new GenericDatumWriter<>(schema, GenericData.get());
    }
}

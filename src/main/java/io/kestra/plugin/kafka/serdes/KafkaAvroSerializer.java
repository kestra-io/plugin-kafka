package io.kestra.plugin.kafka.serdes;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.data.TimeConversions;

import java.util.HashMap;
import java.util.Map;

public class KafkaAvroSerializer extends io.confluent.kafka.serializers.KafkaAvroSerializer {

    static {
        org.apache.avro.generic.GenericData genericData = AvroSchemaUtils.getGenericData();
        genericData.addLogicalTypeConversion(new AdditionalConversions.ZonedDateTimeMillisConversion());
        genericData.addLogicalTypeConversion(new AdditionalConversions.ZonedDateTimeMicrosConversion());
        genericData.addLogicalTypeConversion(new AdditionalConversions.OffsetDateTimeMillisConversion());
        genericData.addLogicalTypeConversion(new AdditionalConversions.OffsetDateTimeMicrosConversion());
        // IMPORTANT - we need to (re)add TimeConversions to ensure that ZonedDateTime/OffsetDateTime
        // do not override TimeConversions for deserialization
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        HashMap<String, Object> mutable = new HashMap<>(configs);
        if (!mutable.containsKey(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG)) {
            // by default, always use LogicalType
            mutable.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        }
        super.configure(mutable, isKey);
    }
}

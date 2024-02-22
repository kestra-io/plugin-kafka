package io.kestra.plugin.kafka.serdes;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;

public abstract class GenericData {
    private final static org.apache.avro.generic.GenericData GENERIC_DATA = new org.apache.avro.generic.GenericData();

    static {
        GENERIC_DATA.addLogicalTypeConversion(new Conversions.DecimalConversion());
        GENERIC_DATA.addLogicalTypeConversion(new Conversions.UUIDConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.DateConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new AdditionalConversions.ZonedDateTimeMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new AdditionalConversions.ZonedDateTimeMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new AdditionalConversions.OffsetDateTimeMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new AdditionalConversions.OffsetDateTimeMicrosConversion());
    }

    public static org.apache.avro.generic.GenericData get() {
        return GENERIC_DATA;
    }
}

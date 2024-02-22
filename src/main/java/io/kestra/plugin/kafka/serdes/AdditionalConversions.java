package io.kestra.plugin.kafka.serdes;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class AdditionalConversions {

    public static class OffsetDateTimeMicrosConversion extends Conversion<OffsetDateTime> {

        private final TimeConversions.TimestampMicrosConversion timestampMicrosConversion = new TimeConversions.TimestampMicrosConversion();

        @Override
        public Class<OffsetDateTime> getConvertedType() {
            return OffsetDateTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-micros";
        }

        @Override
        public String adjustAndSetValue(String varName, String valParamName) {
            return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MICROS);";
        }

        @Override
        public OffsetDateTime fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
            Instant instant = timestampMicrosConversion.fromLong(microsFromEpoch, schema, type);
            return OffsetDateTime.ofInstant(instant,ZoneId.systemDefault().getRules().getOffset(Instant.now()));
        }

        @Override
        public Long toLong(OffsetDateTime odt, Schema schema, LogicalType type) {
            Instant instant = odt.toInstant();
            return timestampMicrosConversion.toLong(instant, schema, type);
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class OffsetDateTimeMillisConversion extends Conversion<OffsetDateTime> {

        private final TimeConversions.TimestampMillisConversion timestampMillisConversion = new TimeConversions.TimestampMillisConversion();

        @Override
        public Class<OffsetDateTime> getConvertedType() {
            return OffsetDateTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        @Override
        public String adjustAndSetValue(String varName, String valParamName) {
            return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MILLIS);";
        }

        @Override
        public OffsetDateTime fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
            Instant instant = timestampMillisConversion.fromLong(millisFromEpoch, schema, type);
            return OffsetDateTime.ofInstant(instant,ZoneId.systemDefault().getRules().getOffset(Instant.now()));
        }

        @Override
        public Long toLong(OffsetDateTime odt, Schema schema, LogicalType type) {
            Instant instant = odt.toInstant();
            return timestampMillisConversion.toLong(instant, schema, type);
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }
    
    public static class ZonedDateTimeMicrosConversion extends Conversion<ZonedDateTime> {

        private final TimeConversions.TimestampMicrosConversion timestampMicrosConversion = new TimeConversions.TimestampMicrosConversion();

        @Override
        public Class<ZonedDateTime> getConvertedType() {
            return ZonedDateTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-micros";
        }

        @Override
        public String adjustAndSetValue(String varName, String valParamName) {
            return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MICROS);";
        }

        @Override
        public ZonedDateTime fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
            Instant instant = timestampMicrosConversion.fromLong(microsFromEpoch, schema, type);
            return ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
        }

        @Override
        public Long toLong(ZonedDateTime zdt, Schema schema, LogicalType type) {
            Instant instant = zdt.toInstant();
            return timestampMicrosConversion.toLong(instant, schema, type);
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class ZonedDateTimeMillisConversion extends Conversion<ZonedDateTime> {

        private final TimeConversions.TimestampMillisConversion timestampMillisConversion = new TimeConversions.TimestampMillisConversion();

        @Override
        public Class<ZonedDateTime> getConvertedType() {
            return ZonedDateTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        @Override
        public String adjustAndSetValue(String varName, String valParamName) {
            return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MILLIS);";
        }

        @Override
        public ZonedDateTime fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
            Instant instant = timestampMillisConversion.fromLong(millisFromEpoch, schema, type);
            return ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
        }

        @Override
        public Long toLong(ZonedDateTime zdt, Schema schema, LogicalType type) {
            Instant instant = zdt.toInstant();
            return timestampMillisConversion.toLong(instant, schema, type);
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }
}

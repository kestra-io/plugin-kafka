package io.kestra.plugin.kafka.serdes;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class AdditionalConversions {
    private static Long toLongMillis(Instant instant) {
        return instant.toEpochMilli();
    }

    private static Long toLongMicros(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();

        if (seconds < 0 && nanos > 0) {
            long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
            long adjustment = (nanos / 1_000L) - 1_000_000;

            return Math.addExact(micros, adjustment);
        } else {
            long micros = Math.multiplyExact(seconds, 1_000_000L);

            return Math.addExact(micros, nanos / 1_000L);
        }
    }
    
    private static Instant fromLongMicros(Long microsFromEpoch) {
        long epochSeconds = microsFromEpoch / (1_000_000L);
        long nanoAdjustment = (microsFromEpoch % (1_000_000L)) * 1_000L;

        return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
    }

    public static class OffsetDateTimeMicrosConversion extends Conversion<OffsetDateTime> {
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
            return fromLongMicros(microsFromEpoch).atOffset(ZoneId.systemDefault().getRules().getOffset(Instant.now()));
        }

        @Override
        public Long toLong(OffsetDateTime instant, Schema schema, LogicalType type) {
            return toLongMicros(instant.toInstant());
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class OffsetDateTimeMillisConversion extends Conversion<OffsetDateTime> {
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
            return Instant.ofEpochMilli(millisFromEpoch).atOffset(ZoneId.systemDefault().getRules().getOffset(Instant.now()));
        }

        @Override
        public Long toLong(OffsetDateTime timestamp, Schema schema, LogicalType type) {
            return toLongMillis(timestamp.toInstant());
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }
    
    public static class ZonedDateTimeMicrosConversion extends Conversion<ZonedDateTime> {
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
            return fromLongMicros(microsFromEpoch).atZone(ZoneId.systemDefault());
        }

        @Override
        public Long toLong(ZonedDateTime instant, Schema schema, LogicalType type) {
            return toLongMicros(instant.toInstant());
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class ZonedDateTimeMillisConversion extends Conversion<ZonedDateTime> {
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
            return Instant.ofEpochMilli(millisFromEpoch).atZone(ZoneId.systemDefault());
        }

        @Override
        public Long toLong(ZonedDateTime timestamp, Schema schema, LogicalType type) {
            return toLongMillis(timestamp.toInstant());
        }

        @Override
        public Schema getRecommendedSchema() {
            return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }
}

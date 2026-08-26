package io.kestra.plugin.kafka.admin;

public enum PatternType {
    LITERAL,
    PREFIXED,
    MATCH;

    org.apache.kafka.common.resource.PatternType toKafkaType() {
        try {
            return org.apache.kafka.common.resource.PatternType.valueOf(this.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Kafka resource pattern type '" + this + "' is not supported by the current Kafka client library version", e);
        }
    }
}

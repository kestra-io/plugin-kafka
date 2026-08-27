package io.kestra.plugin.kafka;

public enum ResourceType {
    TOPIC,
    GROUP,
    CLUSTER,
    TRANSACTIONAL_ID,
    DELEGATION_TOKEN,
    USER;

    org.apache.kafka.common.resource.ResourceType toKafkaType() {
        try {
            return org.apache.kafka.common.resource.ResourceType.valueOf(this.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Kafka resource type '" + this + "' is not supported by the current Kafka client library version", e);
        }
    }
}

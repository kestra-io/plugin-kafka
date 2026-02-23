package io.kestra.plugin.kafka;

public enum QueueAcknowledgeType {
    ACCEPT,
    RELEASE,
    REJECT,
    RENEW;

    org.apache.kafka.clients.consumer.AcknowledgeType toKafkaType() {
        try {
            return org.apache.kafka.clients.consumer.AcknowledgeType.valueOf(this.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Kafka client acknowledge type '" + this + "' is not supported by the current Kafka client library version", e);
        }
    }
}

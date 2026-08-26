package io.kestra.plugin.kafka.admin;

public enum ScramMechanism {
    SCRAM_SHA_256,
    SCRAM_SHA_512;

    org.apache.kafka.clients.admin.ScramMechanism toKafkaType() {
        try {
            return org.apache.kafka.clients.admin.ScramMechanism.valueOf(this.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Kafka SCRAM mechanism '" + this + "' is not supported by the current Kafka client library version", e);
        }
    }
}

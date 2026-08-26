package io.kestra.plugin.kafka.admin;

public enum AclOperation {
    ALL,
    READ,
    WRITE,
    CREATE,
    DELETE,
    ALTER,
    DESCRIBE,
    CLUSTER_ACTION,
    DESCRIBE_CONFIGS,
    ALTER_CONFIGS,
    IDEMPOTENT_WRITE;

    org.apache.kafka.common.acl.AclOperation toKafkaType() {
        try {
            return org.apache.kafka.common.acl.AclOperation.valueOf(this.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Kafka ACL operation '" + this + "' is not supported by the current Kafka client library version", e);
        }
    }
}

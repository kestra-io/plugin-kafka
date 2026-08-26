package io.kestra.plugin.kafka;

public enum AclPermissionType {
    ALLOW,
    DENY;

    org.apache.kafka.common.acl.AclPermissionType toKafkaType() {
        try {
            return org.apache.kafka.common.acl.AclPermissionType.valueOf(this.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Kafka ACL permission type '" + this + "' is not supported by the current Kafka client library version", e);
        }
    }
}

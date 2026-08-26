# Kestra Kafka Plugin

## What

- Provides plugin components under `io.kestra.plugin.kafka`.
- Includes classes such as `QueueAcknowledgeType`, `Message`, `Consume`, `Produce`.
- Provides control-plane (AdminClient) tasks under `io.kestra.plugin.kafka.admin` for provisioning multi-tenant clusters (topics, ACLs, quotas, SCRAM credentials, consumer groups, log dirs).

## Why

- What user problem does this solve? Teams need to produce, consume, and trigger workflows from Apache Kafka topics, including share-group queue semantics from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Kafka steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Kafka.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `kafka`

Infrastructure dependencies (Docker Compose services):

- `kafka`
- `schema-registry`

### Key Plugin Classes

- `io.kestra.plugin.kafka.Consume`
- `io.kestra.plugin.kafka.Produce`
- `io.kestra.plugin.kafka.RealtimeTrigger`
- `io.kestra.plugin.kafka.Trigger`
- `io.kestra.plugin.kafka.admin.AbstractKafkaAdminTask` — shared AdminClient lifecycle/timeout base for all admin tasks
- `io.kestra.plugin.kafka.admin.TopicCreate`, `TopicUpdate`, `TopicDelete`, `TopicList`, `TopicDescribe`, `TopicCreatePartitions`
- `io.kestra.plugin.kafka.admin.AclCreate`, `AclDelete`, `AclList`
- `io.kestra.plugin.kafka.admin.QuotaAlter`, `QuotaDescribe`
- `io.kestra.plugin.kafka.admin.ScramCredentialCreate`, `ScramCredentialDelete`
- `io.kestra.plugin.kafka.admin.ConsumerGroupList`, `ConsumerGroupDescribe`, `ConsumerGroupAlterOffsets`, `ConsumerGroupDelete`
- `io.kestra.plugin.kafka.admin.DescribeLogDirs`

### Project Structure

```
plugin-kafka/
├── src/main/java/io/kestra/plugin/kafka/serdes/
├── src/main/java/io/kestra/plugin/kafka/admin/
├── src/test/java/io/kestra/plugin/kafka/serdes/
├── src/test/java/io/kestra/plugin/kafka/admin/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines

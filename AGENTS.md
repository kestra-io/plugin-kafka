# Kestra Kafka Plugin

## What

- Provides plugin components under `io.kestra.plugin.kafka`.
- Includes classes such as `QueueAcknowledgeType`, `Message`, `Consume`, `Produce`.
- Provides control-plane (AdminClient) tasks for provisioning multi-tenant clusters (topics, ACLs, quotas, SCRAM credentials, consumer groups, log dirs).

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
- `kafka-connect`

### Key Plugin Classes

- `io.kestra.plugin.kafka.Consume`
- `io.kestra.plugin.kafka.Produce`
- `io.kestra.plugin.kafka.RealtimeTrigger`
- `io.kestra.plugin.kafka.Trigger`
- `io.kestra.plugin.kafka.AbstractKafkaAdminTask` — shared AdminClient lifecycle/timeout base for all admin tasks
- `io.kestra.plugin.kafka.TopicCreate`, `TopicUpdate`, `TopicDelete`, `TopicList`, `TopicDescribe`, `TopicCreatePartitions`
- `io.kestra.plugin.kafka.AclCreate`, `AclDelete`, `AclList`
- `io.kestra.plugin.kafka.QuotaAlter`, `QuotaDescribe`
- `io.kestra.plugin.kafka.ScramCredentialCreate`, `ScramCredentialDelete`
- `io.kestra.plugin.kafka.ConsumerGroupList`, `ConsumerGroupDescribe`, `ConsumerGroupAlterOffsets`, `ConsumerGroupDelete`
- `io.kestra.plugin.kafka.DescribeLogDirs`
- `io.kestra.plugin.kafka.AbstractKafkaConnectTask` — shared HTTP client/auth/error-handling base for all Kafka Connect REST API tasks (Kafka Connect has no Java admin client)
- `io.kestra.plugin.kafka.ConnectorCreate`, `ConnectorUpdateConfig`, `ConnectorGetStatus`, `ConnectorPause`, `ConnectorResume`, `ConnectorRestart`, `ConnectorDelete`, `ConnectorList`, `ConnectorGetConfig`, `ConnectorGetOffsets`, `ConnectorAlterOffsets`, `ConnectorResetOffsets`
- `io.kestra.plugin.kafka.ConnectorStatusTrigger` — polls a connector's status and fires an execution when it or any of its tasks reaches a target state

### Project Structure

```
plugin-kafka/
├── src/main/java/io/kestra/plugin/kafka/serdes/
├── src/test/java/io/kestra/plugin/kafka/serdes/
├── build.gradle
└── README.md
```

Admin (control-plane) tasks live directly under `io.kestra.plugin.kafka` (root package), not a subpackage — the plugin's doc-lint tooling (`PKG-003`) forbids mixing root-level and subpackage tasks/triggers, and moving the existing data-plane tasks (`Produce`/`Consume`/`Trigger`/`RealtimeTrigger`) into a subpackage would be a breaking change for existing flows.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines

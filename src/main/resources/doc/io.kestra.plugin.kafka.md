# How to use the Apache Kafka plugin

The plugin uses Apache Kafka's native client configuration model — any standard Kafka client property is valid in task configuration.

## Authentication

All tasks pass connection and security configuration through the `properties` map. The only required key is `bootstrap.servers`; any [Kafka consumer](https://kafka.apache.org/documentation/#consumerconfigs) or [producer](https://kafka.apache.org/documentation/#producerconfigs) config key is accepted, including SASL and SSL settings. Store connection details in [secrets](https://kestra.io/docs/concepts/secret).

For SSL, pass `ssl.keystore.location` and `ssl.truststore.location` as base64-encoded file content rather than file paths — the plugin decodes them to a temporary file before passing them to the Kafka client.

## Common properties

Pass Schema Registry configuration (e.g., `schema.registry.url`) in the `serdeProperties` map, not `properties`. This applies to any task using Avro or other schema-aware serializers.

## Tasks

Use `Produce` to publish messages to a topic and `Consume` to read a batch of records as a step within a running flow. For triggering flows from incoming messages, choose between `Trigger` and `RealtimeTrigger`: `Trigger` polls on a fixed interval and starts one execution per batch — use `maxRecords` or `maxDuration` to cap batch size; `RealtimeTrigger` starts one execution per record as it arrives with no batching. Use `Trigger` when you want predictable execution rate; use `RealtimeTrigger` when latency matters.

## Admin tasks

Control-plane tasks built on the Kafka AdminClient, typically used to provision and administer multi-tenant clusters (see [Kafka multi-tenancy](https://kafka.apache.org/documentation/#operations_multitenancy)). Like the data-plane tasks, every admin task takes connection settings through a `properties` map (`bootstrap.servers` required); an `AdminClient` is created and closed per task run. Every AdminClient call is bounded by a `timeout` property (defaults to `PT30S`) so a task never hangs the worker indefinitely.

- **Topics**: `TopicCreate` (defaults to replication factor `1`, unsuitable for production; set `ifNotExists: true` for idempotent provisioning), `TopicUpdate` (alter `retention.ms`/`retention.bytes` and other configs), `TopicDelete`, `TopicList`, `TopicDescribe` (partitions layout and effective configs), `TopicCreatePartitions`.
- **ACLs**: `AclCreate`, `AclDelete`, `AclList`. Use `patternType: PREFIXED` on `AclCreate` to authorize an entire per-tenant topic namespace (e.g. `tenant_acme_`) with a single ACL. `AclDelete`/`AclList` take the same filter fields, all optional — an unset filter field matches any value, so scope `resourceName`/`resourceType` carefully. `AclDelete` refuses to run when every filter field is unset (or renders empty), since that would match every ACL on the cluster; set `deleteAll: true` to confirm that is intentional.
- **Quotas**: `QuotaAlter` and `QuotaDescribe`, keyed by a `user`/`client-id`/`ip` entity combination, covering `producer_byte_rate`, `consumer_byte_rate`, `request_percentage` and `controller_mutation_rate`.
- **SCRAM credentials**: `ScramCredentialCreate` (upserts a SASL/SCRAM user credential — `password` is a secret property) and `ScramCredentialDelete`.
- **Consumer groups**: `ConsumerGroupList`, `ConsumerGroupDescribe` (includes per-partition committed offset, end offset and lag), `ConsumerGroupAlterOffsets`, `ConsumerGroupDelete`. Deleting or altering offsets of a group with active members fails with `GroupNotEmptyException` — stop its consumers first.
- **Metering**: `DescribeLogDirs` reports per-partition on-disk size and replica lag per broker, plus a `topicSizes` summary useful for per-tenant storage metering.

List/describe tasks return structured `Map`/`List` outputs (not files), since control-plane result sets are small enough to template directly, e.g. `{{ outputs.topic_describe.configs['retention.ms'] }}`. Delete and alter-offsets tasks are destructive and irreversible — there is no built-in approval step, so gate them with Kestra's own approval features if needed.

## Kafka Connect tasks

Kafka Connect has no dedicated Java admin client — every task in this group calls the Connect [REST API](https://kafka.apache.org/documentation/#connect_rest) directly. Every task takes `connectUrl` (e.g. `http://connect:8083`), and optionally `username`/`password` for HTTP basic auth and `headers` for anything else (a reverse proxy, a bearer token). No `Authorization` header is sent when `username`/`password` are unset. Store credentials in [secrets](https://kestra.io/docs/concepts/secret).

- **Lifecycle**: `ConnectorCreate`, `ConnectorUpdateConfig`, `ConnectorDelete`, `ConnectorPause`, `ConnectorResume`, `ConnectorRestart` (`includeTasks`/`onlyFailed`, both default `false`).
- **Inspection**: `ConnectorGetStatus` returns typed `connectorState`/`tasks[*].state` fields usable directly in flow conditions. `ConnectorList` returns connector names, or name+status pairs with `expandStatus: true`. `ConnectorGetConfig` returns a `config` output shaped exactly like `ConnectorCreate`'s `config` input, so it pipes straight into `ConnectorCreate`/`ConnectorUpdateConfig` to clone or restore a connector.
- **Offsets**: `ConnectorGetOffsets` reads offsets and requires Kafka Connect clusters running Kafka 3.5+. `ConnectorAlterOffsets`/`ConnectorResetOffsets` additionally require the connector to be in the `STOPPED` state and Kafka 3.6+, since altering/resetting offsets via `PATCH`/`DELETE /connectors/{name}/offsets` is a [KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875:+First-class+offsets+support+in+Kafka+Connect) concept that shipped after the read-only offsets endpoint; these two tasks don't pre-validate the connector's state, they surface Connect's error body verbatim if it isn't stopped. On older clusters, delete and recreate the connector instead.

Every task fails with the Connect API's error body verbatim on a non-2xx response (e.g. 409 on `ConnectorCreate` for a name that already exists, 404 naming the connector on a lookup for a connector that doesn't exist).

## Triggers

`ConnectorStatusTrigger` polls a connector's status on a fixed interval (default `PT1M`) and fires an execution when the connector or any of its tasks matches `targetState` (case-insensitive), exposing the same typed fields as `ConnectorGetStatus`. A connector deleted mid-poll is treated as no match for that tick, not a trigger failure.

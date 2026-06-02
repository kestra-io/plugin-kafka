# How to use the Apache Kafka plugin

The plugin uses Apache Kafka's native client configuration model — any standard Kafka client property is valid in task configuration.

## Authentication

All tasks pass connection and security configuration through the `properties` map. The only required key is `bootstrap.servers`; any [Kafka consumer](https://kafka.apache.org/documentation/#consumerconfigs) or [producer](https://kafka.apache.org/documentation/#producerconfigs) config key is accepted, including SASL and SSL settings. Store connection details in [secrets](https://kestra.io/docs/concepts/secret).

For SSL, pass `ssl.keystore.location` and `ssl.truststore.location` as base64-encoded file content rather than file paths — the plugin decodes them to a temporary file before passing them to the Kafka client.

## Common properties

Pass Schema Registry configuration (e.g., `schema.registry.url`) in the `serdeProperties` map, not `properties`. This applies to any task using Avro or other schema-aware serializers.

## Tasks

Use `Produce` to publish messages to a topic and `Consume` to read a batch of records as a step within a running flow. For triggering flows from incoming messages, choose between `Trigger` and `RealtimeTrigger`: `Trigger` polls on a fixed interval and starts one execution per batch — use `maxRecords` or `maxDuration` to cap batch size; `RealtimeTrigger` starts one execution per record as it arrives with no batching. Use `Trigger` when you want predictable execution rate; use `RealtimeTrigger` when latency matters.

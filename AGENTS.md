# Kestra Kafka Plugin

## What

- Provides plugin components under `io.kestra.plugin.kafka`.
- Includes classes such as `QueueAcknowledgeType`, `Message`, `Consume`, `Produce`.

## Why

- This plugin integrates Kestra with Apache Kafka.
- It provides tasks that produce, consume, and trigger workflows from Apache Kafka topics, including share-group queue semantics.

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

### Project Structure

```
plugin-kafka/
├── src/main/java/io/kestra/plugin/kafka/serdes/
├── src/test/java/io/kestra/plugin/kafka/serdes/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines

# Kestra Kafka Plugin

## What

Leverage Apache Kafka messaging in Kestra data workflows. Exposes 4 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Kafka, allowing orchestration of Apache Kafka-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.

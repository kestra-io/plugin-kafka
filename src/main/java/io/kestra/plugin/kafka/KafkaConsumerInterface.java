package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

public interface KafkaConsumerInterface {
    @Schema(
        title = "Kafka topic(s) to consume from",
        description = "String or list of strings; mutually exclusive with `topicPattern`."
    )
    @PluginProperty(group = "advanced")
    Object getTopic();

    @Schema(
        title = "Regex pattern of topics to consume from",
        description = "Subscribes to topics matching the pattern and receives dynamic partition assignments; mutually exclusive with `topic`."
    )
    @PluginProperty(group = "advanced")
    Property<String> getTopicPattern();

    @Schema(
        title = "Specific partitions to consume",
        description = "Manually assign partitions; bypasses consumer group rebalancing."
    )
    @PluginProperty(group = "advanced")
    Property<List<Integer>> getPartitions();

    @Schema(
        title = "Kafka consumer group ID",
        description = "Determines offset management; mandatory for `groupType: SHARE` (share group) and for the polling `Trigger`. Optional for `Consume`. Required for `RealtimeTrigger` with a plain `topic`, unless `topicPattern` or `since` is set. Without it, records are read from the beginning (or from `since`), no offsets are committed, and with `topicPattern` the matching topics are resolved once at startup, so topics created later are not picked up."
    )
    @PluginProperty(group = "advanced")
    Property<String> getGroupId();

    @Schema(
        title = "Group protocol to consume with",
        description = """
            `CONSUMER` (default) uses classic Kafka consumer groups for backward compatibility.
            `SHARE` uses Kafka share groups (queue semantics) and requires `groupId`.
            In `SHARE` mode, only `topic` is supported for subscription.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<GroupType> getGroupType();

    @Schema(
        title = "Acknowledgement action for SHARE group type",
        description = """
            Used only when `groupType` is `SHARE`.
            `ACCEPT` marks records as processed, `RELEASE` returns records to the queue, `REJECT` negatively acknowledges records,
            and `RENEW` extends the acquisition lock timeout for the current delivery attempt without changing record state.
            Ignored when `groupType` is `CONSUMER`.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<QueueAcknowledgeType> getAcknowledgeType();

    @Schema(
        title = "Deserializer used for the key",
        description = "Default STRING. Options: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<SerdeType> getKeyDeserializer();

    @Schema(
        title = "Deserializer used for the value",
        description = "Default STRING. Options: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<SerdeType> getValueDeserializer();

    @Schema(
        title = "Timestamp to start consuming from",
        description = "ISO-8601 instant used when no consumer group offsets exist; ignored when a consumer group controls offsets."
    )
    @PluginProperty(group = "advanced")
    Property<String> getSince();

    @Schema(
        title = "Behavior on serde error",
        description = """
            Applies to value deserialization errors only: `SKIPPED` (default), `STORE` to internal storage, or `DLQ` to the configured topic.
            Key deserialization errors are not handled.
            For RealtimeTrigger, `STORE` starts an execution whose `trigger.value` is the stored file URI.
            """
    )
    @PluginProperty(group = "advanced")
    OnSerdeError getOnSerdeError();

    @Builder
    @Getter
    class OnSerdeError {

        @Schema(title = "Action to take on serde error")
        @NotNull
        @Builder.Default
        Property<OnSerdeErrorBehavior> type = Property.ofValue(OnSerdeErrorBehavior.SKIPPED);

        @Schema(title = "Topic used when type is DLQ")
        Property<String> topic;
    }

    enum OnSerdeErrorBehavior {
        SKIPPED, DLQ, STORE
    }
}

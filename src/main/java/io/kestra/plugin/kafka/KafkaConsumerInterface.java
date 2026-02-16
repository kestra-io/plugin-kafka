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
    Object getTopic();

    @Schema(
        title = "Regex pattern of topics to consume from",
        description = "Subscribes to topics matching the pattern and receives dynamic partition assignments; mutually exclusive with `topic`."
    )
    Property<String> getTopicPattern();

    @Schema(
        title = "Specific partitions to consume",
        description = "Manually assign partitions; bypasses consumer group rebalancing."
    )
    Property<List<Integer>> getPartitions();

    @Schema(
        title = "Kafka consumer group ID",
        description = "Determines offset management; required when using `topicPattern`."
    )
    Property<String> getGroupId();

    @Schema(
        title = "Deserializer used for the key",
        description = "Default STRING. Options: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    Property<SerdeType> getKeyDeserializer();

    @Schema(
        title = "Deserializer used for the value",
        description = "Default STRING. Options: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    Property<SerdeType> getValueDeserializer();

    @Schema(
        title = "Timestamp to start consuming from",
        description = "ISO-8601 instant used when no consumer group offsets exist; ignored when a consumer group controls offsets."
    )
    Property<String> getSince();

    @Schema(
        title = "Behavior on serde error",
        description = "Applies when valueDeserializer is JSON: `SKIPPED` (default), `STORE` to internal storage, or `DLQ` to the configured topic."
    )
    @PluginProperty
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

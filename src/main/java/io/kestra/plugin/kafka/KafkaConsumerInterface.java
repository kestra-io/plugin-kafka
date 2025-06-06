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
        title = "Kafka topic(s) to consume messages from.",
        description = "It can be a string or a list of strings to consume from one or multiple topics."
    )
    Object getTopic();

    @Schema(
        title = "Kafka topic pattern to consume messages from.",
        description = "Consumer will subscribe to all topics matching the specified pattern to get dynamically assigned partitions."
    )
    Property<String> getTopicPattern();

    @Schema(
        title = "Topic partitions to consume messages from.",
        description = "Manually assign a list of partitions to the consumer."
    )
    Property<List<Integer>> getPartitions();

    @Schema(
        title = "Kafka consumer group ID.",
        description = "Using a consumer group, we will fetch only records that haven't been consumed yet."
    )
    Property<String> getGroupId();

    @Schema(
        title = "The deserializer used for the key.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    Property<SerdeType> getKeyDeserializer();

    @Schema(
        title = "The deserializer used for the value.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    Property<SerdeType> getValueDeserializer();

    @Schema(
        title = "Timestamp of a message to start consuming messages from.",
        description = "By default, we consume all messages from the topics with no consumer group or depending on the " +
            "configuration of the `auto.offset.reset` property. However, you can provide an arbitrary start time.\n" +
            "This property is ignored if a consumer group is used.\n" +
            "It must be a valid ISO 8601 date."
    )
    Property<String> getSince();

    @Schema(
        title = "On serde error.",
        description = """
    Set the behavior wanted when valueDeserializer is JSON and a serde error has occurred :
        - SKIPPED : all invalid messages will be skipped
        - STORE : messages will be ignored and stored as a file
        - DLQ : messages will be ignored and sent to the DLQ specified in `topic`
    """
    )
    @PluginProperty
    OnSerdeError getOnSerdeError();

    @Builder
    @Getter
    class OnSerdeError {

        @Schema(title = "Behavior in case of serde error.")
        @NotNull
        @Builder.Default
        Property<OnSerdeErrorBehavior> type = Property.of(OnSerdeErrorBehavior.SKIPPED);

        @Schema(title = "Topic used when DLQ has been chosen.")
        Property<String> topic;
    }

    enum OnSerdeErrorBehavior {
        SKIPPED, DLQ, STORE
    }
}

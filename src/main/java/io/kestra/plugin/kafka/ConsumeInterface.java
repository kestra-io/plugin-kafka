package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import javax.validation.constraints.NotNull;

public interface ConsumeInterface {
    @Schema(
        title = "Kafka topic(s) to consume messages from.",
        description = "It can be a string or a list of strings to consume from one or multiple topics."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Object getTopic();

    @Schema(
        title = "Kafka consumer group ID.",
        description = "Using a consumer group, we will fetch only records that haven't been consumed yet."
    )
    @PluginProperty(dynamic = true)
    String getGroupId();

    @Schema(
        title = "The deserializer used for the key.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    SerdeType getKeyDeserializer();

    @Schema(
        title = "The deserializer used for the value.",
        description = "Possible values are: `STRING`, `INTEGER`, `FLOAT`, `DOUBLE`, `LONG`, `SHORT`, `BYTE_ARRAY`, `BYTE_BUFFER`, `BYTES`, `UUID`, `VOID`, `AVRO`, `JSON`."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    SerdeType getValueDeserializer();

    @Schema(
        title = "Timestamp of a message to start consuming messages from.",
        description = "By default, we consume all messages from the topics with no consumer group or depending on the " +
            "configuration of the `auto.offset.reset` property. However, you can provide an arbitrary start time.\n" +
            "This property is ignored if a consumer group is used.\n" +
            "It must be a valid ISO 8601 date."
    )
    @PluginProperty(dynamic = true)
    String getSince();

    @Schema(
        title = "How often to poll for a record.",
        description = "If no records are available, the maximum wait duration to wait for new records. "
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Duration getPollDuration();

    @Schema(
        title = "The maximum number of records to fetch before stopping the consumption process.",
        description = "It's a soft limit evaluated every second."
    )
    @PluginProperty(dynamic = false)
    Integer getMaxRecords();

    @Schema(
        title = "The maximum duration to wait for new records before stopping the consumption process.",
        description = "It's a soft limit evaluated every second."
    )
    @PluginProperty(dynamic = false)
    Duration getMaxDuration();
}

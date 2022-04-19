package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import javax.validation.constraints.NotNull;

public interface ConsumeInterface {
    @Schema(
        title = "Kafka topic(s) where to consume message",
        description = "Can be a string or a List of string to consume from multiple topic"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Object getTopic();

    @Schema(
        title = "The consumer group",
        description = "Using consumer group, we will fetch only records not already consumed"
    )
    @PluginProperty(dynamic = true)
    String getGroupId();

    @Schema(
        title = "Deserializer used for the key"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    SerdeType getKeyDeserializer();

    @Schema(
        title = "Deserializer used for the value"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    SerdeType getValueDeserializer();

    @Schema(
        title = "Timestamp of message to start with",
        description = "By default, we consume all messages from the topics with no consumer group or depending on " +
            "configuration `auto.offset.reset` with consumer group, but you can provide a arbitrary start time.\n" +
            "This property is ignore if a consumer group is used.\n" +
            "Must be a valid iso 8601 date."
    )
    @PluginProperty(dynamic = true)
    String getSince();

    @Schema(
        title = "Duration waiting for record to be polled",
        description = "If no records are available, the max wait to wait for a new records. "
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Duration getPollDuration();

    @Schema(
        title = "The max number of rows to fetch before stopping",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Integer getMaxRecords();

    @Schema(
        title = "The max duration waiting for new rows",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Duration getMaxDuration();
}

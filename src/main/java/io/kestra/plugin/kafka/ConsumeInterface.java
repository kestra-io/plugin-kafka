package io.kestra.plugin.kafka;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface ConsumeInterface extends KafkaConsumerInterface {

    @Schema(
        title = "Maximum records to consume before stopping",
        description = "Soft limit checked on each poll."
    )
    @PluginProperty(group = "execution")
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum duration to wait before stopping",
        description = """
            Total consume deadline (soft limit checked on each poll). When unset, `pollDuration` is used as the deadline.
            """
    )
    @PluginProperty(group = "execution")
    Property<Duration> getMaxDuration();

    @Schema(
        title = "How often to poll for records",
        description = """
            Maximum wait per poll when no records are available. Also acts as the overall consume deadline when `maxDuration` is unset.
            """
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Duration> getPollDuration();
}

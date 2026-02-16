package io.kestra.plugin.kafka;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;

public interface ConsumeInterface extends KafkaConsumerInterface {

    @Schema(
        title = "Maximum records to consume before stopping",
        description = "Soft limit checked on each poll."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum duration to wait before stopping",
        description = "Soft limit checked on each poll."
    )
    Property<Duration> getMaxDuration();

    @Schema(
        title = "How often to poll for records",
        description = "Maximum wait per poll when no records are available."
    )
    @NotNull
    Property<Duration> getPollDuration();
}

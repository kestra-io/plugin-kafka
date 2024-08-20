package io.kestra.plugin.kafka;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

import jakarta.validation.constraints.NotNull;

public interface ConsumeInterface extends KafkaConsumerInterface {

    @Schema(
        title = "The maximum number of records to fetch before stopping the consumption process.",
        description = "It's a soft limit evaluated every second."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "The maximum duration to wait for new records before stopping the consumption process.",
        description = "It's a soft limit evaluated every second."
    )
    Property<Duration> getMaxDuration();

    @Schema(
        title = "How often to poll for a record.",
        description = "If no records are available, the maximum wait duration to wait for new records. "
    )
    @NotNull
    Property<Duration> getPollDuration();
}

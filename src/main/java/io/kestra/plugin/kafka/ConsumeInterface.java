package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import java.util.List;

import jakarta.validation.constraints.NotNull;

public interface ConsumeInterface extends KafkaConsumerInterface {

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

    @Schema(
        title = "How often to poll for a record.",
        description = "If no records are available, the maximum wait duration to wait for new records. "
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Duration getPollDuration();
}

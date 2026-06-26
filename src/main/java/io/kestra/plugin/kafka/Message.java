package io.kestra.plugin.kafka;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.List;

@Builder
@Getter
public class Message implements io.kestra.core.models.tasks.Output {
    @Schema(title = "The message key")
    Object key;

    @Schema(title = "The message value")
    Object value;

    @Schema(title = "The topic the message belongs to")
    String topic;

    @Schema(title = "The message headers")
    List<Pair<String, String>> headers;

    @Schema(title = "The partition the message belongs to")
    Integer partition;

    @Schema(title = "The message timestamp")
    Instant timestamp;

    @Schema(title = "The message offset")
    Long offset;
}

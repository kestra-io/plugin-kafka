package io.kestra.plugin.kafka;

import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Instant;
import java.util.List;

@Builder
@Getter
public class Message implements io.kestra.core.models.tasks.Output {
    Object key;
    Object value;
    String topic;
    List<Pair<String, String>> headers;
    Integer partition;
    Instant timestamp;
    Long offset;
}

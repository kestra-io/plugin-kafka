package io.kestra.plugin.kafka;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

/**
 * Small static helpers shared by {@link AbstractKafkaAdminTask} and {@link AbstractKafkaConnectTask}, which
 * cannot share a common ancestor since one extends {@code Task} directly and the other implements a REST-backed
 * connection interface.
 */
final class KafkaTaskUtils {

    private KafkaTaskUtils() {
    }

    /**
     * Renders a required property, failing with a message naming the missing field instead of an opaque
     * {@code NoSuchElementException}.
     */
    static <T> T requireRendered(RunContext runContext, Property<T> property, Class<T> type, String fieldName) throws IllegalVariableEvaluationException {
        return runContext.render(property).as(type)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property '" + fieldName + "'"));
    }
}

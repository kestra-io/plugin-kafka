package io.kestra.plugin.kafka.admin;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.KafkaClientProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.KafkaFuture;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractKafkaAdminTask extends Task {

    @Schema(
        title = "Kafka AdminClient properties",
        description = "Must include `bootstrap.servers`; accepts any Kafka [AdminClient](https://kafka.apache.org/documentation/#adminclientconfigs) config. " +
            "Provide base64-encoded content for `ssl.keystore.location` and `ssl.truststore.location` when using SSL."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<Map<String, String>> properties;

    @Schema(
        title = "AdminClient call timeout",
        description = "Maximum duration to wait for each AdminClient operation to complete before failing the task. Defaults to `PT30S` (30 seconds)."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Duration> timeout = Property.ofValue(Duration.ofSeconds(30));

    protected Properties createAdminProperties(RunContext runContext) throws Exception {
        return KafkaClientProperties.create(this.properties, runContext);
    }

    protected Duration renderTimeout(RunContext runContext) throws IllegalVariableEvaluationException {
        return runContext.render(this.timeout).as(Duration.class).orElse(Duration.ofSeconds(30));
    }

    /**
     * Renders a required property, failing with a message naming the missing field instead of an opaque
     * {@code NoSuchElementException}.
     */
    protected static <T> T requireRendered(RunContext runContext, Property<T> property, Class<T> type, String fieldName) throws IllegalVariableEvaluationException {
        return runContext.render(property).as(type)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property '" + fieldName + "'"));
    }

    /**
     * Fails with a message naming the missing field instead of silently proceeding with an empty list.
     */
    protected static <T> List<T> requireNonEmpty(List<T> values, String fieldName) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Missing required property '" + fieldName + "'");
        }
        return values;
    }

    /**
     * Blocks on a {@link KafkaFuture} with a bounded timeout and unwraps {@link ExecutionException}
     * so callers see the actual Kafka exception (e.g. {@code UnknownTopicOrPartitionException}) instead
     * of a generic wrapper.
     */
    protected static <T> T get(KafkaFuture<T> future, Duration timeout) throws Exception {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        } catch (TimeoutException e) {
            throw new TimeoutException("AdminClient operation did not complete within " + timeout + " — increase the `timeout` property or check broker connectivity");
        }
    }
}

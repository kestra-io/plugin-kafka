package io.kestra.plugin.kafka.admin;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AbstractKafkaAdminTaskTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldUnwrapExecutionExceptionToTheRealKafkaException() {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new UnknownTopicOrPartitionException("This topic does not exist"));

        UnknownTopicOrPartitionException e = assertThrows(
            UnknownTopicOrPartitionException.class,
            () -> AbstractKafkaAdminTask.get(future, Duration.ofSeconds(5))
        );
        assertThat(e.getMessage(), is("This topic does not exist"));
    }

    @Test
    void shouldReturnValueWhenFutureCompletes() throws Exception {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        future.complete("done");

        assertThat(AbstractKafkaAdminTask.get(future, Duration.ofSeconds(5)), is("done"));
    }

    @Test
    void shouldDefaultTimeoutTo30Seconds() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        TopicList task = TopicList.builder()
            .properties(Property.ofValue(Map.of("bootstrap.servers", "localhost:9092")))
            .build();

        assertThat(task.renderTimeout(runContext), is(Duration.ofSeconds(30)));
    }
}

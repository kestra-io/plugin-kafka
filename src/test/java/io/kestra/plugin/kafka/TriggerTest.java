package io.kestra.plugin.kafka;


import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    @Value("${kafka.registry}")
    private String registry;

    @BeforeEach
    void produceMessages() throws Exception {
        var task = Produce.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Produce.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .serdeProperties(Property.ofValue(Map.of("schema.registry.url", this.registry)))
            .keySerializer(Property.ofValue(SerdeType.STRING))
            .valueSerializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue("tu_trigger"))
            .from(List.of(
                ImmutableMap.builder()
                    .put("key", "key1")
                    .put("value", "value1")
                    .build(),
                ImmutableMap.builder()
                    .put("key", "key2")
                    .put("value", "value2")
                    .build()
            ))
            .build();

        task.run(runContextFactory.of(Map.of()));
    }

    @Test
    @EvaluateTrigger(flow = "flows/trigger.yaml", triggerId = "watch")
    void flow(Optional<Execution> optionalExecution) {
        assertThat(optionalExecution.isPresent(), is(true));

        var execution = optionalExecution.get();
        assertThat(execution.getFlowId(), is("trigger"));

        var messagesCount = (Integer) execution.getTrigger().getVariables().get("messagesCount");
        assertThat(messagesCount, greaterThanOrEqualTo(2));
    }
}

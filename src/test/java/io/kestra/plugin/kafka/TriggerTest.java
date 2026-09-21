package io.kestra.plugin.kafka;


import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

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

    @Test
    void shouldUnblockInFlightEvaluateOnKill() throws Exception {
        // Empty, never-produced-to topic: the underlying Consume's first poll() has nothing to
        // return and blocks for the full pollDuration unless kill() wakes it up.
        var topic = "tu_trigger_kill_" + IdUtils.create();
        var groupId = "tu_trigger_kill_group_" + IdUtils.create();

        Trigger trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .topic(topic)
            .groupId(Property.ofValue(groupId))
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();

        var completed = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>();
        Thread runner = new Thread(() -> {
            try {
                trigger.evaluate(conditionContext, null);
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        // Give evaluate() time to build its Consume task, subscribe, and enter the blocking poll().
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        long killStart = System.currentTimeMillis();
        trigger.kill();
        long killElapsedMs = System.currentTimeMillis() - killStart;

        assertThat("Trigger.kill() must not block for the full pollDuration", killElapsedMs, lessThan(15000L));
        assertThat("evaluate() must return promptly after kill()", completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A killed evaluate() must not be reported as a clean poll result", thrown.get(), notNullValue());
    }
}

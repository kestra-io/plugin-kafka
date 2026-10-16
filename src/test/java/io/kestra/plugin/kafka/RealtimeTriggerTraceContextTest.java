package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kafka.models.TraceContext;
import jakarta.inject.Inject;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNull;

@KestraTest
class RealtimeTriggerTraceContextTest {
    private static final String VALID_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Inject
    private RunContextFactory runContextFactory;

    @SuperBuilder
    @ToString
    @EqualsAndHashCode(callSuper = true)
    @Getter
    @NoArgsConstructor
    public static class TestableRealtimeTrigger extends RealtimeTrigger {
        private ConsumerRecord<Object, Object> recordToEmit;

        @Override
        public Publisher<ConsumerRecord<Object, Object>> publisher(Consume task, RunContext runContext) {
            return Flux.just(recordToEmit);
        }
    }

    @Test
    @DisplayName("Should not extract or propagate trace context when disabled by default")
    void shouldNotPropagateWhenDisabled() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("trigger-default")
            .type(RealtimeTrigger.class.getName())
            .topic("test-topic")
            .build();

        var headers = new RecordHeaders();
        headers.add("traceparent", VALID_TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<Object, Object>("test-topic", 0, 1L, "key", "val");
        headers.forEach(h -> record.headers().add(h));
        Message message = Message.builder().headers(Consume.processHeaders(record.headers())).build();

        Execution execution = Execution.builder().id("exec-1").build();
        trigger.propagateTraceContext(runContext, record, message, execution);

        assertNull(execution.getTraceParent());
    }

    @Test
    @DisplayName("Should extract and propagate valid traceparent when enabled")
    void shouldPropagateWhenEnabled() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("trigger-enabled")
            .type(RealtimeTrigger.class.getName())
            .topic("test-topic")
            .traceContext(TraceContext.builder()
                .enabled(Property.ofValue(true))
                .build())
            .build();

        var headers = new RecordHeaders();
        headers.add("traceparent", VALID_TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<Object, Object>("test-topic", 0, 1L, "key", "val");
        headers.forEach(h -> record.headers().add(h));
        Message message = Message.builder().headers(Consume.processHeaders(record.headers())).build();

        Execution execution = Execution.builder().id("exec-2").build();
        trigger.propagateTraceContext(runContext, record, message, execution);

        assertThat(execution.getTraceParent(), is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should not fail execution when header has malformed traceparent")
    void shouldHandleMalformedGracefully() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("trigger-malformed")
            .type(RealtimeTrigger.class.getName())
            .topic("test-topic")
            .traceContext(TraceContext.builder()
                .enabled(Property.ofValue(true))
                .build())
            .build();

        var headers = new RecordHeaders();
        headers.add("traceparent", "malformed-value".getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<Object, Object>("test-topic", 0, 1L, "key", "val");
        headers.forEach(h -> record.headers().add(h));
        Message message = Message.builder().headers(Consume.processHeaders(record.headers())).build();

        Execution execution = Execution.builder().id("exec-3").build();
        trigger.propagateTraceContext(runContext, record, message, execution);

        assertNull(execution.getTraceParent());
    }

    @Test
    @DisplayName("Should extract traceparent from message body when bodyExtractor is configured")
    void shouldExtractFromBodyWhenConfigured() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("trigger-body")
            .type(RealtimeTrigger.class.getName())
            .topic("test-topic")
            .traceContext(TraceContext.builder()
                .enabled(Property.ofValue(true))
                .bodyExtractor(Property.ofValue("trace.parent"))
                .build())
            .build();

        Map<String, Object> bodyMap = Map.of("trace", Map.of("parent", VALID_TRACEPARENT));
        var record = new ConsumerRecord<Object, Object>("test-topic", 0, 1L, "key", bodyMap);
        Message message = Message.builder().value(bodyMap).build();

        Execution execution = Execution.builder().id("exec-4").build();
        trigger.propagateTraceContext(runContext, record, message, execution);

        assertThat(execution.getTraceParent(), is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should extract and propagate traceparent when evaluated through trigger.evaluate()")
    void shouldPropagateThroughEvaluateWhenEnabled() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Flow flow = Flow.builder()
            .namespace("io.kestra.tests")
            .id("realtime_flow")
            .revision(1)
            .build();
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("realtime_flow")
            .triggerId("realtime_trigger")
            .date(ZonedDateTime.now())
            .build();

        var headers = new RecordHeaders();
        headers.add("traceparent", VALID_TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<Object, Object>("test-topic", 0, 1L, "key", "val");
        headers.forEach(h -> record.headers().add(h));

        TestableRealtimeTrigger trigger = TestableRealtimeTrigger.builder()
            .id("trigger-evaluate-enabled")
            .type(RealtimeTrigger.class.getName())
            .topic("test-topic")
            .traceContext(TraceContext.builder()
                .enabled(Property.ofValue(true))
                .build())
            .recordToEmit(record)
            .build();

        Execution execution = Flux.from(trigger.evaluate(conditionContext, triggerContext)).blockFirst();

        assertThat(execution, notNullValue());
        assertThat(execution.getTraceParent(), is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should not propagate traceparent through evaluate() when traceContext is disabled")
    void shouldNotPropagateThroughEvaluateWhenDisabled() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Flow flow = Flow.builder()
            .namespace("io.kestra.tests")
            .id("realtime_flow")
            .revision(1)
            .build();
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("io.kestra.tests")
            .flowId("realtime_flow")
            .triggerId("realtime_trigger")
            .date(ZonedDateTime.now())
            .build();

        var headers = new RecordHeaders();
        headers.add("traceparent", VALID_TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<Object, Object>("test-topic", 0, 1L, "key", "val");
        headers.forEach(h -> record.headers().add(h));

        TestableRealtimeTrigger trigger = TestableRealtimeTrigger.builder()
            .id("trigger-evaluate-disabled")
            .type(RealtimeTrigger.class.getName())
            .topic("test-topic")
            .recordToEmit(record)
            .build();

        Execution execution = Flux.from(trigger.evaluate(conditionContext, triggerContext)).blockFirst();

        assertThat(execution, notNullValue());
        assertNull(execution.getTraceParent());
    }
}

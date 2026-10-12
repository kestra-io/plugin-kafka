package io.kestra.plugin.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

class KafkaTraceContextExtractorTest {
    private static final String VALID_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KafkaTraceContextExtractorTest.class);

    @Test
    @DisplayName("Should validate valid W3C traceparent correctly")
    void shouldValidateValidTraceparent() {
        assertTrue(KafkaTraceContextExtractor.isValidTraceParent(VALID_TRACEPARENT));
        assertTrue(KafkaTraceContextExtractor.isValidTraceParent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"));
    }

    @Test
    @DisplayName("Should reject invalid or malformed W3C traceparents")
    void shouldRejectMalformedTraceparent() {
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent(null));
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent(""));
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent("invalid"));
        // Version ff is invalid
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
        // Trace-id all zeros is invalid
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"));
        // Parent-id all zeros is invalid
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"));
        // Bad segment length
        assertFalse(KafkaTraceContextExtractor.isValidTraceParent("00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01"));
    }

    @Test
    @DisplayName("Should extract traceparent from ConsumerRecord headers")
    void shouldExtractFromRecordHeaders() {
        var headers = new RecordHeaders();
        headers.add("traceparent", VALID_TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<>("test-topic", 0, 1L, "key", "value");
        headers.forEach(h -> record.headers().add(h));

        String extracted = KafkaTraceContextExtractor.extractTraceParent(record, null, "traceparent", null, LOGGER);
        assertThat(extracted, is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should extract traceparent with custom header name")
    void shouldExtractWithCustomHeader() {
        var headers = new RecordHeaders();
        headers.add("custom-trace-id", VALID_TRACEPARENT.getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<>("test-topic", 0, 1L, "key", "value");
        headers.forEach(h -> record.headers().add(h));

        String extracted = KafkaTraceContextExtractor.extractTraceParent(record, null, "custom-trace-id", null, LOGGER);
        assertThat(extracted, is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should gracefully handle malformed header without failing")
    void shouldHandleMalformedHeaderGracefully() {
        var headers = new RecordHeaders();
        headers.add("traceparent", "malformed-traceparent".getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<>("test-topic", 0, 1L, "key", "value");
        headers.forEach(h -> record.headers().add(h));

        String extracted = KafkaTraceContextExtractor.extractTraceParent(record, null, "traceparent", null, LOGGER);
        assertNull(extracted);
    }

    @Test
    @DisplayName("Should extract traceparent from Map body when bodyExtractor is set")
    void shouldExtractFromMapBody() {
        var record = new ConsumerRecord<>("test-topic", 0, 1L, "key", Map.of(
            "trace", Map.of("parent", VALID_TRACEPARENT)
        ));

        String extracted = KafkaTraceContextExtractor.extractTraceParent(record, null, "traceparent", "trace.parent", LOGGER);
        assertThat(extracted, is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should extract traceparent from JSON String body when bodyExtractor is set")
    void shouldExtractFromJsonStringBody() {
        String json = "{\"traceparent\": \"" + VALID_TRACEPARENT + "\", \"user\": \"test\"}";
        var record = new ConsumerRecord<>("test-topic", 0, 1L, "key", json);

        String extracted = KafkaTraceContextExtractor.extractTraceParent(record, null, "traceparent", "traceparent", LOGGER);
        assertThat(extracted, is(VALID_TRACEPARENT));
    }

    @Test
    @DisplayName("Should return null if neither headers nor body contain trace context")
    void shouldReturnNullWhenMissing() {
        var record = new ConsumerRecord<>("test-topic", 0, 1L, "key", "hello world");
        String extracted = KafkaTraceContextExtractor.extractTraceParent(record, null, "traceparent", null, LOGGER);
        assertNull(extracted);
    }
}

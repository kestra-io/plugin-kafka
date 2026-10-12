package io.kestra.plugin.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.serializers.JacksonMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class KafkaTraceContextExtractor {
    // W3C TraceContext specification traceparent regex:
    // version (00) - trace-id (32 hex) - parent-id (16 hex) - trace-flags (2 hex)
    // All-zero trace-id or parent-id is invalid per W3C specification.
    private static final Pattern W3C_TRACEPARENT_PATTERN = Pattern.compile(
        "^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Extracts traceparent from ConsumerRecord headers first, then optionally from body.
     * Never throws exceptions; returns null if not found or malformed.
     */
    public static String extractTraceParent(
        ConsumerRecord<?, ?> record,
        Message message,
        String traceparentHeaderKey,
        String bodyExtractorPath,
        Logger logger
    ) {
        // 1. Try extracting from Kafka headers
        String traceParentFromHeaders = extractFromHeaders(record, message, traceparentHeaderKey, logger);
        if (traceParentFromHeaders != null) {
            return traceParentFromHeaders;
        }

        // 2. Try extracting from body if bodyExtractorPath is defined
        if (bodyExtractorPath != null && !bodyExtractorPath.isBlank()) {
            return extractFromBody(record, message, bodyExtractorPath.trim(), logger);
        }

        return null;
    }

    private static String extractFromHeaders(
        ConsumerRecord<?, ?> record,
        Message message,
        String headerKey,
        Logger logger
    ) {
        String key = (headerKey != null && !headerKey.isBlank()) ? headerKey : "traceparent";

        // Check ConsumerRecord headers first
        if (record != null && record.headers() != null) {
            Header header = record.headers().lastHeader(key);
            if (header != null && header.value() != null) {
                String candidate = new String(header.value(), StandardCharsets.UTF_8).trim();
                return validateAndReturn(candidate, "Kafka header '" + key + "'", logger);
            }
        }

        // Check Message headers as fallback
        if (message != null && message.getHeaders() != null) {
            List<Pair<String, String>> headers = message.getHeaders();
            for (int i = headers.size() - 1; i >= 0; i--) {
                Pair<String, String> pair = headers.get(i);
                if (key.equalsIgnoreCase(pair.getKey()) && pair.getValue() != null) {
                    return validateAndReturn(pair.getValue().trim(), "Message header '" + key + "'", logger);
                }
            }
        }

        return null;
    }

    private static String extractFromBody(
        ConsumerRecord<?, ?> record,
        Message message,
        String path,
        Logger logger
    ) {
        Object body = (message != null && message.getValue() != null) ? message.getValue() : (record != null ? record.value() : null);
        if (body == null) {
            if (logger != null && logger.isDebugEnabled()) {
                logger.debug("Cannot extract trace context from empty message body for path '{}'", path);
            }
            return null;
        }

        try {
            Map<?, ?> map = null;
            if (body instanceof Map<?, ?> mapBody) {
                map = mapBody;
            } else if (body instanceof String strBody) {
                String trimmed = strBody.trim();
                if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                    map = JacksonMapper.ofJson().readValue(trimmed, new TypeReference<Map<String, Object>>() {});
                }
            }

            if (map != null) {
                Object extracted = findNestedValue(map, path);
                if (extracted != null) {
                    return validateAndReturn(extracted.toString().trim(), "body path '" + path + "'", logger);
                }
            }
        } catch (Exception e) {
            if (logger != null && logger.isDebugEnabled()) {
                logger.debug("Failed to extract trace context from body at path '{}': {}", path, e.getMessage());
            }
        }

        return null;
    }

    private static Object findNestedValue(Map<?, ?> map, String path) {
        String[] parts = path.split("\\.");
        Object current = map;
        for (String part : parts) {
            if (current instanceof Map<?, ?> currentMap) {
                current = currentMap.get(part);
            } else {
                return null;
            }
        }
        return current;
    }

    public static boolean isValidTraceParent(String traceParent) {
        if (traceParent == null) {
            return false;
        }
        var matcher = W3C_TRACEPARENT_PATTERN.matcher(traceParent);
        if (!matcher.matches()) {
            return false;
        }
        String version = matcher.group(1);
        String traceId = matcher.group(2);
        String parentId = matcher.group(3);

        // Version ff is forbidden by W3C specification
        if ("ff".equalsIgnoreCase(version)) {
            return false;
        }

        // Trace ID cannot be all zeros
        if ("00000000000000000000000000000000".equals(traceId)) {
            return false;
        }

        // Parent ID cannot be all zeros
        if ("0000000000000000".equals(parentId)) {
            return false;
        }

        return true;
    }

    private static String validateAndReturn(String candidate, String sourceDescription, Logger logger) {
        if (isValidTraceParent(candidate)) {
            if (logger != null && logger.isDebugEnabled()) {
                logger.debug("Successfully extracted valid W3C traceparent '{}' from {}", candidate, sourceDescription);
            }
            return candidate;
        } else {
            if (logger != null && logger.isDebugEnabled()) {
                logger.debug("Ignoring invalid or malformed W3C traceparent '{}' from {}", candidate, sourceDescription);
            }
            return null;
        }
    }
}

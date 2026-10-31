package io.kestra.plugin.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.serializers.JacksonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class KafkaTraceContextExtractor {
    // W3C Trace Context traceparent format: version-trace_id-parent_id-trace_flags
    private static final Pattern W3C_TRACEPARENT_PATTERN = Pattern.compile(
        "^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$",
        Pattern.CASE_INSENSITIVE
    );

    private static final int MAX_LOGGED_PREFIX = 16;

    public static String extractTraceParent(
        ConsumerRecord<?, ?> record,
        Message message,
        String traceparentHeaderKey,
        String bodyExtractorPath,
        Logger logger
    ) {
        var traceParentFromHeaders = extractFromHeaders(record, message, traceparentHeaderKey, logger);
        if (traceParentFromHeaders != null) {
            return traceParentFromHeaders;
        }

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
        var key = (headerKey != null && !headerKey.isBlank()) ? headerKey : "traceparent";

        if (record != null && record.headers() != null) {
            var header = record.headers().lastHeader(key);
            if (header != null && header.value() != null) {
                var candidate = new String(header.value(), StandardCharsets.UTF_8).trim();
                return validateAndReturn(candidate, "Kafka header '" + key + "'", logger);
            }
        }

        if (message != null && message.getHeaders() != null) {
            var headers = message.getHeaders();
            for (int i = headers.size() - 1; i >= 0; i--) {
                var pair = headers.get(i);
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
        var body = (message != null && message.getValue() != null) ? message.getValue() : (record != null ? record.value() : null);
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
                var trimmed = strBody.trim();
                if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
                    map = JacksonMapper.ofJson().readValue(trimmed, new TypeReference<Map<String, Object>>() {});
                }
            }

            if (map != null) {
                var extracted = findNestedValue(map, path);
                if (extracted != null) {
                    return validateAndReturn(extracted.toString().trim(), "body path '" + path + "'", logger);
                }
            }
        } catch (Exception e) {
            if (logger != null && logger.isDebugEnabled()) {
                // Jackson messages can echo fragments of the (untrusted) body, so never log them raw
                logger.debug("Failed to extract trace context from body at path '{}': {} ({})", path, e.getClass().getSimpleName(), describeForLog(e.getMessage()));
            }
        }

        return null;
    }

    private static Object findNestedValue(Map<?, ?> map, String path) {
        var parts = path.split("\\.");
        Object current = map;
        for (var part : parts) {
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
        var version = matcher.group(1);
        var traceId = matcher.group(2);
        var parentId = matcher.group(3);

        if ("ff".equalsIgnoreCase(version)) {
            return false;
        }

        if ("00000000000000000000000000000000".equals(traceId)) {
            return false;
        }

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
                logger.debug("Ignoring invalid or malformed W3C traceparent ({}) from {}", describeForLog(candidate), sourceDescription);
            }
            return null;
        }
    }

    /**
     * Describes an untrusted value for logs: its length and a short prefix with control characters
     * (CR, LF, ...) replaced, so a record writer cannot inject log lines or flood the logs.
     */
    static String describeForLog(String value) {
        if (value == null) {
            return "null";
        }
        var prefix = value.substring(0, Math.min(value.length(), MAX_LOGGED_PREFIX));
        var sanitized = new StringBuilder(prefix.length());
        for (int i = 0; i < prefix.length(); i++) {
            var c = prefix.charAt(i);
            sanitized.append(Character.isISOControl(c) || Character.isWhitespace(c) && c != ' ' ? '_' : c);
        }
        return "length=" + value.length() + ", prefix='" + sanitized + "'";
    }
}

package io.kestra.plugin.kafka;

import lombok.Getter;

/**
 * Raised for any Kafka Connect REST API call that could not be completed as requested: an unreachable worker,
 * a non-2xx response, or an empty response body where one was expected. {@link #getStatusCode()} is {@code -1}
 * when the worker itself could not be reached (no HTTP response was received).
 */
@Getter
public class KafkaConnectApiException extends RuntimeException {
    private final int statusCode;
    private final String responseBody;

    public KafkaConnectApiException(String message, int statusCode, String responseBody) {
        super(message);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public KafkaConnectApiException(String message, int statusCode, String responseBody, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }
}

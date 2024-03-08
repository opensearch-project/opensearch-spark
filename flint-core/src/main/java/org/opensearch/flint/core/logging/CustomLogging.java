/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * CustomLogging provides a structured logging framework that supports various log levels
 * and formats log messages as JSON. This new log format follows OTEL convention
 * https://opentelemetry.io/docs/specs/semconv/general/logs/
 */
public class CustomLogging {
    private static final Logger logger = LogManager.getLogger(CustomLogging.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String CLIENT_ID;
    private static final String DOMAIN_NAME;
    private static final String UNKNOWN = "UNKNOWN";

    /**
     * Default severity level follows https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber
     */
    private static final Map<String, Integer> severityLevelMap = Map.of(
            "TRACE", 1,
            "DEBUG", 5,
            "INFO", 9,
            "WARN", 13,
            "ERROR", 17,
            "FATAL", 21
    );
    private static final BiConsumer<String, Throwable> defaultLogAction = logger::info;
    private static final Map<String, BiConsumer<String, Throwable>> logLevelActions = new HashMap<>();

    static {
        String[] parts = System.getenv().getOrDefault("FLINT_CLUSTER_NAME", UNKNOWN + ":" + UNKNOWN).split(":");
        CLIENT_ID = parts.length == 2 ? parts[0] : UNKNOWN;
        DOMAIN_NAME = parts.length == 2 ? parts[1] : UNKNOWN;

        logLevelActions.put("DEBUG", logger::debug);
        logLevelActions.put("INFO", logger::info);
        logLevelActions.put("WARN", logger::warn);
        logLevelActions.put("ERROR", logger::error);
        logLevelActions.put("FATAL", logger::fatal);
    }

    private static int getSeverityNumber(String level) {
        return severityLevelMap.getOrDefault(level, 0);
    }

    private static String convertToJson(Map<String, Object> logEventMap) {
        try {
            return OBJECT_MAPPER.writeValueAsString(logEventMap);
        } catch (JsonProcessingException e) {
            System.err.println("Error serializing log event to JSON: " + e.getMessage());
            return "{\"Error\":\"Error serializing log event\"}";
        }
    }

    /**
     * Constructs a log event map containing log details such as timestamp, severity,
     * message body, and custom attributes including domainName and clientId.
     *
     * @param level     The severity level of the log.
     * @param content   The main content of the log message.
     * @param throwable An optional Throwable associated with log messages for error levels.
     * @return A map representation of the log event.
     */
    protected static Map<String, Object> constructLogEventMap(String level, Object content, Throwable throwable) {
        if (content == null) {
            throw new IllegalArgumentException("Log message must not be null");
        }

        Map<String, Object> logEventMap = new LinkedHashMap<>();
        Map<String, Object> body = new LinkedHashMap<>();
        constructMessageBody(content, body);

        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("domainName", DOMAIN_NAME);
        attributes.put("clientId", CLIENT_ID);

        if (throwable != null) {
            attributes.put("exception.type", throwable.getClass().getName());
            attributes.put("exception.message", throwable.getMessage());
        }

        logEventMap.put("timestamp", System.currentTimeMillis());
        logEventMap.put("severityText", level);
        logEventMap.put("severityNumber", getSeverityNumber(level));
        logEventMap.put("body", body);
        logEventMap.put("attributes", attributes);

        return logEventMap;
    }

    private static void constructMessageBody(Object content, Map<String, Object> body) {
        if (content instanceof Message) {
            Message message = (Message) content;
            body.put("message", message.getFormattedMessage());
            if (content instanceof OperationMessage && message.getParameters().length > 0) {
                body.put("statusCode", message.getParameters()[0]);
            }
        } else {
            body.put("message", content.toString());
        }
    }

    /**
     * Logs a message with the specified severity level, message content, and an optional throwable.
     *
     * @param level     The severity level of the log.
     * @param content   The content of the log message.
     * @param throwable An optional Throwable for logging errors or exceptions.
     */
    private static void log(String level, Object content, Throwable throwable) {
        Map<String, Object> logEventMap = constructLogEventMap(level, content, throwable);
        String jsonMessage = convertToJson(logEventMap);
        logLevelActions.getOrDefault(level, defaultLogAction).accept(jsonMessage, throwable);
    }

    // Public logging methods for various severity levels.

    public static void logDebug(Object message) {
        log("DEBUG", message, null);
    }

    public static void logInfo(Object message) {
        log("INFO", message, null);
    }

    public static void logWarning(Object message) {
        log("WARN", message, null);
    }

    public static void logWarning(Object message, Throwable e) {
        log("WARN", message, e);
    }

    public static void logError(Object message) {
        log("ERROR", message, null);
    }

    public static void logError(Object message, Throwable throwable) {
        log("ERROR", message, throwable);
    }

    public static void logFatal(Object message) {
        log("FATAL", message, null);
    }

    public static void logFatal(Object message, Throwable throwable) {
        log("FATAL", message, throwable);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * CustomJsonLayout is a plugin for formatting log events as JSON strings.
 *
 * <p>The layout is designed to be used with OpenSearch Flint logging. It extracts environment-specific information,
 * such as the cluster name, from the environment variable "FLINT_CLUSTER_NAME" and splits it into domain name and client ID.</p>
 *
 */
@Plugin(name = "CustomJsonLayout", category = "Core", elementType = Layout.ELEMENT_TYPE, printObject = true)
public class CustomJsonLayout extends AbstractStringLayout {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String CLIENT_ID;
    private static final String DOMAIN_NAME;
    private static final String UNKNOWN = "UNKNOWN";

    static {
        String value = System.getenv().getOrDefault("FLINT_CLUSTER_NAME", "");
        String[] parts = value.split(":");
        if (value.isEmpty() || parts.length != 2) {
            CLIENT_ID = UNKNOWN;
            DOMAIN_NAME = UNKNOWN;
        } else {
            CLIENT_ID = parts[0];
            DOMAIN_NAME = parts[1];
        }
    }

    protected CustomJsonLayout(Charset charset) {
        super(charset);
    }

    /**
     * Plugin factory method to create an instance of CustomJsonLayout.
     *
     * @param charset The charset for encoding the log event. If not specified, defaults to UTF-8.
     * @return A new instance of CustomJsonLayout with the specified charset.
     */
    @PluginFactory
    public static CustomJsonLayout createLayout(@PluginAttribute(value = "charset", defaultString = "UTF-8")  Charset charset) {
        return new CustomJsonLayout(charset);
    }

    /**
     * Converts the log event to a JSON string.
     * If the log event's message does not follow the expected format, it returns the formatted message directly.
     *
     * @param event the log event to format.
     * @return A string representation of the log event in JSON format.
     */
    @Override
    public String toSerializable(LogEvent event) {
        Map<String, Object> logEventMap = new HashMap<>();
        logEventMap.put("timestamp", event.getTimeMillis());
        logEventMap.put("message", event.getMessage().getFormattedMessage());
        logEventMap.put("domainName", DOMAIN_NAME);
        logEventMap.put("clientId", CLIENT_ID);

        if (event.getMessage() instanceof OperationMessage && event.getMessage().getParameters().length == 1) {
            logEventMap.put("StatusCode", event.getMessage().getParameters()[0]);
        }

        Throwable throwable = event.getThrown();
        if (throwable != null) {
            logEventMap.put("Exception", throwable.getClass().getName());
            logEventMap.put("ExceptionMessage", throwable.getMessage());
        }

        return convertToJson(logEventMap) + System.lineSeparator();
    }

    private String convertToJson(Map<String, Object> logEventMap) {
        try {
            return OBJECT_MAPPER.writeValueAsString(logEventMap);
        } catch (JsonProcessingException e) {
            // Logging this error using System.err to avoid recursion
            System.err.println("Error serializing log event to JSON: " + e.getMessage());
            return "{\"Error\":\"Error serializing log event\"}";
        }
    }
}
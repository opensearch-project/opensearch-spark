/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import org.apache.logging.log4j.message.Message;

/**
 * Represents an operation message with optional status code for logging purposes.
 */
public final class OperationMessage implements Message {
    private final String message;
    private final Integer statusCode;

    /**
     * Constructs an OperationMessage without a status code.
     *
     * @param message The message content.
     */
    public OperationMessage(String message) {
        this(message, null);
    }

    /**
     * Constructs an OperationMessage with a message and an optional status code.
     *
     * @param message The message content.
     * @param statusCode An optional status code, can be null.
     */
    public OperationMessage(String message, Integer statusCode) {
        this.message = message;
        this.statusCode = statusCode;
    }

    @Override
    public String getFormattedMessage() {
        return message;
    }

    @Override
    public String getFormat() {
        return message;
    }

    @Override
    public Object[] getParameters() {
        // Always return an array, even if empty, to avoid null checks on the consuming side
        return statusCode != null ? new Object[]{statusCode} : new Object[]{};
    }

    @Override
    public Throwable getThrowable() {
        // This implementation does not support Throwable directly
        return null;
    }
}

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
    private final String errorCode;
    private final String exceptionType;
    private final String requestId;
    private final String extendedRequestId;

    /**
     * Constructs an OperationMessage without structured error context.
     *
     * @param message The message content.
     */
    public OperationMessage(String message) {
        this(message, null);
    }

    /**
     * Constructs an OperationMessage with an optional status code.
     *
     * @param message The message content.
     * @param statusCode An optional status code, can be null.
     */
    public OperationMessage(String message, Integer statusCode) {
        this(message, statusCode, null, null, null, null);
    }

    /**
     * Constructs an OperationMessage with typed, non-customer-derived error context.
     */
    public OperationMessage(
            String message,
            Integer statusCode,
            String errorCode,
            String exceptionType,
            String requestId,
            String extendedRequestId) {
        this.message = message;
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.exceptionType = exceptionType;
        this.requestId = requestId;
        this.extendedRequestId = extendedRequestId;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getExceptionType() {
        return exceptionType;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getExtendedRequestId() {
        return extendedRequestId;
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
        // Preserve the existing Message contract for callers that inspect status through parameters.
        return statusCode != null ? new Object[]{statusCode} : new Object[]{};
    }

    @Override
    public Throwable getThrowable() {
        return null;
    }
}

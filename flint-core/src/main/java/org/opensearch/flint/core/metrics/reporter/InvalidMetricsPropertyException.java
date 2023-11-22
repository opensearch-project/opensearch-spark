/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics.reporter;


import java.io.Serializable;

public class InvalidMetricsPropertyException extends RuntimeException implements Serializable {

    public InvalidMetricsPropertyException(final String message) {
        super(message);
    }

    public InvalidMetricsPropertyException(final String message, final Throwable cause) {
        super(message, cause);
    }
}

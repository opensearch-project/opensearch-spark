/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.flint.core.logging.CustomLogging;
import org.opensearch.flint.core.metrics.MetricsUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class IRestHighLevelClientTest {

    private static final String PREFIX = "testPrefix";

    @Test
    public void recordOperationFailureParsesStatusCodeFromRuntimeExceptionMessage() {
        // A 403 wrapped in a plain RuntimeException, with the status code only present as text.
        Throwable t = new RuntimeException("Access denied; Status Code: 403; Error Code: Forbidden");

        try (MockedStatic<MetricsUtil> metricsUtil = mockStatic(MetricsUtil.class);
             MockedStatic<CustomLogging> ignored = mockStatic(CustomLogging.class)) {
            IRestHighLevelClient.recordOperationFailure(PREFIX, t);

            // Must be classified as 403/4xx, NOT 5xx.
            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".403.count"), times(1));
            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".4xx.count"), times(1));
            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".5xx.count"), never());
        }
    }

    @Test
    public void recordOperationFailureParsesStatusCodeFromNestedCause() {
        Throwable cause = new RuntimeException("downstream failure; Status Code: 429; Error Code: TooManyRequests");
        Throwable t = new RuntimeException("wrapper", cause);

        try (MockedStatic<MetricsUtil> metricsUtil = mockStatic(MetricsUtil.class);
             MockedStatic<CustomLogging> ignored = mockStatic(CustomLogging.class)) {
            IRestHighLevelClient.recordOperationFailure(PREFIX, t);

            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".429.count"), times(1));
            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".4xx.count"), times(1));
            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".5xx.count"), never());
        }
    }

    @Test
    public void recordOperationFailureDefaultsTo5xxWhenNoStatusCodeInMessage() {
        Throwable t = new RuntimeException("some unexpected failure without a status code");

        try (MockedStatic<MetricsUtil> metricsUtil = mockStatic(MetricsUtil.class);
             MockedStatic<CustomLogging> ignored = mockStatic(CustomLogging.class)) {
            IRestHighLevelClient.recordOperationFailure(PREFIX, t);

            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".5xx.count"), times(1));
            metricsUtil.verify(() -> MetricsUtil.incrementCounter(PREFIX + ".4xx.count"), never());
        }
    }

    @Test
    public void extractStatusCodeFromMessageReturnsDefaultForNullMessage() {
        assertEquals(500, IRestHighLevelClient.extractStatusCodeFromMessage(new RuntimeException()));
    }

    @Test
    public void extractStatusCodeFromMessageParsesCode() {
        Throwable t = new RuntimeException("... Status Code: 404; ...");
        assertEquals(404, IRestHighLevelClient.extractStatusCodeFromMessage(t));
    }
}

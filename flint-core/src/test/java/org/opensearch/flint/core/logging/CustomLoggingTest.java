/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CustomLoggingTest {
    private static final String testMsg = "Test message";

    @Parameterized.Parameter(0)
    public Object content;

    @Parameterized.Parameter(1)
    public String expectedMessage;

    @Parameterized.Parameter(2)
    public Object expectedStatusCode;

    @Parameterized.Parameter(3)
    public String severityLevel;

    @Parameterized.Parameter(4)
    public Throwable throwable;

    @Parameterized.Parameters(name = "{index}: Test with content={0}, expectedMessage={1}, expectedStatusCode={2}, severityLevel={3}, throwable={4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {testMsg, testMsg, null, "INFO", null},
                {new SimpleMessage(testMsg), testMsg, null, "DEBUG", null},
                {new OperationMessage(testMsg, 403), testMsg, 403, "ERROR", new RuntimeException("Test Exception")},
                {testMsg, testMsg, null, "UNDEFINED_LEVEL", null}, // Test with an undefined severity level
                {new SimpleMessage(testMsg), testMsg, null, "INFO", new Exception("New Exception")},
                {testMsg, testMsg, null, "WARN", null},
                {"", "", null, "INFO", null},
                {new SimpleMessage(testMsg), testMsg, null, "FATAL", new Error("Critical Error")},
        });
    }

    @Test
    public void testConstructLogEventMap() {
        Map<String, Object> logEventMap = CustomLogging.constructLogEventMap(severityLevel, content, throwable);

        assertEquals("Incorrect severity text", severityLevel, logEventMap.get("severityText"));
        assertTrue("Timestamp should be present and greater than 0", (Long)logEventMap.get("timestamp") > 0);
        assertNotNull("Severity number should not be null", logEventMap.get("severityNumber"));

        Map<String, Object> body = (Map<String, Object>) logEventMap.get("body");
        assertEquals("Expected message value not found", expectedMessage, body.get("message"));
        if (expectedStatusCode != null) {
            assertEquals("Expected status code not found", expectedStatusCode, body.get("statusCode"));
        } else {
            assertNull("Status code should be null", body.get("statusCode"));
        }

        if (throwable != null) {
            Map<String, Object> attributes = (Map<String, Object>) logEventMap.get("attributes");
            assertEquals("Exception type should match", throwable.getClass().getName(), attributes.get("exception.type"));
            assertEquals("Exception message should match", throwable.getMessage(), attributes.get("exception.message"));
        }
    }
}

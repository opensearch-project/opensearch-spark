/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class CustomJsonLayoutTest {
    @Test
    public void testSuccessfulLogPlainMessage() {
        LogEvent event = mock(LogEvent.class);
        when(event.getMessage()).thenReturn(mock(Message.class));
        when(event.getMessage().getFormattedMessage()).thenReturn("Test message");

        CustomJsonLayout layout = CustomJsonLayout.createLayout(StandardCharsets.UTF_8);
        String result = layout.toSerializable(event);
        assertNotNull(result);
        assertEquals(result, "Test message" + System.lineSeparator());
    }

    @Test
    public void testSuccessfulLogOperationMessage() throws NoSuchFieldException, IllegalAccessException {
        Class<?> classOfMap = System.getenv().getClass();
        Field field = classOfMap.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writeableEnvironmentVariables = (Map<String, String>)field.get(System.getenv());
        writeableEnvironmentVariables.put("FLINT_CLUSTER_NAME", "1234567890:testDomain");

        LogEvent event = mock(LogEvent.class);
        when(event.getTimeMillis()).thenReturn(System.currentTimeMillis());
        when(event.getMessage()).thenReturn(mock(OperationMessage.class));
        when(event.getMessage().getFormattedMessage()).thenReturn("Test message");
        when(event.getMessage().getParameters()).thenReturn(new Object[] {new Integer(200)});

        try {
            CustomJsonLayout layout = CustomJsonLayout.createLayout(StandardCharsets.UTF_8);

            String result = layout.toSerializable(event);
            assertNotNull(result);
            assertTrue(result.contains("\"message\":\"Test message\""));
            assertTrue(result.contains("\"clientId\":\"1234567890\""));
            assertTrue(result.contains("\"domainName\":\"testDomain\""));
            assertTrue(result.contains("\"StatusCode\":200"));
        } finally {
            // since system environment is shared by other tests. Make sure to remove them before exiting.
            writeableEnvironmentVariables.remove("FLINT_CLUSTER_NAME");
        }
    }
}

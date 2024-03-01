package org.opensearch.flint.core.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomJsonLayoutTest {
    private Map<String, String> writeableEnvironmentVariables;

    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        // Setup writable environment variables
        Class<?> classOfMap = System.getenv().getClass();
        Field field = classOfMap.getDeclaredField("m");
        field.setAccessible(true);
        writeableEnvironmentVariables = (Map<String, String>) field.get(System.getenv());
        writeableEnvironmentVariables.put("FLINT_CLUSTER_NAME", "1234567890:testDomain");
    }

    private void cleanUp() {
        // Clean up environment variables
        writeableEnvironmentVariables.remove("FLINT_CLUSTER_NAME");
    }

    private LogEvent setupLogEvent(Class<? extends Message> messageClass, String formattedMessage, Object... parameters) {
        LogEvent event = mock(LogEvent.class);
        when(event.getTimeMillis()).thenReturn(System.currentTimeMillis());

        Message message = mock(messageClass);
        when(message.getFormattedMessage()).thenReturn(formattedMessage);
        if (parameters.length > 0) {
            when(message.getParameters()).thenReturn(parameters);
        }
        when(event.getMessage()).thenReturn(message);

        Exception mockException = new Exception("Test exception message");
        when(event.getThrown()).thenReturn(mockException);

        return event;
    }

    @Test
    public void testSuccessfulLogPlainMessage() {
        LogEvent event = setupLogEvent(Message.class, "Test message");

        try {
            CustomJsonLayout layout = CustomJsonLayout.createLayout(StandardCharsets.UTF_8);

            String result = layout.toSerializable(event);
            assertNotNull(result);
            assertTrue(result.contains("\"message\":\"Test message\""));
            assertTrue(result.contains("\"clientId\":\"1234567890\""));
            assertTrue(result.contains("\"domainName\":\"testDomain\""));
            assertFalse(result.contains("\"StatusCode\""));
            assertTrue(result.contains("\"Exception\":\"java.lang.Exception\""));
            assertTrue(result.contains("\"ExceptionMessage\":\"Test exception message\""));
        } finally {
            cleanUp();
        }
    }

    @Test
    public void testSuccessfulLogOperationMessage() {
        LogEvent event = setupLogEvent(OperationMessage.class, "Test message", 200);

        try {
            CustomJsonLayout layout = CustomJsonLayout.createLayout(StandardCharsets.UTF_8);

            String result = layout.toSerializable(event);
            assertNotNull(result);
            assertTrue(result.contains("\"message\":\"Test message\""));
            assertTrue(result.contains("\"clientId\":\"1234567890\""));
            assertTrue(result.contains("\"domainName\":\"testDomain\""));
            assertTrue(result.contains("\"StatusCode\":200"));
            assertTrue(result.contains("\"Exception\":\"java.lang.Exception\""));
            assertTrue(result.contains("\"ExceptionMessage\":\"Test exception message\""));
        } finally {
            cleanUp();
        }
    }
}

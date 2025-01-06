/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mockStatic;
import static org.opensearch.sql.expression.function.SerializableUdf.*;

public class SerializableTimeUdfTest {

    private MockedStatic<LocalDateTime> mockedDateTime;

    @Before
    public void setup() {
        final LocalDateTime now = LocalDateTime.parse("2000-01-03T01:01:01.100");
        mockedDateTime = mockStatic(LocalDateTime.class, Mockito.CALLS_REAL_METHODS);
        mockedDateTime.when(LocalDateTime::now).thenReturn(now);
    }

    @After
    public void teardown() {
        mockedDateTime.close();
    }

    @Test
    public void relativeDateTimeTest() {

        /* These are only basic tests of the relative date time functionality.
          For more comprehensive tests, see {@link TimeUtilsTest}.
         */

        testValid("now", "2000-01-03T01:01:01.100");
        testValid("-60m", "2000-01-03T00:01:01.100");
        testValid("-h", "2000-01-03T00:01:01.100");
        testValid("+2wk", "2000-01-17T01:01:01.100");
        testValid("-1h@h", "2000-01-03T00:00");
        testValid("@d", "2000-01-03T00:00");

        testInvalid("invalid", "The relative date time 'invalid' is not supported.");
        testInvalid("INVALID", "The relative date time 'INVALID' is not supported.");
        testInvalid("~h", "The relative date time '~h' is not supported.");
        testInvalid("+1.1h", "The relative date time '+1.1h' is not supported.");
        testInvalid("+ms", "The relative date time unit 'ms' is not supported.");
        testInvalid("+1INVALID", "The relative date time unit 'INVALID' is not supported.");
        testInvalid("@INVALID", "The relative date time unit 'INVALID' is not supported.");
        testInvalid("@ms", "The relative date time unit 'ms' is not supported.");
        testInvalid("@w8", "The relative date time unit 'w8' is not supported.");
    }

    private void testValid(String relativeDateTimeString, String expectedDateTimeString) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        assertEquals(testMessage, expectedDateTimeString, relativeDateTimeFunction.apply(relativeDateTimeString));
    }

    private void testInvalid(String relativeDateTimeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class,
                () -> relativeDateTimeFunction.apply(relativeDateTimeString)).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

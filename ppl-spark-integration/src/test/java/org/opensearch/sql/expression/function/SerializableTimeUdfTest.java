/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.expression.function.SerializableUdf.relativeTimestampFunction;

public class SerializableTimeUdfTest {

    // Monday, Jan 03, 2000 @ 01:01:01.100
    private final Timestamp MOCK_TIMESTAMP = Timestamp.valueOf("2000-01-03 01:01:01.100");

    @Test
    public void relativeTimestampTest() {

        /* These are only basic tests of the relative date time functionality.
          For more comprehensive tests, see {@link TimeUtilsTest}.
         */

        testValid("now", "2000-01-03 01:01:01.100");
        testValid("-60m", "2000-01-03 00:01:01.100");
        testValid("-h", "2000-01-03 00:01:01.100");
        testValid("+2wk", "2000-01-17 01:01:01.100");
        testValid("-1h@h", "2000-01-03 00:00:00");
        testValid("@d", "2000-01-03 00:00:00");

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

    private void testValid(String relativeString, String expectedTimestampString) {
        String testMessage = String.format("\"%s\"", relativeString);
        Timestamp expectedTimestamp = Timestamp.valueOf(expectedTimestampString);
        Timestamp actualTimestamp = relativeTimestampFunction.apply(relativeString, MOCK_TIMESTAMP);
        assertEquals(testMessage, expectedTimestamp, actualTimestamp);
    }

    private void testInvalid(String relativeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class, () -> relativeTimestampFunction.apply(relativeString, MOCK_TIMESTAMP)).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

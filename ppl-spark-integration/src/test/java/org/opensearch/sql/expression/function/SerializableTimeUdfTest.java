/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.expression.function.SerializableUdf.relativeTimestampFunction;

public class SerializableTimeUdfTest {

    // Monday, Jan 03, 2000 @ 01:01:01.100
    private final Instant MOCK_INSTANT = Instant.parse("2000-01-03T01:01:01.100Z");
    private final ZoneId MOCK_ZONE_ID = ZoneId.of("UTC");

    @Test
    public void relativeTimestampTest() {

        /* These are only basic tests of the relative date time functionality.
          For more comprehensive tests, see {@link TimeUtilsTest}.
         */

        testValid("-60m", "2000-01-03 00:01:01.100");
        testValid("-H", "2000-01-03 00:01:01.100");
        testValid("+2wk", "2000-01-17 01:01:01.100");
        testValid("-1h@W3", "1999-12-29 00:00:00");
        testValid("@d", "2000-01-03 00:00:00");
        testValid("now", "2000-01-03 01:01:01.100");

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
        Timestamp actualTimestamp = relativeTimestampFunction.apply(relativeString, MOCK_INSTANT, MOCK_ZONE_ID.toString());
        assertEquals(testMessage, expectedTimestamp, actualTimestamp);
    }

    private void testInvalid(String relativeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class, () -> relativeTimestampFunction.apply(relativeString, MOCK_INSTANT, MOCK_ZONE_ID.toString())).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

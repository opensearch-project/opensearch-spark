/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.expression.function.SerializableUdf.relativeTimestampFunction;

public class SerializableTimeUdfTest {

    // Monday, Jan 03, 2000 @ 01:01:01.100 UTC
    private final LocalDateTime MOCK_LOCAL_DATE_TIME = LocalDateTime.parse("2000-01-03T01:01:01.100");
    private final ZoneOffset MOCK_ZONE_OFFSET = ZoneOffset.UTC;
    private final Instant MOCK_INSTANT = MOCK_LOCAL_DATE_TIME.toInstant(MOCK_ZONE_OFFSET);

    @Test
    public void relativeTimestampTest() {

        /* These are only basic tests of the relative date time functionality.
          For more comprehensive tests, see {@link TimeUtilsTest}.
         */

        testValid("now", "2000-01-03T01:01:01.100Z");
        testValid("-60m", "2000-01-03T00:01:01.100Z");
        testValid("-h", "2000-01-03T00:01:01.100Z");
        testValid("+2wk", "2000-01-17T01:01:01.100Z");
        testValid("-1h@h", "2000-01-03T00:00:00Z");
        testValid("@d", "2000-01-03T00:00:00Z");

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
        String actualTimestampString = relativeTimestampFunction.apply(relativeString, MOCK_INSTANT, MOCK_ZONE_OFFSET.toString()).toString();
        assertEquals(testMessage, expectedTimestampString, actualTimestampString);
    }

    private void testInvalid(String relativeDateTimeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class,
                () -> relativeTimestampFunction.apply(relativeDateTimeString, MOCK_INSTANT, MOCK_ZONE_OFFSET.toString())).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

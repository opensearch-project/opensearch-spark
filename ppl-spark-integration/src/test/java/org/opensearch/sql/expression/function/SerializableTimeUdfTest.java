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

public class SerializableTimeUdfTest {

    // Monday, Jan 03, 2000 @ 01:01:01.100 UTC
    private final ZoneId MOCK_ZONE_ID = ZoneId.of("UTC");
    private final Instant MOCK_INSTANT = Instant.parse("2000-01-03T01:01:01.100Z");
    private final Timestamp MOCK_TIMESTAMP = Timestamp.from(MOCK_INSTANT);

    @Test
    public void relativeTimestampTest() {

        /// These are only basic tests of the relative time functionality.
        /// For more comprehensive tests, see [TimeUtilsTest].

        testValidInstant("-60m", "2000-01-03T00:01:01.100Z");
        testValidInstant("-H", "2000-01-03T00:01:01.100Z");
        testValidInstant("+2wk", "2000-01-17T01:01:01.100Z");
        testValidInstant("-1h@W3", "1999-12-29T00:00:00Z");
        testValidInstant("@d", "2000-01-03T00:00:00Z");
        testValidInstant("now", "2000-01-03T01:01:01.100Z");

        testValidTimestamp("-60m", "2000-01-03T00:01:01.100Z");
        testValidTimestamp("-H", "2000-01-03T00:01:01.100Z");
        testValidTimestamp("+2wk", "2000-01-17T01:01:01.100Z");
        testValidTimestamp("-1h@W3", "1999-12-29T00:00:00Z");
        testValidTimestamp("@d", "2000-01-03T00:00:00Z");
        testValidTimestamp("now", "2000-01-03T01:01:01.100Z");

        testInvalidString("invalid", "The relative date time 'invalid' is not supported.");
        testInvalidString("INVALID", "The relative date time 'INVALID' is not supported.");
        testInvalidString("~h", "The relative date time '~h' is not supported.");
        testInvalidString("+1.1h", "The relative date time '+1.1h' is not supported.");
        testInvalidString("+ms", "The relative date time unit 'ms' is not supported.");
        testInvalidString("+1INVALID", "The relative date time unit 'INVALID' is not supported.");
        testInvalidString("@INVALID", "The relative date time unit 'INVALID' is not supported.");
        testInvalidString("@ms", "The relative date time unit 'ms' is not supported.");
        testInvalidString("@w8", "The relative date time unit 'w8' is not supported.");
    }

    private void testValidInstant(String relativeString, String expectedInstantString) {
        String testMessage = String.format("\"%s\"", relativeString);
        Instant expectedInstant = Instant.parse(expectedInstantString);
        Instant actualTimestamp = SerializableUdf.relativeTimestampFunction.apply(relativeString, MOCK_INSTANT, MOCK_ZONE_ID.toString());
        assertEquals(testMessage, expectedInstant, actualTimestamp);
    }

    private void testValidTimestamp(String relativeString, String expectedInstantString) {
        String testMessage = String.format("\"%s\"", relativeString);
        Instant expectedInstant = Instant.parse(expectedInstantString);
        Instant actualTimestamp = SerializableUdf.relativeTimestampFunction.apply(relativeString, MOCK_TIMESTAMP, MOCK_ZONE_ID.toString());
        assertEquals(testMessage, expectedInstant, actualTimestamp);
    }

    private void testInvalidString(String relativeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class, () -> SerializableUdf.relativeTimestampFunction.apply(relativeString, MOCK_INSTANT, MOCK_ZONE_ID.toString())).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

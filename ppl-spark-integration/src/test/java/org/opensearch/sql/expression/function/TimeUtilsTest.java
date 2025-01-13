/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.ZonedDateTime;

import org.junit.Test;

public class TimeUtilsTest {

    // Monday, Jan 03, 2000 @ 01:01:01.100 UTC
    private final ZonedDateTime MOCK_ZONED_DATE_TIME = ZonedDateTime.parse("2000-01-03T01:01:01.100Z");

    @Test
    public void testRelative() {
        testValid("-60m", "2000-01-03T00:01:01.100Z");
        testValid("-H", "2000-01-03T00:01:01.100Z");
        testValid("+2wk", "2000-01-17T01:01:01.100Z");
        testValid("-1h@W3", "1999-12-29T00:00:00Z");
        testValid("@d", "2000-01-03T00:00Z");
        testValid("now", "2000-01-03T01:01:01.100Z");

        testInvalid("invalid", "The relative date time 'invalid' is not supported.");
    }

    @Test
    public void testRelativeCaseInsensitive() {
        testValid("NOW", "2000-01-03T01:01:01.100Z");
        testValid("-60M", "2000-01-03T00:01:01.100Z");
        testValid("-H", "2000-01-03T00:01:01.100Z");
        testValid("+2WK", "2000-01-17T01:01:01.100Z");
        testValid("-1H@H", "2000-01-03T00:00Z");
        testValid("@D", "2000-01-03T00:00Z");

        testInvalid("INVALID", "The relative date time 'INVALID' is not supported.");
    }

    @Test
    public void testRelativeOffsetSign() {
        testValid("+1h", "2000-01-03T02:01:01.100Z");
        testValid("-h", "2000-01-03T00:01:01.100Z");

        testInvalid("~h", "The relative date time '~h' is not supported.");
    }

    @Test
    public void testRelativeOffsetValue() {
        testValid("+h", "2000-01-03T02:01:01.100Z");
        testValid("+0h", "2000-01-03T01:01:01.100Z");
        testValid("+12h", "2000-01-03T13:01:01.100Z");
        testValid("-3d", "1999-12-31T01:01:01.100Z");

        testInvalid("+1.1h", "The relative date time '+1.1h' is not supported.");
    }

    @Test
    public void testRelativeOffsetUnit() {
        testValid("+s", "2000-01-03T01:01:02.1Z");
        testValid("+sec", "2000-01-03T01:01:02.1Z");
        testValid("+secs", "2000-01-03T01:01:02.1Z");
        testValid("+second", "2000-01-03T01:01:02.1Z");
        testValid("+seconds", "2000-01-03T01:01:02.1Z");

        testValid("+m", "2000-01-03T01:02:01.100Z");
        testValid("+min", "2000-01-03T01:02:01.100Z");
        testValid("+mins", "2000-01-03T01:02:01.100Z");
        testValid("+minute", "2000-01-03T01:02:01.100Z");
        testValid("+minutes", "2000-01-03T01:02:01.100Z");

        testValid("+h", "2000-01-03T02:01:01.100Z");
        testValid("+hr", "2000-01-03T02:01:01.100Z");
        testValid("+hrs", "2000-01-03T02:01:01.100Z");
        testValid("+hour", "2000-01-03T02:01:01.100Z");
        testValid("+hours", "2000-01-03T02:01:01.100Z");

        testValid("+d", "2000-01-04T01:01:01.100Z");
        testValid("+day", "2000-01-04T01:01:01.100Z");
        testValid("+days", "2000-01-04T01:01:01.100Z");

        testValid("+w", "2000-01-10T01:01:01.100Z");
        testValid("+wk", "2000-01-10T01:01:01.100Z");
        testValid("+wks", "2000-01-10T01:01:01.100Z");
        testValid("+week", "2000-01-10T01:01:01.100Z");
        testValid("+weeks", "2000-01-10T01:01:01.100Z");

        testValid("+mon", "2000-02-03T01:01:01.100Z");
        testValid("+month", "2000-02-03T01:01:01.100Z");
        testValid("+months", "2000-02-03T01:01:01.100Z");

        testValid("+q", "2000-04-03T01:01:01.100Z");
        testValid("+qtr", "2000-04-03T01:01:01.100Z");
        testValid("+qtrs", "2000-04-03T01:01:01.100Z");
        testValid("+quarter", "2000-04-03T01:01:01.100Z");
        testValid("+quarters", "2000-04-03T01:01:01.100Z");

        testValid("+y", "2001-01-03T01:01:01.100Z");
        testValid("+yr", "2001-01-03T01:01:01.100Z");
        testValid("+yrs", "2001-01-03T01:01:01.100Z");
        testValid("+year", "2001-01-03T01:01:01.100Z");
        testValid("+years", "2001-01-03T01:01:01.100Z");

        testInvalid("+ms", "The relative date time unit 'ms' is not supported.");
        testInvalid("+1INVALID", "The relative date time unit 'INVALID' is not supported.");
        testInvalid("+now", "The relative date time unit 'now' is not supported.");
    }

    @Test
    public void testRelativeSnap() {
        testValid("@s", "2000-01-03T01:01:01Z");
        testValid("@sec", "2000-01-03T01:01:01Z");
        testValid("@secs", "2000-01-03T01:01:01Z");
        testValid("@second", "2000-01-03T01:01:01Z");
        testValid("@seconds", "2000-01-03T01:01:01Z");

        testValid("@m", "2000-01-03T01:01Z");
        testValid("@min", "2000-01-03T01:01Z");
        testValid("@mins", "2000-01-03T01:01Z");
        testValid("@minute", "2000-01-03T01:01Z");
        testValid("@minutes", "2000-01-03T01:01Z");

        testValid("@h", "2000-01-03T01:00Z");
        testValid("@hr", "2000-01-03T01:00Z");
        testValid("@hrs", "2000-01-03T01:00Z");
        testValid("@hour", "2000-01-03T01:00Z");
        testValid("@hours", "2000-01-03T01:00Z");

        testValid("@d", "2000-01-03T00:00Z");
        testValid("@day", "2000-01-03T00:00Z");
        testValid("@days", "2000-01-03T00:00Z");

        testValid("@w", "2000-01-02T00:00Z");
        testValid("@wk", "2000-01-02T00:00Z");
        testValid("@wks", "2000-01-02T00:00Z");
        testValid("@week", "2000-01-02T00:00Z");
        testValid("@weeks", "2000-01-02T00:00Z");

        testValid("@mon", "2000-01-01T00:00Z");
        testValid("@month", "2000-01-01T00:00Z");
        testValid("@months", "2000-01-01T00:00Z");

        testValid("@q", "2000-01-01T00:00Z");
        testValid("@qtr", "2000-01-01T00:00Z");
        testValid("@qtrs", "2000-01-01T00:00Z");
        testValid("@quarter", "2000-01-01T00:00Z");
        testValid("@quarters", "2000-01-01T00:00Z");

        testValid("@y", "2000-01-01T00:00Z");
        testValid("@yr", "2000-01-01T00:00Z");
        testValid("@yrs", "2000-01-01T00:00Z");
        testValid("@year", "2000-01-01T00:00Z");
        testValid("@years", "2000-01-01T00:00Z");

        testValid("@w0", "2000-01-02T00:00Z");
        testValid("@w1", "2000-01-03T00:00Z");
        testValid("@w2", "1999-12-28T00:00Z");
        testValid("@w3", "1999-12-29T00:00Z");
        testValid("@w4", "1999-12-30T00:00Z");
        testValid("@w5", "1999-12-31T00:00Z");
        testValid("@w6", "2000-01-01T00:00Z");
        testValid("@w7", "2000-01-02T00:00Z");

        testInvalid("@INVALID", "The relative date time unit 'INVALID' is not supported.");
        testInvalid("@ms", "The relative date time unit 'ms' is not supported.");
        testInvalid("@w8", "The relative date time unit 'w8' is not supported.");
        testInvalid("@now", "The relative date time unit 'now' is not supported.");
    }

    private void testValid(String relativeDateTimeString, String expectedDateTimeString) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        ZonedDateTime expectedDateTime = ZonedDateTime.parse(expectedDateTimeString);
        ZonedDateTime actualDateTime = TimeUtils.getRelativeZonedDateTime(relativeDateTimeString, MOCK_ZONED_DATE_TIME);
        assertEquals(testMessage, expectedDateTime, actualDateTime);
    }

    private void testInvalid(String relativeDateTimeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class,
                () -> TimeUtils.getRelativeZonedDateTime(relativeDateTimeString, MOCK_ZONED_DATE_TIME)).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

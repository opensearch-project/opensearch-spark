/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.LocalDateTime;

import org.junit.Test;

public class TimeUtilsTest {

    // Monday, Jan 03, 2000 @ 01:01:01.100
    private final LocalDateTime dateTime = LocalDateTime.parse("2000-01-03T01:01:01.100");

    @Test
    public void testRelative() {
        testValid("now", "2000-01-03T01:01:01.100");
        testValid("-60m", "2000-01-03T00:01:01.100");
        testValid("-h", "2000-01-03T00:01:01.100");
        testValid("+2wk", "2000-01-17T01:01:01.100");
        testValid("-1h@h", "2000-01-03T00:00");
        testValid("@d", "2000-01-03T00:00");

        testInvalid("INVALID", "The relative date time 'INVALID' is not supported.");
    }

    @Test
    public void testRelativeOffsetSign() {
        testValid("+h", "2000-01-03T02:01:01.100");
        testValid("-h", "2000-01-03T00:01:01.100");

        testInvalid("~h", "The relative date time '~h' is not supported.");
    }

    @Test
    public void testRelativeOffsetValue() {
        testValid("+h", "2000-01-03T02:01:01.100");
        testValid("+0h", "2000-01-03T01:01:01.100");
        testValid("+1h", "2000-01-03T02:01:01.100");
        testValid("+12h", "2000-01-03T13:01:01.100");

        testInvalid("+1.1h", "The relative date time '+1.1h' is not supported.");
    }

    @Test
    public void testRelativeOffsetUnit() {
        testValid("+s", "2000-01-03T01:01:02.1");
        testValid("+sec", "2000-01-03T01:01:02.1");
        testValid("+secs", "2000-01-03T01:01:02.1");
        testValid("+second", "2000-01-03T01:01:02.1");
        testValid("+seconds", "2000-01-03T01:01:02.1");

        testValid("+m", "2000-01-03T01:02:01.100");
        testValid("+min", "2000-01-03T01:02:01.100");
        testValid("+mins", "2000-01-03T01:02:01.100");
        testValid("+minute", "2000-01-03T01:02:01.100");
        testValid("+minutes", "2000-01-03T01:02:01.100");

        testValid("+h", "2000-01-03T02:01:01.100");
        testValid("+hr", "2000-01-03T02:01:01.100");
        testValid("+hrs", "2000-01-03T02:01:01.100");
        testValid("+hour", "2000-01-03T02:01:01.100");
        testValid("+hours", "2000-01-03T02:01:01.100");

        testValid("+d", "2000-01-04T01:01:01.100");
        testValid("+day", "2000-01-04T01:01:01.100");
        testValid("+days", "2000-01-04T01:01:01.100");

        testValid("+w", "2000-01-10T01:01:01.100");
        testValid("+wk", "2000-01-10T01:01:01.100");
        testValid("+wks", "2000-01-10T01:01:01.100");
        testValid("+week", "2000-01-10T01:01:01.100");
        testValid("+weeks", "2000-01-10T01:01:01.100");

        testValid("+mon", "2000-02-03T01:01:01.100");
        testValid("+month", "2000-02-03T01:01:01.100");
        testValid("+months", "2000-02-03T01:01:01.100");

        testValid("+q", "2000-04-03T01:01:01.100");
        testValid("+qtr", "2000-04-03T01:01:01.100");
        testValid("+qtrs", "2000-04-03T01:01:01.100");
        testValid("+quarter", "2000-04-03T01:01:01.100");
        testValid("+quarters", "2000-04-03T01:01:01.100");

        testValid("+y", "2001-01-03T01:01:01.100");
        testValid("+yr", "2001-01-03T01:01:01.100");
        testValid("+yrs", "2001-01-03T01:01:01.100");
        testValid("+year", "2001-01-03T01:01:01.100");
        testValid("+years", "2001-01-03T01:01:01.100");

        testInvalid("+1INVALID", "The relative date time unit 'INVALID' is not supported.");
    }

    @Test
    public void testRelativeSnap() {
        testValid("@s", "2000-01-03T01:01:01");
        testValid("@sec", "2000-01-03T01:01:01");
        testValid("@secs", "2000-01-03T01:01:01");
        testValid("@second", "2000-01-03T01:01:01");
        testValid("@seconds", "2000-01-03T01:01:01");

        testValid("@m", "2000-01-03T01:01");
        testValid("@min", "2000-01-03T01:01");
        testValid("@mins", "2000-01-03T01:01");
        testValid("@minute", "2000-01-03T01:01");
        testValid("@minutes", "2000-01-03T01:01");

        testValid("@h", "2000-01-03T01:00");
        testValid("@hr", "2000-01-03T01:00");
        testValid("@hrs", "2000-01-03T01:00");
        testValid("@hour", "2000-01-03T01:00");
        testValid("@hours", "2000-01-03T01:00");

        testValid("@d", "2000-01-03T00:00");
        testValid("@day", "2000-01-03T00:00");
        testValid("@days", "2000-01-03T00:00");

        testValid("@w", "2000-01-02T00:00");
        testValid("@wk", "2000-01-02T00:00");
        testValid("@wks", "2000-01-02T00:00");
        testValid("@week", "2000-01-02T00:00");
        testValid("@weeks", "2000-01-02T00:00");

        testValid("@mon", "2000-01-01T00:00");
        testValid("@month", "2000-01-01T00:00");
        testValid("@months", "2000-01-01T00:00");

        testValid("@q", "2000-01-01T00:00");
        testValid("@qtr", "2000-01-01T00:00");
        testValid("@qtrs", "2000-01-01T00:00");
        testValid("@quarter", "2000-01-01T00:00");
        testValid("@quarters", "2000-01-01T00:00");

        testValid("@y", "2000-01-01T00:00");
        testValid("@yr", "2000-01-01T00:00");
        testValid("@yrs", "2000-01-01T00:00");
        testValid("@year", "2000-01-01T00:00");
        testValid("@years", "2000-01-01T00:00");

        testValid("@w0", "2000-01-02T00:00");
        testValid("@w1", "2000-01-03T00:00");
        testValid("@w2", "1999-12-28T00:00");
        testValid("@w3", "1999-12-29T00:00");
        testValid("@w4", "1999-12-30T00:00");
        testValid("@w5", "1999-12-31T00:00");
        testValid("@w6", "2000-01-01T00:00");
        testValid("@w7", "2000-01-02T00:00");

        testInvalid("@INVALID", "The relative date time unit 'INVALID' is not supported.");
    }

    private void testValid(String relativeDateTimeString, String expectedDateTimeString) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        LocalDateTime expectedDateTime = LocalDateTime.parse(expectedDateTimeString);
        LocalDateTime actualDateTime = TimeUtils.getRelativeDateTime(relativeDateTimeString, dateTime);
        assertEquals(testMessage, expectedDateTime, actualDateTime);
    }

    private void testInvalid(String relativeDateTimeString, String expectedExceptionMessage) {
        String testMessage = String.format("\"%s\"", relativeDateTimeString);
        String actualExceptionMessage = assertThrows(testMessage, RuntimeException.class,
                () -> TimeUtils.getRelativeDateTime(relativeDateTimeString, dateTime)).getMessage();
        assertEquals(expectedExceptionMessage, actualExceptionMessage);
    }
}

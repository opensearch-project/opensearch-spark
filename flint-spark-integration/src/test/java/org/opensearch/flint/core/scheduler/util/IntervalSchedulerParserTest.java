/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler.util;

import org.junit.Test;
import org.opensearch.flint.spark.scheduler.util.IntervalSchedulerParser;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IntervalSchedulerParserTest {

    @Test(expected = IllegalArgumentException.class)
    public void testParseNull() {
        IntervalSchedulerParser.parse(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseMillisNull() {
        IntervalSchedulerParser.parseAndConvertToMillis(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseEmptyString() {
        IntervalSchedulerParser.parse("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseMillisEmptyString() {
        IntervalSchedulerParser.parseAndConvertToMillis("");
    }

    @Test
    public void testParseString() {
        Schedule schedule = IntervalSchedulerParser.parse("10 minutes");
        assertTrue(schedule instanceof IntervalSchedule);
        IntervalSchedule intervalSchedule = (IntervalSchedule) schedule;
        assertEquals(10, intervalSchedule.getInterval());
        assertEquals(ChronoUnit.MINUTES, intervalSchedule.getUnit());
    }

    @Test
    public void testParseMillisString() {
        Long millis = IntervalSchedulerParser.parseAndConvertToMillis("10 minutes");
        assertEquals(600000, millis.longValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidFormat() {
        IntervalSchedulerParser.parse("invalid format");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseMillisInvalidFormat() {
        IntervalSchedulerParser.parseAndConvertToMillis("invalid format");
    }

    @Test
    public void testParseStringScheduleMinutes() {
        IntervalSchedule schedule = IntervalSchedulerParser.parse("5 minutes");
        assertEquals(5, schedule.getInterval());
        assertEquals(ChronoUnit.MINUTES, schedule.getUnit());
    }

    @Test
    public void testParseMillisStringScheduleMinutes() {
        Long millis = IntervalSchedulerParser.parseAndConvertToMillis("5 minutes");
        assertEquals(300000, millis.longValue());
    }

    @Test
    public void testParseStringScheduleHours() {
        IntervalSchedule schedule = IntervalSchedulerParser.parse("2 hours");
        assertEquals(120, schedule.getInterval());
        assertEquals(ChronoUnit.MINUTES, schedule.getUnit());
    }

    @Test
    public void testParseMillisStringScheduleHours() {
        Long millis = IntervalSchedulerParser.parseAndConvertToMillis("2 hours");
        assertEquals(7200000, millis.longValue());
    }

    @Test
    public void testParseStringScheduleDays() {
        IntervalSchedule schedule = IntervalSchedulerParser.parse("1 day");
        assertEquals(1440, schedule.getInterval());
        assertEquals(ChronoUnit.MINUTES, schedule.getUnit());
    }

    @Test
    public void testParseMillisStringScheduleDays() {
        Long millis = IntervalSchedulerParser.parseAndConvertToMillis("1 day");
        assertEquals(86400000, millis.longValue());
    }

    @Test
    public void testParseStringScheduleStartTime() {
        Instant before = Instant.now();
        IntervalSchedule schedule = IntervalSchedulerParser.parse("30 minutes");
        Instant after = Instant.now();
        
        assertTrue(schedule.getStartTime().isAfter(before) || schedule.getStartTime().equals(before));
        assertTrue(schedule.getStartTime().isBefore(after) || schedule.getStartTime().equals(after));
    }
}
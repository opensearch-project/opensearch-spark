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
    public void testParseEmptyString() {
        IntervalSchedulerParser.parse("");
    }

    @Test
    public void testParseString() {
        Schedule result = IntervalSchedulerParser.parse("10 minutes");
        assertTrue(result instanceof IntervalSchedule);
        IntervalSchedule intervalSchedule = (IntervalSchedule) result;
        assertEquals(10, intervalSchedule.getInterval());
        assertEquals(ChronoUnit.MINUTES, intervalSchedule.getUnit());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidFormat() {
        IntervalSchedulerParser.parse("invalid format");
    }

    @Test
    public void testParseStringScheduleMinutes() {
        IntervalSchedule result = IntervalSchedulerParser.parse("5 minutes");
        assertEquals(5, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    @Test
    public void testParseStringScheduleHours() {
        IntervalSchedule result = IntervalSchedulerParser.parse("2 hours");
        assertEquals(120, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    @Test
    public void testParseStringScheduleDays() {
        IntervalSchedule result = IntervalSchedulerParser.parse("1 day");
        assertEquals(1440, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    @Test
    public void testParseStringScheduleStartTime() {
        Instant before = Instant.now();
        IntervalSchedule result = IntervalSchedulerParser.parse("30 minutes");
        Instant after = Instant.now();
        
        assertTrue(result.getStartTime().isAfter(before) || result.getStartTime().equals(before));
        assertTrue(result.getStartTime().isBefore(after) || result.getStartTime().equals(after));
    }
}
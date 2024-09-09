/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler.util;

import org.junit.Test;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class IntervalSchedulerParserTest {

    @Test
    public void testParseNull() {
        assertNull(IntervalSchedulerParser.parse(null));
    }

    @Test
    public void testParseScheduleInstance() {
        Schedule schedule = new IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES);
        assertEquals(schedule, IntervalSchedulerParser.parse(schedule));
    }

    @Test
    public void testParseScalaOptionWithString() {
        scala.Option<String> option = scala.Option.apply("5 minutes");
        Schedule result = IntervalSchedulerParser.parse(option);
        assertTrue(result instanceof IntervalSchedule);
    }

    @Test
    public void testParseScalaOptionEmpty() {
        scala.Option<String> option = scala.Option.empty();
        assertNull(IntervalSchedulerParser.parse(option));
    }

    @Test
    public void testParseScalaOptionWithNonString() {
        scala.Option<Integer> option = scala.Option.apply(5);
        assertNull(IntervalSchedulerParser.parse(option));
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
    public void testParseInvalidObject() {
        IntervalSchedulerParser.parse(new Object());
    }

    @Test
    public void testParseStringScheduleMinutes() {
        IntervalSchedule result = IntervalSchedulerParser.parseStringSchedule("5 minutes");
        assertEquals(5, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    @Test
    public void testParseStringScheduleHours() {
        IntervalSchedule result = IntervalSchedulerParser.parseStringSchedule("2 hours");
        assertEquals(120, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    @Test
    public void testParseStringScheduleDays() {
        IntervalSchedule result = IntervalSchedulerParser.parseStringSchedule("1 day");
        assertEquals(1440, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseStringScheduleInvalidFormat() {
        IntervalSchedulerParser.parseStringSchedule("invalid format");
    }

    @Test
    public void testParseStringScheduleStartTime() {
        Instant before = Instant.now();
        IntervalSchedule result = IntervalSchedulerParser.parseStringSchedule("30 minutes");
        Instant after = Instant.now();
        
        assertTrue(result.getStartTime().isAfter(before) || result.getStartTime().equals(before));
        assertTrue(result.getStartTime().isBefore(after) || result.getStartTime().equals(after));
    }
}
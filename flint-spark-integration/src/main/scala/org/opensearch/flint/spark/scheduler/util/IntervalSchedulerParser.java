/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler.util;

import org.apache.parquet.Strings;
import org.apache.spark.sql.execution.streaming.Triggers;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Utility class for parsing interval schedules.
 */
public class IntervalSchedulerParser {

    /**
     * Parses a schedule string into an integer in milliseconds.
     *
     * @param scheduleStr the schedule string to parse
     * @return the parsed integer
     * @throws IllegalArgumentException if the schedule string is invalid
     */
    public static Long parseAndConvertToMillis(String scheduleStr) {
        if (Strings.isNullOrEmpty(scheduleStr)) {
            throw new IllegalArgumentException("Schedule string must not be null or empty.");
        }

        return Triggers.convert(scheduleStr);
    }

    /**
     * Parses a schedule string into an IntervalSchedule.
     *
     * @param scheduleStr the schedule string to parse
     * @return the parsed IntervalSchedule
     * @throws IllegalArgumentException if the schedule string is invalid
     */
    public static IntervalSchedule parse(String scheduleStr) {
        // Convert milliseconds to minutes (rounding down)
        int minutes = (int) (parseAndConvertToMillis(scheduleStr) / (60 * 1000));

        // Use the current time as the start time
        Instant startTime = Instant.now();

        return new IntervalSchedule(startTime, minutes, ChronoUnit.MINUTES);
    }
}
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.scheduler.util;

import org.apache.spark.sql.execution.streaming.Triggers;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.opensearch.flint.core.logging.CustomLogging.logInfo;

public class IntervalSchedulerParser {

  public static Schedule parse(Object schedule) {
    if (schedule == null) {
      return null;
    }

    if (schedule instanceof Schedule) {
      return (Schedule) schedule;
    }

    logInfo(schedule.getClass().getSimpleName());

    if (schedule instanceof scala.Option) {
      scala.Option<?> option = (scala.Option<?>) schedule;
      if (option.isDefined()) {
        Object value = option.get();
        if (value instanceof String) {
          return parseStringSchedule((String) value);
        }
      }
      return null;
    }

    if (schedule instanceof String) {
      return parseStringSchedule((String) schedule);
    }

    throw new IllegalArgumentException("Schedule must be a String, Option[String], or Schedule object for parsing.");
  }

  public static IntervalSchedule parseStringSchedule(String scheduleStr) {
    Long millis = Triggers.convert(scheduleStr);

    // Convert milliseconds to minutes (rounding down)
    int minutes = (int) (millis / (60 * 1000));

    // Use the current time as the start time
    Instant startTime = Instant.now();

    return new IntervalSchedule(startTime, minutes, ChronoUnit.MINUTES);
  }
}
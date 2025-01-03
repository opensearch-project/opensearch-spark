/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import lombok.experimental.UtilityClass;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
public class TimeUtils {

    private static final String now = "now";
    private static final String negativeSign = "-";

    // Pattern for relative date time string
    private static final String patternStringOffset = "(?<offsetSign>[+-])(?<offsetValue>\\d+)?(?<offsetUnit>\\w+)";
    private static final String patternStringSnap = "[@](?<snapUnit>\\w+)";
    private static final String patternStringRelative = String.format("(?<offset>%s)?(?<snap>%s)?", patternStringOffset, patternStringSnap);

    private static final Pattern pattern = Pattern.compile(patternStringRelative);

    // Time unit constants
    private static final Set<String> secondUnits = Set.of("s", "sec", "secs", "second", "seconds");
    private static final Set<String> minuteUnits = Set.of("m", "min", "mins", "minute", "minutes");
    private static final Set<String> hourUnits = Set.of("h", "hr", "hrs", "hour", "hours");
    private static final Set<String> dayUnits = Set.of("d", "day", "days");
    private static final Set<String> weekUnits = Set.of("w", "wk", "wks", "week", "weeks");
    private static final Set<String> monthUnits = Set.of("mon", "month", "months");
    private static final Set<String> quarterUnits = Set.of("q", "qtr", "qtrs", "quarter", "quarters");
    private static final Set<String> yearUnits = Set.of("y", "yr", "yrs", "year", "years");

    // Map from time unit constants to the corresponding duration.
    private static final Duration secondDuration = Duration.ofSeconds(1);
    private static final Duration minuteDuration = Duration.ofMinutes(1);
    private static final Duration hourDuration = Duration.ofHours(1);

    private static final Map<String, Duration> durationForTimeUnit = Map.ofEntries(
            Map.entry("s", secondDuration),
            Map.entry("sec", secondDuration),
            Map.entry("secs", secondDuration),
            Map.entry("second", secondDuration),
            Map.entry("seconds", secondDuration),

            Map.entry("m", minuteDuration),
            Map.entry("min", minuteDuration),
            Map.entry("mins", minuteDuration),
            Map.entry("minute", minuteDuration),
            Map.entry("minutes", minuteDuration),

            Map.entry("h", hourDuration),
            Map.entry("hr", hourDuration),
            Map.entry("hrs", hourDuration),
            Map.entry("hour", hourDuration),
            Map.entry("hours", hourDuration));

    // Map from time unit constants to the corresponding period.
    private static final Period periodDay = Period.ofDays(1);
    private static final Period periodWeek = Period.ofWeeks(1);
    private static final Period periodMonth = Period.ofMonths(1);
    private static final Period periodQuarter = Period.ofMonths(3);
    private static final Period periodYear = Period.ofYears(1);

    private static final Map<String, Period> periodForTimeUnit = Map.ofEntries(
            Map.entry("d", periodDay),
            Map.entry("day", periodDay),
            Map.entry("days", periodDay),

            Map.entry("w", periodWeek),
            Map.entry("wk", periodWeek),
            Map.entry("wks", periodWeek),
            Map.entry("week", periodWeek),
            Map.entry("weeks", periodWeek),

            Map.entry("mon", periodMonth),
            Map.entry("month", periodMonth),
            Map.entry("months", periodMonth),

            Map.entry("q", periodQuarter),
            Map.entry("qtr", periodQuarter),
            Map.entry("qtrs", periodQuarter),
            Map.entry("quarter", periodQuarter),
            Map.entry("quarters", periodQuarter),

            Map.entry("y", periodYear),
            Map.entry("yr", periodYear),
            Map.entry("yrs", periodYear),
            Map.entry("year", periodYear),
            Map.entry("years", periodYear));

    // Maps from day of the week unit constants to the corresponding day of the week.
    private static final Map<String, DayOfWeek> daysOfWeekForUnit = Map.ofEntries(
            Map.entry("w0", DayOfWeek.SUNDAY),
            Map.entry("w7", DayOfWeek.SUNDAY),
            Map.entry("w1", DayOfWeek.MONDAY),
            Map.entry("w2", DayOfWeek.TUESDAY),
            Map.entry("w3", DayOfWeek.WEDNESDAY),
            Map.entry("w4", DayOfWeek.THURSDAY),
            Map.entry("w5", DayOfWeek.FRIDAY),
            Map.entry("w6", DayOfWeek.SATURDAY));

    static final int DAYS_PER_WEEK = 7;
    static final int MONTHS_PER_QUARTER = 3;

    /**
     * Returns the {@link LocalDateTime} corresponding to the given relative date time string and date time.
     * Throws {@link RuntimeException} if the relative date time string is not supported.
     */
    public static LocalDateTime getRelativeDateTime(String relativeDateTimeString, LocalDateTime dateTime) {

        if (relativeDateTimeString.equals(now)) {
            return dateTime;
        }

        Matcher matcher = pattern.matcher(relativeDateTimeString);
        if (!matcher.matches()) {
            String message = String.format("The relative date time '%s' is not supported.", relativeDateTimeString);
            throw new RuntimeException(message);
        }

        LocalDateTime relativeDateTime = dateTime;

        if (matcher.group("offset") != null) {
            relativeDateTime = applyOffset(
                    relativeDateTime,
                    matcher.group("offsetSign"),
                    matcher.group("offsetValue"),
                    matcher.group("offsetUnit")
            );
        }

        if (matcher.group("snap") != null) {
            relativeDateTime = applySnap(
                    relativeDateTime,
                    matcher.group("snapUnit")
            );
        }

        return relativeDateTime;
    }

    /**
     * Applies the offset specified by the offset sign, value, and unit to the given date time, and returns the result.
     */
    private LocalDateTime applyOffset(LocalDateTime dateTime, String offsetSignString, String offsetValueString, String offsetUnitString) {
        int offsetValue = Optional.ofNullable(offsetValueString).map(Integer::parseInt).orElse(1);
        if (offsetSignString.equals(negativeSign)) {
            offsetValue *= -1;
        }

        /* {@link Duration} and {@link Period} must be handled separately because, even
          though they both inherit from {@link java.time.temporal.TemporalAmount}, they
           define separate 'multipliedBy' methods. */

        if (durationForTimeUnit.containsKey(offsetUnitString)) {
            final Duration offsetDuration = durationForTimeUnit.get(offsetUnitString).multipliedBy(offsetValue);
            return dateTime.plus(offsetDuration);
        }

        if (periodForTimeUnit.containsKey(offsetUnitString)) {
            final Period offsetPeriod = periodForTimeUnit.get(offsetUnitString).multipliedBy(offsetValue);
            return dateTime.plus(offsetPeriod);
        }

        final String message = String.format("The relative date time unit '%s' is not supported.", offsetUnitString);
        throw new RuntimeException(message);
    }

    /**
     * Snaps the given date time to the start of the previous time period specified by the given snap unit, and returns the result.
     */
    private LocalDateTime applySnap(LocalDateTime dateTime, String snapUnit) {

        if (secondUnits.contains(snapUnit)) {
            return dateTime.truncatedTo(ChronoUnit.SECONDS);
        } else if (minuteUnits.contains(snapUnit)) {
            return dateTime.truncatedTo(ChronoUnit.MINUTES);
        } else if (hourUnits.contains(snapUnit)) {
            return dateTime.truncatedTo(ChronoUnit.HOURS);
        } else if (dayUnits.contains(snapUnit)) {
            return dateTime.truncatedTo(ChronoUnit.DAYS);
        } else if (weekUnits.contains(snapUnit)) {
            return applySnapToDay(dateTime, DayOfWeek.SUNDAY);
        } else if (monthUnits.contains(snapUnit)) {
            return dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
        } else if (quarterUnits.contains(snapUnit)) {
            int monthsToSnap = (dateTime.getMonthValue() - 1) % MONTHS_PER_QUARTER;
            return dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).minusMonths(monthsToSnap);
        } else if (yearUnits.contains(snapUnit)) {
            return dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
        } else if (daysOfWeekForUnit.containsKey(snapUnit)) {
            return applySnapToDay(dateTime, daysOfWeekForUnit.get(snapUnit));
        }

        final String message = String.format("The relative date time unit '%s' is not supported.", snapUnit);
        throw new RuntimeException(message);
    }

    /**
     * Snaps the given date time to the start of the previous specified day of the week, and returns the result.
     */
    private LocalDateTime applySnapToDay(LocalDateTime dateTime, DayOfWeek snapDay) {
        LocalDateTime snapped = dateTime.truncatedTo(ChronoUnit.DAYS);

        DayOfWeek day = dateTime.getDayOfWeek();
        if (day.equals(snapDay)) {
            return snapped;
        }

        int daysToSnap = DAYS_PER_WEEK - snapDay.getValue() + day.getValue();
        return snapped.minusDays(daysToSnap);
    }
}

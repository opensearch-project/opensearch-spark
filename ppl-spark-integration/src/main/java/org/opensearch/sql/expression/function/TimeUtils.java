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

    private static final String NOW = "now";
    private static final String NEGATIVE_SIGN = "-";

    // Pattern for relative date time string.
    private static final Pattern RELATIVE_DATE_TIME_PATTERN = Pattern.compile(String.format(
            "(?<offset>%s)?(?<snap>%s)?",
            "(?<offsetSign>[+-])(?<offsetValue>\\d+)?(?<offsetUnit>\\w+)",
            "[@](?<snapUnit>\\w+)"),
            Pattern.CASE_INSENSITIVE);

    // Supported time units.
    private static final Set<String> SECOND_UNITS_SET = Set.of("s", "sec", "secs", "second", "seconds");
    private static final Set<String> MINUTE_UNITS_SET = Set.of("m", "min", "mins", "minute", "minutes");
    private static final Set<String> HOUR_UNITS_SET = Set.of("h", "hr", "hrs", "hour", "hours");
    private static final Set<String> DAY_UNITS_SET = Set.of("d", "day", "days");
    private static final Set<String> WEEK_UNITS_SET = Set.of("w", "wk", "wks", "week", "weeks");
    private static final Set<String> MONTH_UNITS_SET = Set.of("mon", "month", "months");
    private static final Set<String> QUARTER_UNITS_SET = Set.of("q", "qtr", "qtrs", "quarter", "quarters");
    private static final Set<String> YEAR_UNITS_SET = Set.of("y", "yr", "yrs", "year", "years");

    // Map from time unit to the corresponding duration.
    private static final Duration DURATION_SECOND = Duration.ofSeconds(1);
    private static final Duration DURATION_MINUTE = Duration.ofMinutes(1);
    private static final Duration DURATION_HOUR = Duration.ofHours(1);

    private static final Map<String, Duration> DURATION_FOR_TIME_UNIT_MAP = Map.ofEntries(
            Map.entry("s", DURATION_SECOND),
            Map.entry("sec", DURATION_SECOND),
            Map.entry("secs", DURATION_SECOND),
            Map.entry("second", DURATION_SECOND),
            Map.entry("seconds", DURATION_SECOND),

            Map.entry("m", DURATION_MINUTE),
            Map.entry("min", DURATION_MINUTE),
            Map.entry("mins", DURATION_MINUTE),
            Map.entry("minute", DURATION_MINUTE),
            Map.entry("minutes", DURATION_MINUTE),

            Map.entry("h", DURATION_HOUR),
            Map.entry("hr", DURATION_HOUR),
            Map.entry("hrs", DURATION_HOUR),
            Map.entry("hour", DURATION_HOUR),
            Map.entry("hours", DURATION_HOUR));

    // Map from time unit to the corresponding period.
    private static final Period PERIOD_DAY = Period.ofDays(1);
    private static final Period PERIOD_WEEK = Period.ofWeeks(1);
    private static final Period PERIOD_MONTH = Period.ofMonths(1);
    private static final Period PERIOD_QUARTER = Period.ofMonths(3);
    private static final Period PERIOD_YEAR = Period.ofYears(1);

    private static final Map<String, Period> PERIOD_FOR_TIME_UNIT_MAP = Map.ofEntries(
            Map.entry("d", PERIOD_DAY),
            Map.entry("day", PERIOD_DAY),
            Map.entry("days", PERIOD_DAY),

            Map.entry("w", PERIOD_WEEK),
            Map.entry("wk", PERIOD_WEEK),
            Map.entry("wks", PERIOD_WEEK),
            Map.entry("week", PERIOD_WEEK),
            Map.entry("weeks", PERIOD_WEEK),

            Map.entry("mon", PERIOD_MONTH),
            Map.entry("month", PERIOD_MONTH),
            Map.entry("months", PERIOD_MONTH),

            Map.entry("q", PERIOD_QUARTER),
            Map.entry("qtr", PERIOD_QUARTER),
            Map.entry("qtrs", PERIOD_QUARTER),
            Map.entry("quarter", PERIOD_QUARTER),
            Map.entry("quarters", PERIOD_QUARTER),

            Map.entry("y", PERIOD_YEAR),
            Map.entry("yr", PERIOD_YEAR),
            Map.entry("yrs", PERIOD_YEAR),
            Map.entry("year", PERIOD_YEAR),
            Map.entry("years", PERIOD_YEAR));

    // Map from snap unit to the corresponding day of the week.
    private static final Map<String, DayOfWeek> DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP = Map.ofEntries(
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

        if (relativeDateTimeString.equalsIgnoreCase(NOW)) {
            return dateTime;
        }

        Matcher matcher = RELATIVE_DATE_TIME_PATTERN.matcher(relativeDateTimeString);
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
                    matcher.group("offsetUnit"));
        }

        if (matcher.group("snap") != null) {
            relativeDateTime = applySnap(
                    relativeDateTime,
                    matcher.group("snapUnit"));
        }

        return relativeDateTime;
    }

    /**
     * Applies the offset specified by the offset sign, value,
     * and unit to the given date time, and returns the result.
     */
    private LocalDateTime applyOffset(LocalDateTime dateTime, String offsetSign, String offsetValue, String offsetUnit) {

        int offsetValueInt = Optional.ofNullable(offsetValue).map(Integer::parseInt).orElse(1);
        if (offsetSign.equals(NEGATIVE_SIGN)) {
            offsetValueInt *= -1;
        }

        /* {@link Duration} and {@link Period} must be handled separately because, even
          though they both inherit from {@link java.time.temporal.TemporalAmount}, they
           define separate 'multipliedBy' methods. */

        // Convert to lower case to make case-insensitive.
        String offsetUnitLowerCase = offsetUnit.toLowerCase();

        if (DURATION_FOR_TIME_UNIT_MAP.containsKey(offsetUnitLowerCase)) {
            Duration offsetDuration = DURATION_FOR_TIME_UNIT_MAP.get(offsetUnitLowerCase).multipliedBy(offsetValueInt);
            return dateTime.plus(offsetDuration);
        }

        if (PERIOD_FOR_TIME_UNIT_MAP.containsKey(offsetUnitLowerCase)) {
            Period offsetPeriod = PERIOD_FOR_TIME_UNIT_MAP.get(offsetUnitLowerCase).multipliedBy(offsetValueInt);
            return dateTime.plus(offsetPeriod);
        }

        String message = String.format("The relative date time unit '%s' is not supported.", offsetUnit);
        throw new RuntimeException(message);
    }

    /**
     * Snaps the given date time to the start of the previous time
     * period specified by the given snap unit, and returns the result.
     */
    private LocalDateTime applySnap(LocalDateTime dateTime, String snapUnit) {

        // Convert to lower case to make case-insensitive.
        String snapUnitLowerCase = snapUnit.toLowerCase();

        if (SECOND_UNITS_SET.contains(snapUnitLowerCase)) {
            return dateTime.truncatedTo(ChronoUnit.SECONDS);
        } else if (MINUTE_UNITS_SET.contains(snapUnitLowerCase)) {
            return dateTime.truncatedTo(ChronoUnit.MINUTES);
        } else if (HOUR_UNITS_SET.contains(snapUnitLowerCase)) {
            return dateTime.truncatedTo(ChronoUnit.HOURS);
        } else if (DAY_UNITS_SET.contains(snapUnitLowerCase)) {
            return dateTime.truncatedTo(ChronoUnit.DAYS);
        } else if (WEEK_UNITS_SET.contains(snapUnitLowerCase)) {
            return applySnapToDayOfWeek(dateTime, DayOfWeek.SUNDAY);
        } else if (MONTH_UNITS_SET.contains(snapUnitLowerCase)) {
            return dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
        } else if (QUARTER_UNITS_SET.contains(snapUnitLowerCase)) {
            int monthsToSnap = (dateTime.getMonthValue() - 1) % MONTHS_PER_QUARTER;
            return dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).minusMonths(monthsToSnap);
        } else if (YEAR_UNITS_SET.contains(snapUnitLowerCase)) {
            return dateTime.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
        } else if (DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP.containsKey(snapUnitLowerCase)) {
            return applySnapToDayOfWeek(dateTime, DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP.get(snapUnit));
        }

        String message = String.format("The relative date time unit '%s' is not supported.", snapUnit);
        throw new RuntimeException(message);
    }

    /**
     * Snaps the given date time to the start of the previous
     * specified day of the week, and returns the result.
     */
    private LocalDateTime applySnapToDayOfWeek(LocalDateTime dateTime, DayOfWeek snapDayOfWeek) {
        LocalDateTime snappedDateTime = dateTime.truncatedTo(ChronoUnit.DAYS);

        DayOfWeek dayOfWeek = dateTime.getDayOfWeek();
        if (dayOfWeek.equals(snapDayOfWeek)) {
            return snappedDateTime;
        }

        int daysToSnap = DAYS_PER_WEEK - snapDayOfWeek.getValue() + dayOfWeek.getValue();
        return snappedDateTime.minusDays(daysToSnap);
    }
}

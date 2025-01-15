/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.ImmutableMap;
import lombok.experimental.UtilityClass;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Month;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
public class TimeUtils {

    private static final String NOW = "now";
    private static final String NEGATIVE_SIGN = "-";

    // Pattern for relative string.
    private static final String OFFSET_PATTERN_STRING = "(?<offsetSign>[+-])(?<offsetValue>\\d+)?(?<offsetUnit>\\w+)";
    private static final String SNAP_PATTERN_STRING = "[@](?<snapUnit>\\w+)";

    private static final Pattern RELATIVE_PATTERN = Pattern.compile(String.format(
                    "(?<offset>%s)?(?<snap>%s)?", OFFSET_PATTERN_STRING, SNAP_PATTERN_STRING),
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
    private static final Map<String, Duration> DURATION_FOR_TIME_UNIT_MAP;

    static {
        Map<String, Duration> durationMap = new HashMap<>();
        SECOND_UNITS_SET.forEach(u -> durationMap.put(u, Duration.ofSeconds(1)));
        MINUTE_UNITS_SET.forEach(u -> durationMap.put(u, Duration.ofMinutes(1)));
        HOUR_UNITS_SET.forEach(u -> durationMap.put(u, Duration.ofHours(1)));
        DURATION_FOR_TIME_UNIT_MAP = ImmutableMap.copyOf(durationMap);
    }

    // Map from time unit to the corresponding period.
    private static final Map<String, Period> PERIOD_FOR_TIME_UNIT_MAP;

    static {
        Map<String, Period> periodMap = new HashMap<>();
        DAY_UNITS_SET.forEach(u -> periodMap.put(u, Period.ofDays(1)));
        WEEK_UNITS_SET.forEach(u -> periodMap.put(u, Period.ofWeeks(1)));
        MONTH_UNITS_SET.forEach(u -> periodMap.put(u, Period.ofMonths(1)));
        QUARTER_UNITS_SET.forEach(u -> periodMap.put(u, Period.ofMonths(3)));
        YEAR_UNITS_SET.forEach(u -> periodMap.put(u, Period.ofYears(1)));
        PERIOD_FOR_TIME_UNIT_MAP = ImmutableMap.copyOf(periodMap);
    }

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

    /**
     * Returns the relative {@link ZonedDateTime} corresponding to the given relative string and zoned date time.
     * @see <a href="docs/ppl-lang/functions/ppl-datetime#relative_timestamp">RELATIVE_TIMESTAMP</a> for more details.
     */
    public static ZonedDateTime getRelativeZonedDateTime(String relativeString, ZonedDateTime zonedDateTime) {

        ZonedDateTime relativeZonedDateTime = zonedDateTime;

        if (relativeString.equalsIgnoreCase(NOW)) {
            return zonedDateTime;
        }

        Matcher matcher = RELATIVE_PATTERN.matcher(relativeString);
        if (!matcher.matches()) {
            String message = String.format("The relative date time '%s' is not supported.", relativeString);
            throw new IllegalArgumentException(message);
        }


        if (matcher.group("offset") != null) {
            relativeZonedDateTime = applyOffset(
                    relativeZonedDateTime,
                    matcher.group("offsetSign"),
                    matcher.group("offsetValue"),
                    matcher.group("offsetUnit"));
        }

        if (matcher.group("snap") != null) {
            relativeZonedDateTime = applySnap(
                    relativeZonedDateTime,
                    matcher.group("snapUnit"));
        }

        return relativeZonedDateTime;
    }

    /**
     * Applies the offset specified by the offset sign, value,
     * and unit to the given zoned date time, and returns the result.
     */
    private ZonedDateTime applyOffset(ZonedDateTime zonedDateTime, String offsetSign, String offsetValue, String offsetUnit) {

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
            return zonedDateTime.plus(offsetDuration);
        }

        if (PERIOD_FOR_TIME_UNIT_MAP.containsKey(offsetUnitLowerCase)) {
            Period offsetPeriod = PERIOD_FOR_TIME_UNIT_MAP.get(offsetUnitLowerCase).multipliedBy(offsetValueInt);
            return zonedDateTime.plus(offsetPeriod);
        }

        String message = String.format("The relative date time unit '%s' is not supported.", offsetUnit);
        throw new IllegalArgumentException(message);
    }

    /**
     * Snaps the given zoned date time to the start of the previous time
     * period specified by the given snap unit, and returns the result.
     */
    private ZonedDateTime applySnap(ZonedDateTime zonedDateTime, String snapUnit) {

        // Convert to lower case to make case-insensitive.
        String snapUnitLowerCase = snapUnit.toLowerCase();

        if (SECOND_UNITS_SET.contains(snapUnitLowerCase)) {
            return zonedDateTime.truncatedTo(ChronoUnit.SECONDS);
        } else if (MINUTE_UNITS_SET.contains(snapUnitLowerCase)) {
            return zonedDateTime.truncatedTo(ChronoUnit.MINUTES);
        } else if (HOUR_UNITS_SET.contains(snapUnitLowerCase)) {
            return zonedDateTime.truncatedTo(ChronoUnit.HOURS);
        } else if (DAY_UNITS_SET.contains(snapUnitLowerCase)) {
            return zonedDateTime.truncatedTo(ChronoUnit.DAYS);
        } else if (WEEK_UNITS_SET.contains(snapUnitLowerCase)) {
            return applySnapToDayOfWeek(zonedDateTime, DayOfWeek.SUNDAY);
        } else if (MONTH_UNITS_SET.contains(snapUnitLowerCase)) {
            return zonedDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
        } else if (QUARTER_UNITS_SET.contains(snapUnitLowerCase)) {
            Month snapMonth = zonedDateTime.getMonth().firstMonthOfQuarter();
            return zonedDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).withMonth(snapMonth.getValue());
        } else if (YEAR_UNITS_SET.contains(snapUnitLowerCase)) {
            return zonedDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
        } else if (DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP.containsKey(snapUnitLowerCase)) {
            return applySnapToDayOfWeek(zonedDateTime, DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP.get(snapUnitLowerCase));
        }

        String message = String.format("The relative date time unit '%s' is not supported.", snapUnit);
        throw new IllegalArgumentException(message);
    }

    /**
     * Snaps the given date time to the start of the previous
     * specified day of the week, and returns the result.
     */
    private ZonedDateTime applySnapToDayOfWeek(ZonedDateTime zonedDateTime, DayOfWeek snapDayOfWeek) {
        ZonedDateTime snappedDateTime = zonedDateTime.truncatedTo(ChronoUnit.DAYS);

        int daysToSnap = zonedDateTime.getDayOfWeek().getValue() - snapDayOfWeek.getValue();
        if (daysToSnap < 0) daysToSnap += DayOfWeek.values().length;

        return snappedDateTime.minusDays(daysToSnap);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.collect.ImmutableMap;
import lombok.experimental.UtilityClass;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Period;
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
     * Returns the relative {@link LocalDateTime} corresponding to the given relative string and local date time.
     * <p>
     * The relative time string has syntax {@code [+|-]<offset_time_integer><offset_time_unit>@<snap_time_unit>}, and
     * is made up of two optional components:
     * <ul>
     *     <li>
     *         An offset from the current timestamp at the start of query execution, which is composed of a sign
     *         ({@code +} or {@code -}), an optional time integer, and a time unit. If the time integer is not specified,
     *         it defaults to one. For example, {@code +2hr} corresponds to two hours after the current timestamp, while
     *         {@code -mon} corresponds to one month ago.
     *     </li>
     *     <li>
     *         A snap-to time using the {@code @} symbol followed by a time unit. The snap-to time is applied after the
     *         offset (if specified), and rounds the time <i>down</i> to the start of the specified time unit (i.e.
     *         backwards in time). For example, {@code @wk} corresponds to the start of the current week (Sunday is
     *         considered to be the first day of the week).
     *     </li>
     * </ul>
     * <p>
     * The following offset time units are supported:
     * <table border="1">
     *     <body>
     *          <tr>
     *              <th>Time Unit</th>
     *              <th>Supported Keywords</th>
     *          </tr>
     *          <tr>
     *              <td>Seconds</td>
     *              <td>{@code s}, {@code sec}, {@code secs}, {@code second}, {@code seconds}</td>
     *          </tr>
     *          <tr>
     *              <td>Minutes</td>
     *              <td>{@code m}, {@code min}, {@code mins}, {@code minute}, {@code minutes}</td>
     *          </tr>
     *          <tr>
     *              <td>Hours</td>
     *              <td>{@code h}, {@code hr}, {@code hrs}, {@code hour}, {@code hours}</td>
     *          </tr>
     *          <tr>
     *              <td>Days</td>
     *              <td>{@code d}, {@code day}, {@code days}</td>
     *          </tr>
     *          <tr>
     *              <td>Weeks</td>
     *              <td>{@code w}, {@code wk}, {@code wks}, {@code week}, {@code weeks}</td>
     *          </tr>
     *          <tr>
     *              <td>Quarters</td>
     *              <td>{@code q}, {@code qtr}, {@code qtrs}, {@code quarter}, {@code quarters}</td>
     *          </tr>
     *          <tr>
     *              <td>Years</td>
     *              <td>{@code y}, {@code yr}, {@code yrs}, {@code year}, {@code years}</td>
     *          </tr>
     *      </body>
     * </table>
     * <p>
     * The snap-to time supports all the time units above, as well as the following day of the week time units:
     * <table border="1">
     *     <body>
     *          <tr>
     *              <th>Time Unit</th>
     *              <th>Supported Keywords</th>
     *          </tr>
     *          <tr>
     *              <td>Sunday</td>
     *              <td>{@code w0}, {@code w7}</td>
     *          </tr>
     *          <tr>
     *              <td>Monday</td>
     *              <td>{@code w1}</td>
     *          </tr>
     *          <tr>
     *              <td>Tuesday</td>
     *              <td>{@code w2}</td>
     *          </tr>
     *          <tr>
     *              <td>Wednesday</td>
     *              <td>{@code w3}</td>
     *          </tr>
     *          <tr>
     *              <td>Thursday</td>
     *              <td>{@code w4}</td>
     *          </tr>
     *          <tr>
     *              <td>Friday</td>
     *              <td>{@code w5}</td>
     *          </tr>
     *          <tr>
     *              <td>Saturday</td>
     *              <td>{@code w6}</td>
     *          </tr>
     *      </body>
     * </table>
     * <p>
     * The special relative time string {@code now} for the current timestamp is also supported.
     * <p>
     * For example, if the current timestamp is Monday, January 03, 2000 at 01:01:01 am:
     * <table border="1">
     *     <body>
     *          <tr>
     *              <th>Relative String</th>
     *              <th>Description</th>
     *              <th>Resulting Relative Time</th>
     *          </tr>
     *          <tr>
     *              <td>{@code -60m}</td>
     *              <td>Sixty minutes ago</td>
     *              <td>Monday, January 03, 2000 at 00:01:01 am</td>
     *          </tr>
     *          <tr>
     *              <td>{@code -h}</td>
     *              <td>One hour ago</td>
     *              <td>Monday, January 03, 2000 at 00:01:01 am</td>
     *          </tr>
     *          <tr>
     *              <td>{@code +2wk}</td>
     *              <td>Two weeks from now</td>
     *              <td>Monday, January 17, 2000 at 00:01:01 am</td>
     *          </tr>
     *          <tr>
     *              <td>{@code -1h@w3}</td>
     *              <td>One hour ago, rounded to the start of the previous Wednesday</td>
     *              <td>Wednesday, December 29, 1999 at 00:00:00 am</td>
     *          </tr>
     *          <tr>
     *              <td>{@code @d}</td>
     *              <td>Start of the current day</td>
     *              <td>Monday, January 03, 2000 at 00:00:00 am</td>
     *          </tr>
     *          <tr>
     *              <td>{@code now}</td>
     *              <td>Now</td>
     *              <td>Monday, January 03, 2000 at 01:01:01 am</td>
     *          </tr>
     *      </body>
     * </table>
     */
    public static LocalDateTime getRelativeLocalDateTime(String relativeString, LocalDateTime localDateTime) {

        LocalDateTime relativeLocalDateTime = localDateTime;

        if (relativeString.equalsIgnoreCase(NOW)) {
            return localDateTime;
        }

        Matcher matcher = RELATIVE_PATTERN.matcher(relativeString);
        if (!matcher.matches()) {
            String message = String.format("The relative date time '%s' is not supported.", relativeString);
            throw new IllegalArgumentException(message);
        }


        if (matcher.group("offset") != null) {
            relativeLocalDateTime = applyOffset(
                    relativeLocalDateTime,
                    matcher.group("offsetSign"),
                    matcher.group("offsetValue"),
                    matcher.group("offsetUnit"));
        }

        if (matcher.group("snap") != null) {
            relativeLocalDateTime = applySnap(
                    relativeLocalDateTime,
                    matcher.group("snapUnit"));
        }

        return relativeLocalDateTime;
    }

    /**
     * Applies the offset specified by the offset sign, value,
     * and unit to the given local date time, and returns the result.
     */
    private LocalDateTime applyOffset(LocalDateTime localDateTime, String offsetSign, String offsetValue, String offsetUnit) {

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
            return localDateTime.plus(offsetDuration);
        }

        if (PERIOD_FOR_TIME_UNIT_MAP.containsKey(offsetUnitLowerCase)) {
            Period offsetPeriod = PERIOD_FOR_TIME_UNIT_MAP.get(offsetUnitLowerCase).multipliedBy(offsetValueInt);
            return localDateTime.plus(offsetPeriod);
        }

        String message = String.format("The relative date time unit '%s' is not supported.", offsetUnit);
        throw new IllegalArgumentException(message);
    }

    /**
     * Snaps the given local date time to the start of the previous time
     * period specified by the given snap unit, and returns the result.
     */
    private LocalDateTime applySnap(LocalDateTime localDateTime, String snapUnit) {

        // Convert to lower case to make case-insensitive.
        String snapUnitLowerCase = snapUnit.toLowerCase();

        if (SECOND_UNITS_SET.contains(snapUnitLowerCase)) {
            return localDateTime.truncatedTo(ChronoUnit.SECONDS);
        } else if (MINUTE_UNITS_SET.contains(snapUnitLowerCase)) {
            return localDateTime.truncatedTo(ChronoUnit.MINUTES);
        } else if (HOUR_UNITS_SET.contains(snapUnitLowerCase)) {
            return localDateTime.truncatedTo(ChronoUnit.HOURS);
        } else if (DAY_UNITS_SET.contains(snapUnitLowerCase)) {
            return localDateTime.truncatedTo(ChronoUnit.DAYS);
        } else if (WEEK_UNITS_SET.contains(snapUnitLowerCase)) {
            return applySnapToDayOfWeek(localDateTime, DayOfWeek.SUNDAY);
        } else if (MONTH_UNITS_SET.contains(snapUnitLowerCase)) {
            return localDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
        } else if (QUARTER_UNITS_SET.contains(snapUnitLowerCase)) {
            Month snapMonth = localDateTime.getMonth().firstMonthOfQuarter();
            return localDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).withMonth(snapMonth.getValue());
        } else if (YEAR_UNITS_SET.contains(snapUnitLowerCase)) {
            return localDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
        } else if (DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP.containsKey(snapUnitLowerCase)) {
            return applySnapToDayOfWeek(localDateTime, DAY_OF_THE_WEEK_FOR_SNAP_UNIT_MAP.get(snapUnitLowerCase));
        }

        String message = String.format("The relative date time unit '%s' is not supported.", snapUnit);
        throw new IllegalArgumentException(message);
    }

    /**
     * Snaps the given date time to the start of the previous
     * specified day of the week, and returns the result.
     */
    private LocalDateTime applySnapToDayOfWeek(LocalDateTime dateTime, DayOfWeek snapDayOfWeek) {
        LocalDateTime snappedDateTime = dateTime.truncatedTo(ChronoUnit.DAYS);

        int daysToSnap = dateTime.getDayOfWeek().getValue() - snapDayOfWeek.getValue();
        if (daysToSnap < 0) daysToSnap += DayOfWeek.values().length;

        return snappedDateTime.minusDays(daysToSnap);
    }
}

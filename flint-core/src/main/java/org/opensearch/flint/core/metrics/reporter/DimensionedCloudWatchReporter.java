/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics.reporter;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.InvalidParameterValueException;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.metrics.sink.CloudWatchSink.DimensionNameGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.flint.core.metrics.reporter.DimensionUtils.constructDimension;

/**
 * Reports metrics to <a href="http://aws.amazon.com/cloudwatch/">Amazon's CloudWatch</a> periodically.
 * <p>
 * Use {@link Builder} to construct instances of this class. The {@link Builder}
 * allows to configure what aggregated metrics will be reported as a single {@link MetricDatum} to CloudWatch.
 * <p>
 * There are a bunch of {@code with*} methods that provide a sufficient fine-grained control over what metrics
 * should be reported.
 *
 * Forked from https://github.com/azagniotov/codahale-aggregated-metrics-cloudwatch-reporter.
 */
public class DimensionedCloudWatchReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DimensionedCloudWatchReporter.class);

    // Visible for testing
    public static final String DIMENSION_NAME_TYPE = "Type";

    // Visible for testing
    public static final String DIMENSION_GAUGE = "gauge";

    // Visible for testing
    public static final String DIMENSION_COUNT = "count";

    // Visible for testing
    public static final String DIMENSION_SNAPSHOT_SUMMARY = "snapshot-summary";

    // Visible for testing
    public static final String DIMENSION_SNAPSHOT_MEAN = "snapshot-mean";

    // Visible for testing
    public static final String DIMENSION_SNAPSHOT_STD_DEV = "snapshot-std-dev";

    /**
     * Amazon CloudWatch rejects values that are either too small or too large.
     * Values must be in the range of 8.515920e-109 to 1.174271e+108 (Base 10) or 2e-360 to 2e360 (Base 2).
     * <p>
     * In addition, special values (e.g., NaN, +Infinity, -Infinity) are not supported.
     */
    private static final double SMALLEST_SENDABLE_VALUE = 8.515920e-109;
    private static final double LARGEST_SENDABLE_VALUE = 1.174271e+108;

    private static Map<String, Dimension> constructedDimensions;

    /**
     * Each CloudWatch API request may contain at maximum 20 datums
     */
    private static final int MAXIMUM_DATUMS_PER_REQUEST = 20;

    /**
     * We only submit the difference in counters since the last submission. This way we don't have to reset
     * the counters within this application.
     */
    private final Map<Counting, Long> lastPolledCounts;

    private final Builder builder;
    private final String namespace;
    private final AmazonCloudWatchAsync cloudWatchAsyncClient;
    private final StandardUnit rateUnit;
    private final StandardUnit durationUnit;
    private final boolean shouldParseDimensionsFromName;
    private final boolean shouldAppendDropwizardTypeDimension;
    private MetricFilter filter;

    private DimensionedCloudWatchReporter(final Builder builder) {
        super(builder.metricRegistry, "coda-hale-metrics-cloud-watch-reporter", builder.metricFilter, builder.rateUnit, builder.durationUnit);
        this.builder = builder;
        this.namespace = builder.namespace;
        this.cloudWatchAsyncClient = builder.cloudWatchAsyncClient;
        this.lastPolledCounts = new ConcurrentHashMap<>();
        this.rateUnit = builder.cwRateUnit;
        this.durationUnit = builder.cwDurationUnit;
        this.shouldParseDimensionsFromName = builder.withShouldParseDimensionsFromName;
        this.shouldAppendDropwizardTypeDimension = builder.withShouldAppendDropwizardTypeDimension;
        this.constructedDimensions = new ConcurrentHashMap<>();
        this.filter = MetricFilter.ALL;
    }

    @Override
    public void report(final SortedMap<String, Gauge> gauges,
                       final SortedMap<String, Counter> counters,
                       final SortedMap<String, Histogram> histograms,
                       final SortedMap<String, Meter> meters,
                       final SortedMap<String, Timer> timers) {
        if (builder.withDryRun) {
            LOGGER.warn("** Reporter is running in 'DRY RUN' mode **");
        }

        try {
            final List<MetricDatum> metricData = new ArrayList<>(
                    gauges.size() + counters.size() + 10 * histograms.size() + 10 * timers.size());

            for (final Map.Entry<String, Gauge> gaugeEntry : gauges.entrySet()) {
                processGauge(gaugeEntry.getKey(), gaugeEntry.getValue(), metricData);
            }

            for (final Map.Entry<String, Counter> counterEntry : counters.entrySet()) {
                processCounter(counterEntry.getKey(), counterEntry.getValue(), metricData);
            }

            for (final Map.Entry<String, Histogram> histogramEntry : histograms.entrySet()) {
                processCounter(histogramEntry.getKey(), histogramEntry.getValue(), metricData);
                processHistogram(histogramEntry.getKey(), histogramEntry.getValue(), metricData);
            }

            for (final Map.Entry<String, Meter> meterEntry : meters.entrySet()) {
                processCounter(meterEntry.getKey(), meterEntry.getValue(), metricData);
                processMeter(meterEntry.getKey(), meterEntry.getValue(), metricData);
            }

            for (final Map.Entry<String, Timer> timerEntry : timers.entrySet()) {
                processCounter(timerEntry.getKey(), timerEntry.getValue(), metricData);
                processMeter(timerEntry.getKey(), timerEntry.getValue(), metricData);
                processTimer(timerEntry.getKey(), timerEntry.getValue(), metricData);
            }

            final Collection<List<MetricDatum>> metricDataPartitions = partition(metricData, MAXIMUM_DATUMS_PER_REQUEST);
            final List<Future<PutMetricDataResult>> cloudWatchFutures = new ArrayList<>(metricData.size());

            for (final List<MetricDatum> partition : metricDataPartitions) {
                final PutMetricDataRequest putMetricDataRequest = new PutMetricDataRequest()
                        .withNamespace(namespace)
                        .withMetricData(partition);

                if (builder.withDryRun) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Dry run - constructed PutMetricDataRequest: {}", putMetricDataRequest);
                    }
                } else {
                    cloudWatchFutures.add(cloudWatchAsyncClient.putMetricDataAsync(putMetricDataRequest));
                }
            }

            for (final Future<PutMetricDataResult> cloudWatchFuture : cloudWatchFutures) {
                try {
                    cloudWatchFuture.get();
                } catch (final Exception e) {
                    LOGGER.error("Error reporting metrics to CloudWatch. The data in this CloudWatch API request " +
                            "may have been discarded, did not make it to CloudWatch.", e);
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Sent {} metric datums to CloudWatch. Namespace: {}, metric data {}", metricData.size(), namespace, metricData);
            }

        } catch (final RuntimeException e) {
            LOGGER.error("Error marshalling CloudWatch metrics.", e);
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } catch (final Exception e) {
            LOGGER.error("Error when stopping the reporter.", e);
        } finally {
            if (!builder.withDryRun) {
                try {
                    cloudWatchAsyncClient.shutdown();
                } catch (final Exception e) {
                    LOGGER.error("Error shutting down AmazonCloudWatchAsync", cloudWatchAsyncClient, e);
                }
            }
        }
    }

    private void processGauge(final String metricName, final Gauge gauge, final List<MetricDatum> metricData) {
        if (gauge.getValue() instanceof Number) {
            final Number number = (Number) gauge.getValue();
            stageMetricDatum(true, metricName, number.doubleValue(), StandardUnit.None, DIMENSION_GAUGE, metricData);
        }
    }

    private void processCounter(final String metricName, final Counting counter, final List<MetricDatum> metricData) {
        long currentCount = counter.getCount();
        Long lastCount = lastPolledCounts.get(counter);
        lastPolledCounts.put(counter, currentCount);

        if (lastCount == null) {
            lastCount = 0L;
        }

        // Only submit metrics that have changed - let's save some money!
        final long delta = currentCount - lastCount;
        stageMetricDatum(true, metricName, delta, StandardUnit.Count, DIMENSION_COUNT, metricData);
    }

    /**
     * The rates of {@link Metered} are reported after being converted using the rate factor, which is deduced from
     * the set rate unit
     *
     * @see Timer#getSnapshot
     * @see #getRateUnit
     * @see #convertRate(double)
     */
    private void processMeter(final String metricName, final Metered meter, final List<MetricDatum> metricData) {
        final String formattedRate = String.format("-rate [per-%s]", getRateUnit());
        stageMetricDatum(builder.withOneMinuteMeanRate, metricName, convertRate(meter.getOneMinuteRate()), rateUnit, "1-min-mean" + formattedRate, metricData);
        stageMetricDatum(builder.withFiveMinuteMeanRate, metricName, convertRate(meter.getFiveMinuteRate()), rateUnit, "5-min-mean" + formattedRate, metricData);
        stageMetricDatum(builder.withFifteenMinuteMeanRate, metricName, convertRate(meter.getFifteenMinuteRate()), rateUnit, "15-min-mean" + formattedRate, metricData);
        stageMetricDatum(builder.withMeanRate, metricName, convertRate(meter.getMeanRate()), rateUnit, "mean" + formattedRate, metricData);
    }

    /**
     * The {@link Snapshot} values of {@link Timer} are reported as {@link StatisticSet} after conversion. The
     * conversion is done using the duration factor, which is deduced from the set duration unit.
     * <p>
     * Please note, the reported values submitted only if they show some data (greater than zero) in order to:
     * <p>
     * 1. save some money
     * 2. prevent com.amazonaws.services.cloudwatch.model.InvalidParameterValueException if empty {@link Snapshot}
     * is submitted
     * <p>
     * If {@link Builder#withZeroValuesSubmission()} is {@code true}, then all values will be submitted
     *
     * @see Timer#getSnapshot
     * @see #getDurationUnit
     * @see #convertDuration(double)
     */
    private void processTimer(final String metricName, final Timer timer, final List<MetricDatum> metricData) {
        final Snapshot snapshot = timer.getSnapshot();

        if (builder.withZeroValuesSubmission || snapshot.size() > 0) {
            for (final Percentile percentile : builder.percentiles) {
                final double convertedDuration = convertDuration(snapshot.getValue(percentile.getQuantile()));
                stageMetricDatum(true, metricName, convertedDuration, durationUnit, percentile.getDesc(), metricData);
            }
        }

        // prevent empty snapshot from causing InvalidParameterValueException
        if (snapshot.size() > 0) {
            final String formattedDuration = String.format(" [in-%s]", getDurationUnit());
            stageMetricDatum(builder.withArithmeticMean, metricName, convertDuration(snapshot.getMean()), durationUnit, DIMENSION_SNAPSHOT_MEAN + formattedDuration, metricData);
            stageMetricDatum(builder.withStdDev, metricName, convertDuration(snapshot.getStdDev()), durationUnit, DIMENSION_SNAPSHOT_STD_DEV + formattedDuration, metricData);
            stageMetricDatumWithConvertedSnapshot(builder.withStatisticSet, metricName, snapshot, durationUnit, metricData);
        }
    }

    /**
     * The {@link Snapshot} values of {@link Histogram} are reported as {@link StatisticSet} raw. In other words, the
     * conversion using the duration factor does NOT apply.
     * <p>
     * Please note, the reported values submitted only if they show some data (greater than zero) in order to:
     * <p>
     * 1. save some money
     * 2. prevent com.amazonaws.services.cloudwatch.model.InvalidParameterValueException if empty {@link Snapshot}
     * is submitted
     * <p>
     * If {@link Builder#withZeroValuesSubmission()} is {@code true}, then all values will be submitted
     *
     * @see Histogram#getSnapshot
     */
    private void processHistogram(final String metricName, final Histogram histogram, final List<MetricDatum> metricData) {
        final Snapshot snapshot = histogram.getSnapshot();

        if (builder.withZeroValuesSubmission || snapshot.size() > 0) {
            for (final Percentile percentile : builder.percentiles) {
                final double value = snapshot.getValue(percentile.getQuantile());
                stageMetricDatum(true, metricName, value, StandardUnit.None, percentile.getDesc(), metricData);
            }
        }

        // prevent empty snapshot from causing InvalidParameterValueException
        if (snapshot.size() > 0) {
            stageMetricDatum(builder.withArithmeticMean, metricName, snapshot.getMean(), StandardUnit.None, DIMENSION_SNAPSHOT_MEAN, metricData);
            stageMetricDatum(builder.withStdDev, metricName, snapshot.getStdDev(), StandardUnit.None, DIMENSION_SNAPSHOT_STD_DEV, metricData);
            stageMetricDatumWithRawSnapshot(builder.withStatisticSet, metricName, snapshot, StandardUnit.None, metricData);
        }
    }

    /**
     * Please note, the reported values submitted only if they show some data (greater than zero) in order to:
     * <p>
     * 1. save some money
     * 2. prevent com.amazonaws.services.cloudwatch.model.InvalidParameterValueException if empty {@link Snapshot}
     * is submitted
     * <p>
     * If {@link Builder#withZeroValuesSubmission()} is {@code true}, then all values will be submitted
     */
    private void stageMetricDatum(final boolean metricConfigured,
                                  final String metricName,
                                  final double metricValue,
                                  final StandardUnit standardUnit,
                                  final String dimensionValue,
                                  final List<MetricDatum> metricData) {
        // Only submit metrics that show some data, so let's save some money
        if (metricConfigured && (builder.withZeroValuesSubmission || metricValue > 0)) {
            final DimensionedName dimensionedName = DimensionedName.decode(metricName);
            // Add global dimensions for all metrics
            final Set<Dimension> dimensions = new LinkedHashSet<>(builder.globalDimensions);
            if (shouldAppendDropwizardTypeDimension) {
                dimensions.add(new Dimension().withName(DIMENSION_NAME_TYPE).withValue(dimensionValue));
            }

            MetricInfo metricInfo = getMetricInfo(dimensionedName, dimensions);
            for (Set<Dimension> dimensionSet : metricInfo.getDimensionSets()) {
                MetricDatum datum = new MetricDatum()
                        .withTimestamp(new Date(builder.clock.getTime()))
                        .withValue(cleanMetricValue(metricValue))
                        .withMetricName(metricInfo.getMetricName())
                        .withDimensions(dimensionSet)
                        .withUnit(standardUnit);
                metricData.add(datum);
            }
        }
    }

    /**
     * Constructs a {@link MetricInfo} object based on the provided {@link DimensionedName} and a set of additional dimensions.
     * This method processes the metric name contained within {@code dimensionedName} to potentially modify it based on naming conventions
     * and extracts or generates additional dimension sets for detailed metrics reporting.
     * <p>
     * If no specific naming convention is detected, the original set of dimensions is used as is. The method finally encapsulates the metric name
     * and the collection of dimension sets in a {@link MetricInfo} object and returns it.
     *
     * @param dimensionedName An instance of {@link DimensionedName} containing the original metric name and any directly associated dimensions.
     * @param dimensions A set of {@link Dimension} objects provided externally that should be associated with the metric.
     * @return A {@link MetricInfo} object containing the processed metric name and a list of dimension sets for metrics reporting.
     */
    private MetricInfo getMetricInfo(DimensionedName dimensionedName, Set<Dimension> dimensions) {
        // Add dimensions from dimensionedName
        dimensions.addAll(dimensionedName.getDimensions());

        String metricName = dimensionedName.getName();
        String[] parts = metricName.split("\\.");

        List<Set<Dimension>> dimensionSets = new ArrayList<>();
        if (DimensionUtils.doesNameConsistsOfMetricNameSpace(parts)) {
            metricName = constructMetricName(parts);
            // Get dimension sets corresponding to a specific metric source
            constructDimensionSets(dimensionSets, parts);
            // Add dimensions constructed above into each of the dimensionSets
            for (Set<Dimension> dimensionSet : dimensionSets) {
                // Create a copy of each set and add the additional dimensions
                dimensionSet.addAll(dimensions);
            }
        }

        if (dimensionSets.isEmpty()) {
            dimensionSets.add(dimensions);
        }
        return new MetricInfo(metricName, dimensionSets);
    }

    /**
     * Populates a list of dimension sets based on the metric source name extracted from the metric's parts
     * and predefined dimension groupings. This method aims to create detailed and structured dimension
     * sets for metrics, enhancing the granularity and relevance of metric reporting.
     *
     * If no predefined dimension groups exist for the metric source, or if the dimension name groups are
     * not initialized, the method exits without modifying the dimension sets list.
     *
     * @param dimensionSets A list to be populated with sets of {@link Dimension} objects, each representing
     *                      a group of dimensions relevant to the metric's source.
     * @param parts An array of strings derived from splitting the metric's name, used to extract information
     *              like the metric source name and to construct dimensions based on naming conventions.
     */
    private void constructDimensionSets(List<Set<Dimension>> dimensionSets, String[] parts) {
        String metricSourceName = parts[2];
        if (builder.dimensionNameGroups == null || builder.dimensionNameGroups.getDimensionGroups() == null || !builder.dimensionNameGroups.getDimensionGroups().containsKey(metricSourceName)) {
            return;
        }

        for (List<String> dimensionNames: builder.dimensionNameGroups.getDimensionGroups().get(metricSourceName)) {
            Set<Dimension> dimensions = new LinkedHashSet<>();
            for (String dimensionName: dimensionNames) {
                constructedDimensions.putIfAbsent(dimensionName, constructDimension(dimensionName, parts));
                dimensions.add(constructedDimensions.get(dimensionName));
            }
            dimensionSets.add(dimensions);
        }
    }

    /**
     * Constructs a metric name by removing the default prefix added by Spark.
     * This method also removes the metric source name if the metric is emitted from {@link org.apache.spark.metrics.source.FlintMetricSource}.
     * Assumes that the metric name parts include the source name as the third element.
     *
     * @param metricNameParts an array of strings representing parts of the metric name
     * @return a metric name constructed by omitting the default prefix added by Spark
     */
    private String constructMetricName(String[] metricNameParts) {
        // Determines the number of initial parts to skip based on the source name
        int partsToSkip = metricNameParts[2].equals("Flint") ? 3 : 2;
        return Stream.of(metricNameParts).skip(partsToSkip).collect(Collectors.joining("."));
    }

    private void stageMetricDatumWithConvertedSnapshot(final boolean metricConfigured,
                                                       final String metricName,
                                                       final Snapshot snapshot,
                                                       final StandardUnit standardUnit,
                                                       final List<MetricDatum> metricData) {
        if (metricConfigured) {
            final DimensionedName dimensionedName = DimensionedName.decode(metricName);
            double scaledSum = convertDuration(LongStream.of(snapshot.getValues()).sum());
            final StatisticSet statisticSet = new StatisticSet()
                    .withSum(scaledSum)
                    .withSampleCount((double) snapshot.size())
                    .withMinimum(convertDuration(snapshot.getMin()))
                    .withMaximum(convertDuration(snapshot.getMax()));

            final Set<Dimension> dimensions = new LinkedHashSet<>(builder.globalDimensions);
            dimensions.add(new Dimension().withName(DIMENSION_NAME_TYPE).withValue(DIMENSION_SNAPSHOT_SUMMARY));
            dimensions.addAll(dimensionedName.getDimensions());

            metricData.add(new MetricDatum()
                    .withTimestamp(new Date(builder.clock.getTime()))
                    .withMetricName(dimensionedName.getName())
                    .withDimensions(dimensions)
                    .withStatisticValues(statisticSet)
                    .withUnit(standardUnit));
        }
    }

    private void stageMetricDatumWithRawSnapshot(final boolean metricConfigured,
                                                 final String metricName,
                                                 final Snapshot snapshot,
                                                 final StandardUnit standardUnit,
                                                 final List<MetricDatum> metricData) {
        if (metricConfigured) {
            final DimensionedName dimensionedName = DimensionedName.decode(metricName);
            double total = LongStream.of(snapshot.getValues()).sum();
            final StatisticSet statisticSet = new StatisticSet()
                    .withSum(total)
                    .withSampleCount((double) snapshot.size())
                    .withMinimum((double) snapshot.getMin())
                    .withMaximum((double) snapshot.getMax());

            final Set<Dimension> dimensions = new LinkedHashSet<>(builder.globalDimensions);
            dimensions.add(new Dimension().withName(DIMENSION_NAME_TYPE).withValue(DIMENSION_SNAPSHOT_SUMMARY));
            dimensions.addAll(dimensionedName.getDimensions());

            metricData.add(new MetricDatum()
                    .withTimestamp(new Date(builder.clock.getTime()))
                    .withMetricName(dimensionedName.getName())
                    .withDimensions(dimensions)
                    .withStatisticValues(statisticSet)
                    .withUnit(standardUnit));
        }
    }

    private double cleanMetricValue(final double metricValue) {
        double absoluteValue = Math.abs(metricValue);
        if (absoluteValue < SMALLEST_SENDABLE_VALUE) {
            // Allow 0 through untouched, everything else gets rounded to SMALLEST_SENDABLE_VALUE
            if (absoluteValue > 0) {
                if (metricValue < 0) {
                    return -SMALLEST_SENDABLE_VALUE;
                } else {
                    return SMALLEST_SENDABLE_VALUE;
                }
            }
        } else if (absoluteValue > LARGEST_SENDABLE_VALUE) {
            if (metricValue < 0) {
                return -LARGEST_SENDABLE_VALUE;
            } else {
                return LARGEST_SENDABLE_VALUE;
            }
        }
        return metricValue;
    }

    private static <T> Collection<List<T>> partition(final Collection<T> wholeCollection, final int partitionSize) {
        final int[] itemCounter = new int[]{0};

        return wholeCollection.stream()
                .collect(Collectors.groupingBy(item -> itemCounter[0]++ / partitionSize))
                .values();
    }

    /**
     * Creates a new {@link Builder} that sends values from the given {@link MetricRegistry} to the given namespace
     * using the given CloudWatch client.
     *
     * @param metricRegistry {@link MetricRegistry} instance
     * @param client         {@link AmazonCloudWatchAsync} instance
     * @param namespace      the namespace. Must be non-null and not empty.
     * @return {@link Builder} instance
     */
    public static Builder forRegistry(
            final MetricRegistry metricRegistry,
            final AmazonCloudWatchAsync client,
            final String namespace) {
        return new Builder(metricRegistry, client, namespace);
    }

    public enum Percentile {
        P50(0.50, "50%"),
        P75(0.75, "75%"),
        P95(0.95, "95%"),
        P98(0.98, "98%"),
        P99(0.99, "99%"),
        P995(0.995, "99.5%"),
        P999(0.999, "99.9%");

        private final double quantile;
        private final String desc;

        Percentile(final double quantile, final String desc) {
            this.quantile = quantile;
            this.desc = desc;
        }

        public double getQuantile() {
            return quantile;
        }

        public String getDesc() {
            return desc;
        }
    }

    public static class MetricInfo {
        private String metricName;
        private List<Set<Dimension>> dimensionSets;

        public MetricInfo(String metricName, List<Set<Dimension>> dimensionSets) {
            this.metricName = metricName;
            this.dimensionSets = dimensionSets;
        }

        public String getMetricName() {
            return metricName;
        }

        public List<Set<Dimension>> getDimensionSets() {
            return dimensionSets;
        }
    }


    public static class Builder {

        private final String namespace;
        private final AmazonCloudWatchAsync cloudWatchAsyncClient;
        private final MetricRegistry metricRegistry;

        private Percentile[] percentiles;
        private boolean withOneMinuteMeanRate;
        private boolean withFiveMinuteMeanRate;
        private boolean withFifteenMinuteMeanRate;
        private boolean withMeanRate;
        private boolean withArithmeticMean;
        private boolean withStdDev;
        private boolean withDryRun;
        private boolean withZeroValuesSubmission;
        private boolean withStatisticSet;
        private boolean withJvmMetrics;
        private boolean withShouldParseDimensionsFromName;
        private boolean withShouldAppendDropwizardTypeDimension=true;
        private MetricFilter metricFilter;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private StandardUnit cwRateUnit;
        private StandardUnit cwDurationUnit;
        private Set<Dimension> globalDimensions;
        private DimensionNameGroups dimensionNameGroups;
        private final Clock clock;

        private Builder(
                final MetricRegistry metricRegistry,
                final AmazonCloudWatchAsync cloudWatchAsyncClient,
                final String namespace) {
            this.metricRegistry = metricRegistry;
            this.cloudWatchAsyncClient = cloudWatchAsyncClient;
            this.namespace = namespace;
            this.percentiles = new Percentile[]{Percentile.P75, Percentile.P95, Percentile.P999};
            this.metricFilter = MetricFilter.ALL;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.globalDimensions = new LinkedHashSet<>();
            this.cwRateUnit = toStandardUnit(rateUnit);
            this.cwDurationUnit = toStandardUnit(durationUnit);
            this.clock = Clock.defaultClock();
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(final TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(final TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param metricFilter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(final MetricFilter metricFilter) {
            this.metricFilter = metricFilter;
            return this;
        }

        /**
         * If the one minute rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
         * <p>
         * The rate values are converted before reporting based on the rate unit set
         *
         * @return {@code this}
         * @see ScheduledReporter#convertRate(double)
         * @see Meter#getOneMinuteRate()
         * @see Timer#getOneMinuteRate()
         */
        public Builder withOneMinuteMeanRate() {
            withOneMinuteMeanRate = true;
            return this;
        }

        /**
         * If the five minute rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
         * <p>
         * The rate values are converted before reporting based on the rate unit set
         *
         * @return {@code this}
         * @see ScheduledReporter#convertRate(double)
         * @see Meter#getFiveMinuteRate()
         * @see Timer#getFiveMinuteRate()
         */
        public Builder withFiveMinuteMeanRate() {
            withFiveMinuteMeanRate = true;
            return this;
        }

        /**
         * If the fifteen minute rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
         * <p>
         * The rate values are converted before reporting based on the rate unit set
         *
         * @return {@code this}
         * @see ScheduledReporter#convertRate(double)
         * @see Meter#getFifteenMinuteRate()
         * @see Timer#getFifteenMinuteRate()
         */
        public Builder withFifteenMinuteMeanRate() {
            withFifteenMinuteMeanRate = true;
            return this;
        }

        /**
         * If the mean rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
         * <p>
         * The rate values are converted before reporting based on the rate unit set
         *
         * @return {@code this}
         * @see ScheduledReporter#convertRate(double)
         * @see Meter#getMeanRate()
         * @see Timer#getMeanRate()
         */
        public Builder withMeanRate() {
            withMeanRate = true;
            return this;
        }

        /**
         * If the arithmetic mean of {@link Snapshot} values in {@link Histogram} and {@link Timer} should be sent.
         * {@code false} by default.
         * <p>
         * The {@link Timer#getSnapshot()} values are converted before reporting based on the duration unit set
         * The {@link Histogram#getSnapshot()} values are reported as is
         *
         * @return {@code this}
         * @see ScheduledReporter#convertDuration(double)
         * @see Snapshot#getMean()
         */
        public Builder withArithmeticMean() {
            withArithmeticMean = true;
            return this;
        }

        /**
         * If the standard deviation of {@link Snapshot} values in {@link Histogram} and {@link Timer} should be sent.
         * {@code false} by default.
         * <p>
         * The {@link Timer#getSnapshot()} values are converted before reporting based on the duration unit set
         * The {@link Histogram#getSnapshot()} values are reported as is
         *
         * @return {@code this}
         * @see ScheduledReporter#convertDuration(double)
         * @see Snapshot#getStdDev()
         */
        public Builder withStdDev() {
            withStdDev = true;
            return this;
        }

        /**
         * If lifetime {@link Snapshot} summary of {@link Histogram} and {@link Timer} should be translated
         * to {@link StatisticSet} in the most direct way possible and reported. {@code false} by default.
         * <p>
         * The {@link Snapshot} duration values are converted before reporting based on the duration unit set
         *
         * @return {@code this}
         * @see ScheduledReporter#convertDuration(double)
         */
        public Builder withStatisticSet() {
            withStatisticSet = true;
            return this;
        }

        /**
         * If JVM statistic should be reported. Supported metrics include:
         * <p>
         * - Run count and elapsed times for all supported garbage collectors
         * - Memory usage for all memory pools, including off-heap memory
         * - Breakdown of thread states, including deadlocks
         * - File descriptor usage
         * - Buffer pool sizes and utilization (Java 7 only)
         * <p>
         * {@code false} by default.
         *
         * @return {@code this}
         */
        public Builder withJvmMetrics() {
            withJvmMetrics = true;
            return this;
        }

        /**
         * If CloudWatch dimensions should be parsed off the the metric name:
         *
         * {@code false} by default.
         *
         * @return {@code this}
         */
        public Builder withShouldParseDimensionsFromName(final boolean value) {
            withShouldParseDimensionsFromName = value;
            return this;
        }

        /**
         * If the Dropwizard metric type should be reported as a CloudWatch dimension.
         *
         * {@code false} by default.
         *
         * @return {@code this}
         */
        public Builder withShouldAppendDropwizardTypeDimension(final boolean value) {
            withShouldAppendDropwizardTypeDimension = value;
            return this;
        }

        public Builder withDimensionNameGroups(final DimensionNameGroups dimensionNameGroups) {
            this.dimensionNameGroups = dimensionNameGroups;
            return this;
        }

        /**
         * Does not actually POST to CloudWatch, logs the {@link PutMetricDataRequest putMetricDataRequest} instead.
         * {@code false} by default.
         *
         * @return {@code this}
         */
        public Builder withDryRun() {
            withDryRun = true;
            return this;
        }

        /**
         * POSTs to CloudWatch all values. Otherwise, the reporter does not POST values which are zero in order to save
         * costs. Also, some users have been experiencing {@link InvalidParameterValueException} when submitting zero
         * values. Please refer to:
         * https://github.com/azagniotov/codahale-aggregated-metrics-cloudwatch-reporter/issues/4
         * <p>
         * {@code false} by default.
         *
         * @return {@code this}
         */
        public Builder withZeroValuesSubmission() {
            withZeroValuesSubmission = true;
            return this;
        }

        /**
         * The {@link Histogram} and {@link Timer} percentiles to send. If <code>0.5</code> is included, it'll be
         * reported as <code>median</code>.This defaults to <code>0.75, 0.95 and 0.999</code>.
         * <p>
         * The {@link Timer#getSnapshot()} percentile values are converted before reporting based on the duration unit
         * The {@link Histogram#getSnapshot()} percentile values are reported as is
         *
         * @param percentiles the percentiles to send. Replaces the default percentiles.
         * @return {@code this}
         */
        public Builder withPercentiles(final Percentile... percentiles) {
            if (percentiles.length > 0) {
                this.percentiles = percentiles;
            }
            return this;
        }

        /**
         * Global {@link Set} of {@link Dimension} to send with each {@link MetricDatum}. A dimension is a name/value
         * pair that helps you to uniquely identify a metric. Every metric has specific characteristics that describe
         * it, and you can think of dimensions as categories for those characteristics.
         * <p>
         * Whenever you add a unique name/value pair to one of your metrics, you are creating a new metric.
         * Defaults to {@code empty} {@link Set}.
         *
         * @param dimensions arguments in a form of {@code name=value}. The number of arguments is variable and may be
         *                   zero. The maximum number of arguments is limited by the maximum dimension of a Java array
         *                   as defined by the Java Virtual Machine Specification. Each {@code name=value} string
         *                   will be converted to an instance of {@link Dimension}
         * @return {@code this}
         */
        public Builder withGlobalDimensions(final String... dimensions) {
            for (final String pair : dimensions) {
                final List<String> splitted = Stream.of(pair.split("=")).map(String::trim).collect(Collectors.toList());
                this.globalDimensions.add(new Dimension().withName(splitted.get(0)).withValue(splitted.get(1)));
            }
            return this;
        }

        public DimensionedCloudWatchReporter build() {

            if (withJvmMetrics) {
                metricRegistry.register("jvm.uptime", (Gauge<Long>) () -> ManagementFactory.getRuntimeMXBean().getUptime());
                metricRegistry.register("jvm.current_time", (Gauge<Long>) clock::getTime);
                metricRegistry.register("jvm.classes", new ClassLoadingGaugeSet());
                metricRegistry.register("jvm.fd_usage", new FileDescriptorRatioGauge());
                metricRegistry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
                metricRegistry.register("jvm.gc", new GarbageCollectorMetricSet());
                metricRegistry.register("jvm.memory", new MemoryUsageGaugeSet());
                metricRegistry.register("jvm.thread-states", new ThreadStatesGaugeSet());
            }

            cwRateUnit = toStandardUnit(rateUnit);
            cwDurationUnit = toStandardUnit(durationUnit);

            return new DimensionedCloudWatchReporter(this);
        }

        private StandardUnit toStandardUnit(final TimeUnit timeUnit) {
            switch (timeUnit) {
                case SECONDS:
                    return StandardUnit.Seconds;
                case MILLISECONDS:
                    return StandardUnit.Milliseconds;
                case MICROSECONDS:
                    return StandardUnit.Microseconds;
                default:
                    throw new IllegalArgumentException("Unsupported TimeUnit: " + timeUnit);
            }
        }
    }
}

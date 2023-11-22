/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.metrics.sink;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.spark.SecurityManager;
import org.opensearch.flint.core.metrics.reporter.DimensionedCloudWatchReporter;
import org.opensearch.flint.core.metrics.reporter.DimensionedName;
import org.opensearch.flint.core.metrics.reporter.InvalidMetricsPropertyException;

/**
 * Implementation of the Spark metrics {@link Sink} interface
 * for reporting internal Spark metrics into CloudWatch. Spark's metric system uses DropWizard's
 * metric library internally, so this class simply wraps the {@link DimensionedCloudWatchReporter}
 * with the constructor and methods mandated for Spark metric Sinks.
 *
 * @see org.apache.spark.metrics.MetricsSystem
 * @see ScheduledReporter
 * @author kmccaw
 */
public class CloudWatchSink implements Sink {

    private final ScheduledReporter reporter;

    private final long pollingPeriod;

    private final boolean shouldParseInlineDimensions;

    private final boolean shouldAppendDropwizardTypeDimension;

    private final TimeUnit pollingTimeUnit;

    /**
     * Constructor with the signature required by Spark, which loads the class through reflection.
     *
     * @see org.apache.spark.metrics.MetricsSystem
     * @param properties Properties for this sink defined in Spark's "metrics.properties" configuration file.
     * @param metricRegistry The DropWizard MetricRegistry used by Sparks {@link org.apache.spark.metrics.MetricsSystem}
     * @param securityManager Unused argument; required by the Spark sink constructor signature.
     */
    public CloudWatchSink(
            final Properties properties,
            final MetricRegistry metricRegistry,
            final SecurityManager securityManager) {
        // First extract properties defined in the Spark metrics configuration

        // Extract the required namespace property. This is used as the namespace
        // for all metrics reported to CloudWatch
        final Optional<String> namespaceProperty = getProperty(properties, PropertyKeys.NAMESPACE);
        if (!namespaceProperty.isPresent()) {
            final String message = "CloudWatch Spark metrics sink requires '"
                    + PropertyKeys.NAMESPACE + "' property.";
            throw new InvalidMetricsPropertyException(message);
        }

        // Extract the optional AWS credentials. If either of the access or secret keys are
        // missing in the properties, fall back to using the credentials of the EC2 instance.
        final Optional<String> accessKeyProperty = getProperty(properties, PropertyKeys.AWS_ACCESS_KEY_ID);
        final Optional<String> secretKeyProperty = getProperty(properties, PropertyKeys.AWS_SECRET_KEY);
        final AWSCredentialsProvider awsCredentialsProvider;
        if (accessKeyProperty.isPresent() && secretKeyProperty.isPresent()) {
            final AWSCredentials awsCredentials = new BasicAWSCredentials(
                    accessKeyProperty.get(),
                    secretKeyProperty.get());
            awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        } else {
            // If the AWS credentials aren't specified in the properties, fall back to using the
            // DefaultAWSCredentialsProviderChain, which looks for credentials in the order
            // (1) Environment Variables
            // (2) Java System Properties
            // (3) Credentials file at ~/.aws/credentials
            // (4) AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
            // (5) EC2 Instance profile credentials
            awsCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        }

        // Extract the AWS region CloudWatch metrics should be reported to.
        final Optional<String> regionProperty = getProperty(properties, PropertyKeys.AWS_REGION);
        final Regions awsRegion;
        if (regionProperty.isPresent()) {
            try {
                awsRegion = Regions.fromName(regionProperty.get());
            } catch (IllegalArgumentException e) {
                final String message = String.format(
                        "Unable to parse value (%s) for the \"%s\" CloudWatchSink metrics property.",
                        regionProperty.get(),
                        PropertyKeys.AWS_REGION);
                throw new InvalidMetricsPropertyException(message, e);
            }
        } else {
            final AwsRegionProvider regionProvider = new DefaultAwsRegionProviderChain();
            awsRegion = Regions.fromName(regionProvider.getRegion());
        }

        // Extract the polling period, the interval at which metrics are reported.
        final Optional<String> pollingPeriodProperty = getProperty(properties, PropertyKeys.POLLING_PERIOD);
        if (pollingPeriodProperty.isPresent()) {
            try {
                final long parsedPollingPeriod = Long.parseLong(pollingPeriodProperty.get());
                // Confirm that the value of this property is a positive number
                if (parsedPollingPeriod <= 0) {
                    final String message = String.format(
                            "The value (%s) of the \"%s\" CloudWatchSink metrics property is non-positive.",
                            pollingPeriodProperty.get(),
                            PropertyKeys.POLLING_PERIOD);
                    throw new InvalidMetricsPropertyException(message);
                }
                pollingPeriod = parsedPollingPeriod;
            } catch (NumberFormatException e) {
                final String message = String.format(
                        "Unable to parse value (%s) for the \"%s\" CloudWatchSink metrics property.",
                        pollingPeriodProperty.get(),
                        PropertyKeys.POLLING_PERIOD);
                throw new InvalidMetricsPropertyException(message, e);
            }
        } else {
            pollingPeriod = PropertyDefaults.POLLING_PERIOD;
        }

        final Optional<String> pollingTimeUnitProperty = getProperty(properties, PropertyKeys.POLLING_TIME_UNIT);
        if (pollingTimeUnitProperty.isPresent()) {
            try {
                pollingTimeUnit = TimeUnit.valueOf(pollingTimeUnitProperty.get().toUpperCase());
            } catch (IllegalArgumentException e) {
                final String message = String.format(
                        "Unable to parse value (%s) for the \"%s\" CloudWatchSink metrics property.",
                        pollingTimeUnitProperty.get(),
                        PropertyKeys.POLLING_TIME_UNIT);
                throw new InvalidMetricsPropertyException(message, e);
            }
        } else {
            pollingTimeUnit = PropertyDefaults.POLLING_PERIOD_TIME_UNIT;
        }

        // Extract the inline dimension parsing setting.
        final Optional<String> shouldParseInlineDimensionsProperty = getProperty(
                properties,
                PropertyKeys.SHOULD_PARSE_INLINE_DIMENSIONS);
        if (shouldParseInlineDimensionsProperty.isPresent()) {
            try {
                shouldParseInlineDimensions = Boolean.parseBoolean(shouldParseInlineDimensionsProperty.get());
            } catch (IllegalArgumentException e) {
                final String message = String.format(
                        "Unable to parse value (%s) for the \"%s\" CloudWatchSink metrics property.",
                        shouldParseInlineDimensionsProperty.get(),
                        PropertyKeys.SHOULD_PARSE_INLINE_DIMENSIONS);
                throw new InvalidMetricsPropertyException(message, e);
            }
        } else {
            shouldParseInlineDimensions = PropertyDefaults.SHOULD_PARSE_INLINE_DIMENSIONS;
        }

        // Extract the setting to append dropwizard metrics types as a dimension
        final Optional<String> shouldAppendDropwizardTypeDimensionProperty = getProperty(
                properties,
                PropertyKeys.SHOULD_APPEND_DROPWIZARD_TYPE_DIMENSION);
        if (shouldAppendDropwizardTypeDimensionProperty.isPresent()) {
            try {
                shouldAppendDropwizardTypeDimension = Boolean.parseBoolean(shouldAppendDropwizardTypeDimensionProperty.get());
            } catch (IllegalArgumentException e) {
                final String message = String.format(
                        "Unable to parse value (%s) for the \"%s\" CloudWatchSink metrics property.",
                        shouldAppendDropwizardTypeDimensionProperty.get(),
                        PropertyKeys.SHOULD_APPEND_DROPWIZARD_TYPE_DIMENSION);
                throw new InvalidMetricsPropertyException(message, e);
            }
        } else {
            shouldAppendDropwizardTypeDimension = PropertyDefaults.SHOULD_PARSE_INLINE_DIMENSIONS;
        }

        final Optional<String> metricFilterRegex = getProperty(
            properties,
            PropertyKeys.METRIC_FILTER_REGEX);
        MetricFilter metricFilter;
        if (metricFilterRegex.isPresent()) {
            Pattern pattern = Pattern.compile(metricFilterRegex.get());
            metricFilter = (name, metric) -> pattern.matcher(DimensionedName.decode(name).getName()).find();
        } else {
            metricFilter = MetricFilter.ALL;
        }

        final AmazonCloudWatchAsync cloudWatchClient = AmazonCloudWatchAsyncClient.asyncBuilder()
                .withCredentials(awsCredentialsProvider)
                .withRegion(awsRegion)
                .build();

        this.reporter = DimensionedCloudWatchReporter.forRegistry(metricRegistry, cloudWatchClient, namespaceProperty.get())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(metricFilter)
                .withPercentiles(
                        DimensionedCloudWatchReporter.Percentile.P50,
                        DimensionedCloudWatchReporter.Percentile.P75,
                        DimensionedCloudWatchReporter.Percentile.P99)
                .withOneMinuteMeanRate()
                .withFiveMinuteMeanRate()
                .withFifteenMinuteMeanRate()
                .withMeanRate()
                .withArithmeticMean()
                .withStdDev()
                .withStatisticSet()
                .withGlobalDimensions()
                .withShouldParseDimensionsFromName(shouldParseInlineDimensions)
                .withShouldAppendDropwizardTypeDimension(shouldAppendDropwizardTypeDimension)
                .build();
    }

    @Override
    public void start() {
        reporter.start(pollingPeriod, pollingTimeUnit);
    }

    @Override
    public void stop() {
        reporter.stop();
    }

    @Override
    public void report() {
        reporter.report();
    }

    /**
     * Returns the value for specified property key as an Optional.
     * @param properties
     * @param key
     * @return
     */
    private static Optional<String> getProperty(Properties properties, final String key) {
        return Optional.ofNullable(properties.getProperty(key));
    }

    /**
     * The keys used in the metrics properties configuration file.
     */
    private static class PropertyKeys {
        static final String NAMESPACE = "namespace";
        static final String AWS_ACCESS_KEY_ID = "awsAccessKeyId";
        static final String AWS_SECRET_KEY = "awsSecretKey";
        static final String AWS_REGION = "awsRegion";
        static final String POLLING_PERIOD = "pollingPeriod";
        static final String POLLING_TIME_UNIT = "pollingTimeUnit";
        static final String SHOULD_PARSE_INLINE_DIMENSIONS = "shouldParseInlineDimensions";
        static final String SHOULD_APPEND_DROPWIZARD_TYPE_DIMENSION = "shouldAppendDropwizardTypeDimension";
        static final String METRIC_FILTER_REGEX = "regex";
    }

    /**
     * The default values for optional properties in the metrics properties configuration file.
     */
    private static class PropertyDefaults {
        static final long POLLING_PERIOD = 1;
        static final TimeUnit POLLING_PERIOD_TIME_UNIT = TimeUnit.MINUTES;
        static final boolean SHOULD_PARSE_INLINE_DIMENSIONS = false;
    }
}

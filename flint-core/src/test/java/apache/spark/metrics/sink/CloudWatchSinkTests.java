/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package apache.spark.metrics.sink;

import org.apache.spark.SecurityManager;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.sink.CloudWatchSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.util.Properties;
import org.opensearch.flint.core.metrics.reporter.InvalidMetricsPropertyException;

class CloudWatchSinkTests {
    private final MetricRegistry metricRegistry = Mockito.mock(MetricRegistry.class);
    private final SecurityManager securityManager = Mockito.mock(SecurityManager.class);

    @Test
    void should_throwException_when_namespacePropertyIsNotSet() {
        final Properties properties = getDefaultValidProperties();
        properties.remove("namespace");
        final Executable executable = () -> {
            final CloudWatchSink
                cloudWatchSink = new CloudWatchSink(properties, metricRegistry, securityManager);
        };
        Assertions.assertThrows(InvalidMetricsPropertyException.class, executable);
    }

    @Test
    void should_throwException_when_awsPropertyIsInvalid() {
        final Properties properties = getDefaultValidProperties();
        properties.setProperty("awsRegion", "someInvalidRegion");
        final Executable executable = () -> {
            final CloudWatchSink cloudWatchSink = new CloudWatchSink(properties, metricRegistry, securityManager);
        };
        Assertions.assertThrows(InvalidMetricsPropertyException.class, executable);
    }

    @Test
    void should_throwException_when_pollingPeriodPropertyIsNotANumber() {
        final Properties properties = getDefaultValidProperties();
        properties.setProperty("pollingPeriod", "notANumber");
        final Executable executable = () -> {
            final CloudWatchSink cloudWatchSink = new CloudWatchSink(properties, metricRegistry, securityManager);
        };
        Assertions.assertThrows(InvalidMetricsPropertyException.class, executable);
    }

    @Test
    void should_throwException_when_pollingPeriodPropertyIsNegative() {
        final Properties properties = getDefaultValidProperties();
        properties.setProperty("pollingPeriod", "-5");
        final Executable executable = () -> {
            final CloudWatchSink cloudWatchSink = new CloudWatchSink(properties, metricRegistry, securityManager);
        };
        Assertions.assertThrows(InvalidMetricsPropertyException.class, executable);
    }

    @Test
    void should_throwException_when_pollingTimeUnitPropertyIsInvalid() {
        final Properties properties = getDefaultValidProperties();
        properties.setProperty("pollingTimeUnit", "notATimeUnitValue");
        final Executable executable = () -> {
            final CloudWatchSink cloudWatchSink = new CloudWatchSink(properties, metricRegistry, securityManager);
        };
        Assertions.assertThrows(InvalidMetricsPropertyException.class, executable);
    }

    private Properties getDefaultValidProperties() {
        final Properties properties = new Properties();
        properties.setProperty("namespace", "namespaceValue");
        properties.setProperty("awsAccessKeyId", "awsAccessKeyIdValue");
        properties.setProperty("awsSecretKey", "awsSecretKeyValue");
        properties.setProperty("awsRegion", "us-east-1");
        properties.setProperty("pollingPeriod", "1");
        properties.setProperty("pollingTimeUnit", "MINUTES");
        return properties;
    }
}

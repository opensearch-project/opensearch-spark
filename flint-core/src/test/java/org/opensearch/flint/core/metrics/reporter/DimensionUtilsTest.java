/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics.reporter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.cloudwatch.model.Dimension;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.Field;
import java.util.Map;

public class DimensionUtilsTest {
    private static final String[] parts = {"someMetric", "123", "dummySource"};

    @Test
    void testConstructDimensionThrowsIllegalArgumentException() {
        String dimensionName = "InvalidDimension";
        String[] metricNameParts = {};

        final Executable executable = () -> {
            DimensionUtils.constructDimension(dimensionName, metricNameParts);
        };
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, executable);
        Assertions.assertEquals("The provided metric name parts do not consist of a valid metric namespace.", exception.getMessage());
    }
    @Test
    public void testGetInstanceRoleDimensionWithExecutor() {
        Dimension result = DimensionUtils.constructDimension("instanceRole", parts);
        assertEquals("instanceRole", result.getName());
        assertEquals("executor", result.getValue());
    }

    @Test
    public void testGetInstanceRoleDimensionWithRoleName() {
        String[] parts = {"someMetric", "driver", "dummySource"};
        Dimension result = DimensionUtils.constructDimension("instanceRole", parts);
        assertEquals("instanceRole", result.getName());
        assertEquals("driver", result.getValue());
    }

    @Test
    public void testGetDefaultDimensionWithUnknown() {
        Dimension result = DimensionUtils.constructDimension("nonExistentDimension", parts);
        assertEquals("nonExistentDimension", result.getName());
        assertEquals("UNKNOWN", result.getValue());
    }

    @Test
    public void testGetDimensionsFromSystemEnv() throws NoSuchFieldException, IllegalAccessException {
        Class<?> classOfMap = System.getenv().getClass();
        Field field = classOfMap.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writeableEnvironmentVariables = (Map<String, String>)field.get(System.getenv());
        writeableEnvironmentVariables.put("TEST_VAR", "dummy1");
        writeableEnvironmentVariables.put("SERVERLESS_EMR_JOB_ID", "dummy2");
        Dimension result1 = DimensionUtils.constructDimension("TEST_VAR", parts);
        assertEquals("TEST_VAR", result1.getName());
        assertEquals("dummy1", result1.getValue());
        Dimension result2 = DimensionUtils.constructDimension("jobId", parts);
        assertEquals("jobId", result2.getName());
        assertEquals("dummy2", result2.getValue());
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics.reporter;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.cloudwatch.model.Dimension;

import java.lang.reflect.Field;
import java.util.Map;

public class DimensionUtilsTest {
    @Test
    public void testGetInstanceRoleDimensionWithExecutor() {
        String[] parts = {"someMetric", "123"};
        Dimension result = DimensionUtils.constructDimension("instanceRole", parts);
        assertEquals("instanceRole", result.getName());
        assertEquals("executor", result.getValue());
    }

    @Test
    public void testGetInstanceRoleDimensionWithRoleName() {
        String[] parts = {"someMetric", "driver"};
        Dimension result = DimensionUtils.constructDimension("instanceRole", parts);
        assertEquals("instanceRole", result.getName());
        assertEquals("driver", result.getValue());
    }

    @Test
    public void testGetDefaultDimensionWithUnknown() {
        String[] parts = {"someMetric", "123"};
        Dimension result = DimensionUtils.constructDimension("nonExistentDimension", parts);
        assertEquals("nonExistentDimension", result.getName());
        assertEquals("UNKNOWN", result.getValue());
    }

    @Test
    public void testGetDefaultDimensionFromSystemEnv() throws NoSuchFieldException, IllegalAccessException {
        Class<?> classOfMap = System.getenv().getClass();
        Field field = classOfMap.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writeableEnvironmentVariables = (Map<String, String>)field.get(System.getenv());
        writeableEnvironmentVariables.put("TEST_VAR", "12345");
        Dimension result = DimensionUtils.constructDimension("TEST_VAR", new String[]{});
        assertEquals("TEST_VAR", result.getName());
        assertEquals("12345", result.getValue());
    }

    @Test
    public void testConstructJobIdDimension() throws NoSuchFieldException, IllegalAccessException {
        Class<?> classOfMap = System.getenv().getClass();
        Field field = classOfMap.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writeableEnvironmentVariables = (Map<String, String>)field.get(System.getenv());
        writeableEnvironmentVariables.put("SERVERLESS_EMR_JOB_ID", "12345");
        Dimension result = DimensionUtils.constructDimension("jobId", new String[]{});
        assertEquals("jobId", result.getName());
        assertEquals("12345", result.getValue());
    }
}

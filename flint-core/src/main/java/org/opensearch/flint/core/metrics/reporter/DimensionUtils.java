/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics.reporter;

import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.cloudwatch.model.Dimension;

/**
 * Utility class for creating and managing CloudWatch dimensions for metrics reporting in Flint.
 * It facilitates the construction of dimensions based on different system properties and environment
 * variables, supporting the dynamic tagging of metrics with relevant information like job ID,
 * application ID, and more.
 */
public class DimensionUtils {
    private static final String DIMENSION_JOB_ID = "jobId";
    private static final String DIMENSION_APPLICATION_ID = "applicationId";
    private static final String DIMENSION_APPLICATION_NAME = "applicationName";
    private static final String DIMENSION_DOMAIN_ID = "domainId";
    private static final String DIMENSION_INSTANCE_ROLE = "instanceRole";
    private static final String UNKNOWN = "UNKNOWN";

    // Maps dimension names to functions that generate Dimension objects based on specific logic or environment variables
    private static final Map<String, Function<String[], Dimension>> dimensionBuilders = Map.of(
            DIMENSION_INSTANCE_ROLE, DimensionUtils::getInstanceRoleDimension,
            DIMENSION_JOB_ID, ignored -> getEnvironmentVariableDimension("SERVERLESS_EMR_JOB_ID", DIMENSION_JOB_ID),
            DIMENSION_APPLICATION_ID, ignored -> getEnvironmentVariableDimension("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", DIMENSION_APPLICATION_ID),
            DIMENSION_APPLICATION_NAME, ignored -> getEnvironmentVariableDimension("SERVERLESS_EMR_APPLICATION_NAME", DIMENSION_APPLICATION_NAME),
            DIMENSION_DOMAIN_ID, ignored -> getEnvironmentVariableDimension("FLINT_CLUSTER_NAME", DIMENSION_DOMAIN_ID)
    );

    /**
     * Constructs a CloudWatch Dimension object based on the provided dimension name. If a specific
     * builder exists for the dimension name, it is used; otherwise, a default dimension is constructed.
     *
     * @param dimensionName The name of the dimension to construct.
     * @param parts Additional information that might be required by specific dimension builders.
     * @return A CloudWatch Dimension object.
     */
    public static Dimension constructDimension(String dimensionName, String[] parts) {
        return dimensionBuilders.getOrDefault(dimensionName, ignored -> getDefaultDimension(dimensionName))
                .apply(parts);
    }

    /**
     * Generates a Dimension object representing the instance role (either executor or driver) based on the
     * metric name parts provided.
     *
     * @param parts An array where the second element indicates the role by being numeric (executor) or not (driver).
     * @return A Dimension object with the instance role.
     */
    private static Dimension getInstanceRoleDimension(String[] parts) {
        String value = StringUtils.isNumeric(parts[1]) ? "executor" : parts[1];
        return new Dimension().withName(DIMENSION_INSTANCE_ROLE).withValue(value);
    }

    /**
     * Constructs a Dimension object using a system environment variable. If the environment variable is not found,
     * it uses a predefined "UNKNOWN" value.
     *
     * @param envVarName The name of the environment variable to use for the dimension's value.
     * @param dimensionName The name of the dimension.
     * @return A Dimension object populated with the appropriate name and value.
     */
    private static Dimension getEnvironmentVariableDimension(String envVarName, String dimensionName) {
        String value = System.getenv().getOrDefault(envVarName, UNKNOWN);
        return new Dimension().withName(dimensionName).withValue(value);
    }

    /**
     * Provides a generic mechanism to construct a Dimension object with an environment variable value
     * or a default value if the environment variable is not set.
     *
     * @param dimensionName The name of the dimension for which to retrieve the value.
     * @return A Dimension object populated with the dimension name and its corresponding value.
     */
    private static Dimension getDefaultDimension(String dimensionName) {
        return getEnvironmentVariableDimension(dimensionName, dimensionName);
    }
}
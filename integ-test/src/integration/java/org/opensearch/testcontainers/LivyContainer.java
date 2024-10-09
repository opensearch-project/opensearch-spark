/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * LivyContainer Implementation.
 */
public class LivyContainer extends GenericContainer<LivyContainer> {

    private static final int LIVY_PORT = 8998;
    private static final String DEFAULT_IMAGE_NAME = "cambridgesemantics/livy:latest"; // Update with the correct image

    public LivyContainer() {
        this(DockerImageName.parse(DEFAULT_IMAGE_NAME));
    }

    public LivyContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    /**
     * Default Livy Container.
     */
    public LivyContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);

        withExposedPorts(LIVY_PORT);
        withEnv("LIVY_LOG_DIR", "/var/log/livy");
        withEnv("SPARK_HOME", "/usr/lib/spark");
        withEnv("LIVY_HOME", "/usr/local/livy");
        withEnv("LIVY_SERVER_HOST", "0.0.0.0");
        withEnv("LIVY_SERVER_PORT", String.valueOf(LIVY_PORT));
    }

    public int port() {
        return getMappedPort(LIVY_PORT);
    }
}

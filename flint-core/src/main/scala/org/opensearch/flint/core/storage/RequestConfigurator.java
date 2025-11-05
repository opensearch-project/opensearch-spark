/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.apache.http.client.config.RequestConfig;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.flint.core.FlintOptions;

/**
 * allows override default socket timeout in RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS
 * allows override of default connection timeout in RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS
 */
public class RequestConfigurator implements RestClientBuilder.RequestConfigCallback {

    private final FlintOptions options;

    public RequestConfigurator(FlintOptions options) {
        this.options = options;
    }

    @Override
    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
        // Set the connection and socket timeouts in milliseconds
        return requestConfigBuilder.setConnectTimeout(options.getConnectionTimeoutMillis())
                .setSocketTimeout(options.getSocketTimeoutMillis());
    }
}


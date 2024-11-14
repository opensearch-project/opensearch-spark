/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;

// TODO: LoaderCache

import java.util.concurrent.TimeUnit;

public class GeoIpCache {

    public Cache<String, CidrGeoMap> cache;

    private static GeoIpCache cacheInstance = null;

    private GeoIpCache() {
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(3, TimeUnit.DAYS)
                .build();
    }

    public static synchronized GeoIpCache getInstance() {

        if (cacheInstance == null) {
            cacheInstance = new GeoIpCache();
        }

        return cacheInstance;
    }
}

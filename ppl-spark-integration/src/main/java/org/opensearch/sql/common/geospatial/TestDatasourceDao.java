/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import org.apache.commons.lang3.tuple.Pair;

import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.stream.Stream;

public class TestDatasourceDao implements DatasourceDao {

    private String datasource;

    TestDatasourceDao(String datasource) {
        this.datasource = datasource;
    }

    @Override
    public Stream<Pair<BitSet, GeoIpData>> getGeoIps() {

        // Mock GeoIpData entries
        GeoIpData geoIp1 = new GeoIpData("US", "United States", "North America", "US-CA", "California", "Los Angeles", "America/Los_Angeles", "34.0522", "-118.2437");
        GeoIpData geoIp2 = new GeoIpData("CA", "Canada", "North America", "CA-ON", "Ontario", "Toronto", "America/Toronto", "43.65107", "-79.347015");

        BitSet bitSet1 = null;
        BitSet bitSet2 = null;

        try {
            bitSet1 = DatasourceDao.createCidrBitSet("192.168.0.0/24"); // Example CIDR mask
            bitSet2 = DatasourceDao.createCidrBitSet("10.0.0.0/8");     // Example CIDR mask
        } catch (UnknownHostException e) {

        }

        return Stream.of(
                Pair.of(bitSet1, geoIp1),
                Pair.of(bitSet2, geoIp2)
        );
    }

    @Override
    public void close() {

    }
}

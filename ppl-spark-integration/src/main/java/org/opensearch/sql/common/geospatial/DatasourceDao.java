/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.stream.Stream;

public interface DatasourceDao extends AutoCloseable {
    Stream<Pair<BitSet, GeoIpData>> getGeoIps();

    static BitSet createCidrBitSet(String cidr) throws UnknownHostException {
        String[] parts = cidr.split("/");

        InetAddress inetAddress = InetAddress.getByName(parts[0]);
        byte[] bytes = inetAddress.getAddress();
        BitSet cidrKey = BitSet.valueOf(bytes);
        int prefixLength = Integer.parseInt(parts[1]);

        return cidrKey.get(0, prefixLength - 1);
    }

}

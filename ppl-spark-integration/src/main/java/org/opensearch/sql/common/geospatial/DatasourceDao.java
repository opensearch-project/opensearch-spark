/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.stream.Stream;

public interface DatasourceDao extends AutoCloseable {
    Stream<Pair<BitSet, GeoIpData>> getGeoIps();

    static BitSet cidrToBitSet(String cidr) {
        String[] parts = cidr.split("/");

        IPAddressString cidrString = new IPAddressString(cidr);
        IPAddress cidrIpAddress = cidrString.getAddress();

        if (cidrIpAddress == null || cidrIpAddress.getNetworkPrefixLength() == null) {
            throw new IllegalArgumentException("Invalid CIDR notation: " + cidr);
        }

        int prefixLength = cidrIpAddress.getNetworkPrefixLength();
        byte[] cidrBytes = cidrIpAddress.getBytes();
        BitSet cidrKey = BitSet.valueOf(cidrBytes);

        return cidrKey.get(0, prefixLength - 1);
    }

}

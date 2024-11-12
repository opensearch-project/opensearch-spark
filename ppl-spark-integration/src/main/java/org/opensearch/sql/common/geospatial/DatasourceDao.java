/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.geospatial;

import javafx.util.Pair;

import java.util.BitSet;
import java.util.stream.Stream;

public interface DatasourceDao extends AutoCloseable {
    Stream<Pair<BitSet, GeoIpData>> getGeoIps();

    static BitSet createCidrBitSet(String cidr) {
        String[] parts = cidr.split("/");
        String ip = parts[0];
        int prefixLength = Integer.parseInt(parts[1]);
        BitSet bitSet = new BitSet(32);

        String[] octets = ip.split("\\.");
        int bitIndex = 0;
        for (String octet : octets) {
            int octetInt = Integer.parseInt(octet);
            for (int i = 7; i >= 0; i--) {
                bitSet.set(bitIndex++, (octetInt & (1 << i)) != 0);
            }
        }

        for (int i = prefixLength; i < 32; i++) {
            bitSet.clear(i);
        }
        return bitSet;
    }

}

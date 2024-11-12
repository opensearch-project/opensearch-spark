package org.opensearch.sql.common.geospatial;

import javafx.util.Pair;

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
        BitSet bitSet1 = createCidrBitSet("192.168.0.0/24"); // Example CIDR mask
        BitSet bitSet2 = createCidrBitSet("10.0.0.0/8");     // Example CIDR mask

        return Stream.of(
                new Pair<BitSet, GeoIpData>(bitSet1, geoIp1),
                new Pair<BitSet, GeoIpData>(bitSet2, geoIp2)
        );
    }

    @Override
    public void close() {

    }
    private BitSet createCidrBitSet(String cidr) {
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

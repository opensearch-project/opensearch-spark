package org.opensearch.sql.common.geospatial;

import javafx.util.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CidrGeoMap {

    private HashMap<BitSet, GeoIpData> cidrGeoMap;

    public CidrGeoMap(DatasourceDao datasourceDao) {
        Stream<Pair<BitSet, GeoIpData>> dataStream = datasourceDao.getGeoIps();
        cidrGeoMap = dataStream.collect(
                Collectors.toMap(
                        pair -> pair.getKey(),
                        pair -> pair.getValue(),
                        (existing, replacement) -> existing,
                        HashMap::new
                )
        );
    }

    public GeoIpData lookup(String ipAddress) throws UnknownHostException {
        BitSet binaryIP = ipStringToBitSet(ipAddress);

        GeoIpData res = null;

        while (binaryIP.length() > 0 && res == null) {
            res = cidrGeoMap.get(binaryIP);
            binaryIP = binaryIP.get(0, binaryIP.length() - 2);
        }

        // TODO: throw error if no results found

        return res;
    }

    private void put(String cidr, GeoIpData data) throws UnknownHostException {
        String[] parts = cidr.split("/");
        BitSet cidrKey = ipStringToBitSet(parts[0]);
        int prefixLength = Integer.parseInt(parts[1]);
        cidrKey = cidrKey.get(0, prefixLength - 1);

        cidrGeoMap.put(cidrKey, data);
    }

    private BitSet ipStringToBitSet(String ipAddress) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ipAddress);
        byte[] bytes = inetAddress.getAddress();
        return BitSet.valueOf(bytes);
    }
}


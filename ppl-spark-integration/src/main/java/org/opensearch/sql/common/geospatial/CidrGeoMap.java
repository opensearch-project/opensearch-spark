package org.opensearch.sql.common.geospatial;

import java.util.HashMap;
import java.util.Map;

public class CidrGeoMap extends HashMap<String, GeoIpData> {

    private String toBinaryString(String ip) {
        StringBuilder binary = new StringBuilder();
        for (String octet : ip.split("\\.")) {
            binary.append(String.format("%8s", Integer.toBinaryString(Integer.parseInt(octet)))
                    .replace(' ', '0'));
        }
        return binary.toString();
    }

    @Override
    public GeoIpData put(String cidr, GeoIpData data) {
        String[] parts = cidr.split("/");
        String cidrKey = toBinaryString(parts[0]);
        int prefixLength = Integer.parseInt(parts[1]);
        cidrKey = cidrKey.substring(0, prefixLength - 1);

        super.put(cidrKey, data);

        return data;
    }

    @Override
    public GeoIpData get(Object ipAddress) {
        String binaryIP = toBinaryString(ipAddress.toString());

        GeoIpData res = null;

        while (binaryIP.length() > 0 && res == null) {
            res = super.get(binaryIP);
            binaryIP = binaryIP.substring(0, binaryIP.length() - 2);
        }
    }
}


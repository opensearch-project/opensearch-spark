/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.google.common.cache.Cache;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import org.apache.spark.sql.Row;
import org.opensearch.sql.common.geospatial.CidrGeoMap;
import org.opensearch.sql.common.geospatial.DatasourceDao;
import org.opensearch.sql.common.geospatial.DatasourceDaoFactory;
import org.opensearch.sql.common.geospatial.GeoIpCache;
import org.opensearch.sql.common.geospatial.GeoIpData;
import scala.Function2;
import scala.Function3;
import scala.Serializable;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.net.UnknownHostException;
import java.util.List;

public interface SerializableUdf {

    Function2<String,String,Boolean> cidrFunction = new SerializableAbstractFunction2<>() {

        IPAddressStringParameters valOptions = new IPAddressStringParameters.Builder()
                .allowEmpty(false)
                .setEmptyAsLoopback(false)
                .allow_inet_aton(false)
                .allowSingleSegment(false)
                .toParams();

        @Override
        public Boolean apply(String ipAddress, String cidrBlock) {

            IPAddressString parsedIpAddress = new IPAddressString(ipAddress, valOptions);

            try {
                parsedIpAddress.validate();
            } catch (AddressStringException e) {
                throw new RuntimeException("The given ipAddress '"+ipAddress+"' is invalid. It must be a valid IPv4 or IPv6 address. Error details: "+e.getMessage());
            }

            IPAddressString parsedCidrBlock = new IPAddressString(cidrBlock, valOptions);

            try {
                parsedCidrBlock.validate();
            } catch (AddressStringException e) {
                throw new RuntimeException("The given cidrBlock '"+cidrBlock+"' is invalid. It must be a valid CIDR or netmask. Error details: "+e.getMessage());
            }

            if(parsedIpAddress.isIPv4() && parsedCidrBlock.isIPv6() || parsedIpAddress.isIPv6() && parsedCidrBlock.isIPv4()) {
                throw new RuntimeException("The given ipAddress '"+ipAddress+"' and cidrBlock '"+cidrBlock+"' are not compatible. Both must be either IPv4 or IPv6.");
            }

            return parsedCidrBlock.contains(parsedIpAddress);
        }
    };

    Function3<String, String, String, Row> geoIpFunction = new SerializableAbstractFunction3<>() {

        @Override
        public Row apply(String datasource, String ipAddress, String properties) {

            Cache<String, CidrGeoMap> geoIpCache = GeoIpCache.getInstance().cache;
            CidrGeoMap cidrGeoMap = geoIpCache.getIfPresent(datasource);

            if (cidrGeoMap == null) {
                DatasourceDao datasourceDao = DatasourceDaoFactory.GetDatasourceDao(datasource);
                cidrGeoMap = new CidrGeoMap(datasourceDao);
                geoIpCache.put(datasource, cidrGeoMap);
            }

            try {
                return cidrGeoMap.lookup(ipAddress).getRow();
            } catch (UnknownHostException e) {
                throw new RuntimeException("The given ipAddress '" + ipAddress + "' is invalid. It must be a valid IPv4 or IPv6 address. Error details: " + e.getMessage());
            }
        }
    };

    abstract class SerializableAbstractFunction2<T1,T2,R> extends AbstractFunction2<T1,T2,R>
            implements Serializable {
    }

    abstract class SerializableAbstractFunction3<T1,T2,T3,R> extends AbstractFunction3<T1,T2,T3,R>
            implements  Serializable {

    }
}

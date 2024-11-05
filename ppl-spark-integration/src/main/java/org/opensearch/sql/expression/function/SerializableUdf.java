/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import scala.Function2;
import scala.Function3;
import scala.Serializable;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

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

    Function3<String, String, List<String>, Object> geoIpFunction = new SerializableAbstractFunction3<>() {

        @Override
        public Object apply(String datasource, String ipAddress, List<String> properties) {
            Object results = "geoip data";

            //TODO:
            //  1. Check if in-memory cache object for datasource exists.
            //  2. If cache object does not exists create new in-memory cache object from csv retrieved from datasource manifest.
            //  3. Search cached object for GeoIP data.
            //  4. Return GeoIP data.

            return results;
        }
    };

    abstract class SerializableAbstractFunction2<T1,T2,R> extends AbstractFunction2<T1,T2,R>
            implements Serializable {
    }

    abstract class SerializableAbstractFunction3<T1,T2,T3,R> extends AbstractFunction3<T1,T2,T3,R>
            implements  Serializable {

    }
}

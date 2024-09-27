/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import scala.Function2;
import scala.Serializable;
import scala.runtime.AbstractFunction2;


public class SerializableUdf {

    //this class should not have any fields, only static methods and static classes

    private SerializableUdf() {

    }

    public static Function2<String,String,Boolean> cidrFunction = new SerializableAbstractFunction2<>() {

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

            return parsedCidrBlock.contains(parsedIpAddress);
        }
    };

    static abstract public class SerializableAbstractFunction2<T1,T2,R> extends AbstractFunction2<T1,T2,R>
            implements Serializable {
    }
}

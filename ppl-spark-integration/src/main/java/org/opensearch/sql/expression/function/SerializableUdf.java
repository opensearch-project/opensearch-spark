/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;

import scala.Function1;
import scala.Function2;
import scala.Serializable;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

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
        }};

    Function1<String,Boolean> isIpv4 = new SerializableAbstractFunction1<>() {

        IPAddressStringParameters valOptions = new IPAddressStringParameters.Builder()
                .allowEmpty(false)
                .setEmptyAsLoopback(false)
                .allow_inet_aton(false)
                .allowSingleSegment(false)
                .toParams();

        @Override
        public Boolean apply(String ipAddress) {

            IPAddressString parsedIpAddress = new IPAddressString(ipAddress, valOptions);

            try {
                parsedIpAddress.validate();
            } catch (AddressStringException e) {
                throw new RuntimeException("The given ipAddress '"+ipAddress+"' is invalid. It must be a valid IPv4 or IPv6 address. Error details: "+e.getMessage());
            }

            return parsedIpAddress.isIPv4();
        }};

    Function1<String,Boolean> ipToInt = new SerializableAbstractFunction1<>() {

        IPAddressStringParameters valOptions = new IPAddressStringParameters.Builder()
                .allowEmpty(false)
                .setEmptyAsLoopback(false)
                .allow_inet_aton(false)
                .allowSingleSegment(false)
                .toParams();

        @Override
        public Boolean apply(String ipAddress) {

            IPAddressString parsedIpAddress = new IPAddressString(ipAddress, valOptions);

            try {
                parsedIpAddress.validate();
            } catch (AddressStringException e) {
                throw new RuntimeException("The given ipAddress '"+ipAddress+"' is invalid. It must be a valid IPv4 or IPv6 address. Error details: "+e.getMessage());
            }

            return parsedIpAddress.isIPv4();
        }};

    abstract class SerializableAbstractFunction1<T1,R> extends AbstractFunction1<T1,R>
            implements Serializable {
    }

    abstract class SerializableAbstractFunction2<T1,T2,R> extends AbstractFunction2<T1,T2,R>
            implements Serializable {
    }
}

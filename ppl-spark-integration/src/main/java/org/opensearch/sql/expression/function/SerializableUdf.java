/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.expression.function.JsonUtils.objectMapper;
import static org.opensearch.sql.expression.function.JsonUtils.removeNestedKey;
import static org.opensearch.sql.expression.function.JsonUtils.updateNestedJson;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Option;
import scala.Serializable;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

public interface SerializableUdf {

    abstract class SerializableAbstractFunction1<T1, R> extends AbstractFunction1<T1, R>
            implements Serializable {}

    abstract class SerializableAbstractFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R>
            implements Serializable {}

    abstract class SerializableAbstractFunction3<T1, T2, T3, R> extends AbstractFunction3<T1, T2, T3, R>
            implements Serializable {}

    /**
     * Remove specified keys from a JSON string.
     *
     * @param jsonStr      The input JSON string.
     * @param keysToRemove The list of keys to remove.
     * @return A new JSON string without the specified keys.
     */
    Function2<String, WrappedArray<String>, String> jsonDeleteFunction = new SerializableAbstractFunction2<>() {
        @Override
        public String apply(String jsonStr, WrappedArray<String> keysToRemove) {
            if (jsonStr == null) {
                return null;
            }
            try {
                Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, Map.class);
                removeKeys(jsonMap, keysToRemove);
                return objectMapper.writeValueAsString(jsonMap);
            } catch (Exception e) {
                return null;
            }
        }

        private void removeKeys(Map<String, Object> map, WrappedArray<String> keysToRemove) {
            Collection<String> keys = JavaConverters.asJavaCollection(keysToRemove);
            for (String key : keys) {
                String[] keyParts = key.split("\\.");
                removeNestedKey(map, keyParts, 0);
            }
        }
    };

    /**
     * Update the specified key-value pairs in a JSON string. If the key doesn't exist, the key-value is added.
     *
     * @param jsonStr    The input JSON string.
     * @param elements   A list of key-values pairs where the key is the key/path and value item is the updated value.
     * @return A new JSON string with updated values.
     */
    Function2<String, WrappedArray<String>, String> jsonSetFunction = new SerializableAbstractFunction2<>() {
        @Override
        public String apply(String jsonStr, WrappedArray<String> elements) {
            return updateNestedJson(jsonStr, JavaConverters.mutableSeqAsJavaList(elements), (obj, key, value) -> obj.put(key, value));
        }
    };

    /**
     * Append values to JSON arrays based on specified path-values.
     *
     * @param jsonStr    The input JSON string.
     * @param elements   A list of path-values where the first item is the path and subsequent items are values to append.
     * @return The updated JSON string.
     */
    Function2<String, WrappedArray<String>, String> jsonAppendFunction = new SerializableAbstractFunction2<>() {
        @Override
        public String apply(String jsonStr, WrappedArray<String> elements) {
            return updateNestedJson(jsonStr, JavaConverters.mutableSeqAsJavaList(elements), JsonUtils::appendObjectValue);
        }
    };

    /**
     * Extend values to JSON arrays based on specified path-values - flattening any given array/list first
     *
     * @param jsonStr    The input JSON string.
     * @param elements   A list of path-values where the first item is the path and subsequent items are values to append.
     * @return The updated JSON string.
     */
    Function2<String, WrappedArray<String>, String> jsonExtendFunction = new SerializableAbstractFunction2<>() {
        @Override
        public String apply(String jsonStr, WrappedArray<String> elements) {
            return updateNestedJson(jsonStr, JavaConverters.mutableSeqAsJavaList(elements), JsonUtils::extendObjectValue);
        }
    };

    Function2<String, String, Boolean> cidrFunction = new SerializableAbstractFunction2<>() {

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
                throw new RuntimeException("The given ipAddress '" + ipAddress + "' is invalid. It must be a valid IPv4 or IPv6 address. Error details: " + e.getMessage());
            }

            IPAddressString parsedCidrBlock = new IPAddressString(cidrBlock, valOptions);

            try {
                parsedCidrBlock.validate();
            } catch (AddressStringException e) {
                throw new RuntimeException("The given cidrBlock '" + cidrBlock + "' is invalid. It must be a valid CIDR or netmask. Error details: " + e.getMessage());
            }

            if (parsedIpAddress.isIPv4() && parsedCidrBlock.isIPv6() || parsedIpAddress.isIPv6() && parsedCidrBlock.isIPv4()) {
                throw new RuntimeException("The given ipAddress '" + ipAddress + "' and cidrBlock '" + cidrBlock + "' are not compatible. Both must be either IPv4 or IPv6.");
            }

            return parsedCidrBlock.contains(parsedIpAddress);
        }
    };

    class geoIpUtils {
        /**
         * Checks if provided ip string is ipv4 or ipv6.
         *
         * @param ipAddress     To input ip string.
         * @return true if ipAddress is ipv4, false if ipaddress is ipv6, AddressString Exception if invalid ip.
         */
        public static Function1<String,Boolean> isIpv4 = new SerializableAbstractFunction1<>() {

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
            }
        };

        /**
         * Convert ipAddress string to interger representation
         *
         * @param ipAddress    The input ip string.
         * @return converted BigInteger from ipAddress string.
         */
        public static Function1<String,BigInteger> ipToInt = new SerializableAbstractFunction1<>() {
            @Override
            public BigInteger apply(String ipAddress) {
                try {
                    InetAddress inetAddress = InetAddress.getByName(ipAddress);
                    byte[] addressBytes = inetAddress.getAddress();
                    return new BigInteger(1, addressBytes);
                } catch (UnknownHostException e) {
                    System.err.println("Invalid IP address: " + e.getMessage());
                }
                return null;
            }
        };
    }

    /**
     * Returns the {@link Instant} corresponding to the given relative string, current timestamp, and current time zone ID.
     * Throws {@link RuntimeException} if the relative string is not supported.
     */
    Function3<String, Object, String, Instant> relativeTimestampFunction = new SerializableAbstractFunction3<String, Object, String, Instant>() {

        @Override
        public Instant apply(String relativeString, Object currentTimestamp, String zoneIdString) {

            /// If `spark.sql.datetime.java8API.enabled` is set to `true`, [org.apache.spark.sql.types.TimestampType]
            /// is converted to [Instant] by Catalyst; otherwise, [Timestamp] is used instead.
            Instant currentInstant =
                    currentTimestamp instanceof Timestamp
                            ? ((Timestamp) currentTimestamp).toInstant()
                            : (Instant) currentTimestamp;

            ZoneId zoneId = ZoneId.of(zoneIdString);
            ZonedDateTime currentDateTime = ZonedDateTime.ofInstant(currentInstant, zoneId);
            ZonedDateTime relativeDateTime = TimeUtils.getRelativeZonedDateTime(relativeString, currentDateTime);

            return relativeDateTime.toInstant();
        }
    };

    /**
     * Get the function reference according to its name
     *
     * @param funcName PPL BuiltinFunctionName to retrieve.
     * @return relevant ScalaUDF for given function name.
     */
    static ScalaUDF visit(BuiltinFunctionName funcName, List<Expression> expressions) {
        switch (funcName) {
            case JSON_DELETE:
                return new ScalaUDF(jsonDeleteFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_delete"),
                        false,
                        true);
            case JSON_SET:
                return new ScalaUDF(jsonSetFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_set"),
                        false,
                        true);
            case JSON_APPEND:
                return new ScalaUDF(jsonAppendFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_append"),
                        false,
                        true);
            case JSON_EXTEND:
                return new ScalaUDF(jsonExtendFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_extend"),
                        false,
                        true);
            case CIDR:
                return new ScalaUDF(cidrFunction,
                    DataTypes.BooleanType,
                    seq(expressions),
                    seq(),
                    Option.empty(),
                    Option.apply("cidr"),
                    false,
                    true);
            case IS_IPV4:
                return new ScalaUDF(geoIpUtils.isIpv4,
                        DataTypes.BooleanType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("is_ipv4"),
                        false,
                        true);
            case IP_TO_INT:
                return new ScalaUDF(geoIpUtils.ipToInt,
                        DataTypes.createDecimalType(38, 0),
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("ip_to_int"),
                        false,
                        true);
            case RELATIVE_TIMESTAMP:
                return new ScalaUDF(relativeTimestampFunction,
                        DataTypes.TimestampType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("relative_timestamp"),
                        false,
                        true);
            default:
                return null;
        }
    }
}

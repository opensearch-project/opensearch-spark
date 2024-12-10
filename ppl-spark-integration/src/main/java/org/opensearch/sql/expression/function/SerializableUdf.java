/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ScalaUDF;
import org.apache.spark.sql.types.DataTypes;
import scala.Function2;
import scala.Option;
import scala.Serializable;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;
import scala.runtime.AbstractFunction2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;


public interface SerializableUdf {

    ObjectMapper objectMapper = new ObjectMapper();

    abstract class SerializableAbstractFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R>
            implements Serializable {
    }

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

        private void removeNestedKey(Object currentObj, String[] keyParts, int depth) {
            if (currentObj == null || depth >= keyParts.length) {
                return;
            }

            if (currentObj instanceof Map) {
                Map<String, Object> currentMap = (Map<String, Object>) currentObj;
                String currentKey = keyParts[depth];

                if (depth == keyParts.length - 1) {
                    // If it's the last key, remove it from the map
                    currentMap.remove(currentKey);
                } else {
                    // If not the last key, continue traversing
                    if (currentMap.containsKey(currentKey)) {
                        Object nextObj = currentMap.get(currentKey);

                        if (nextObj instanceof List) {
                            // If the value is a list, process each item in the list
                            List<Object> list = (List<Object>) nextObj;
                            for (int i = 0; i < list.size(); i++) {
                                removeNestedKey(list.get(i), keyParts, depth + 1);
                            }
                        } else {
                            // Continue traversing if it's a map
                            removeNestedKey(nextObj, keyParts, depth + 1);
                        }
                    }
                }
            }
        }
    };

    Function2<String, WrappedArray<String>, String> jsonAppendFunction = new SerializableAbstractFunction2<>() {
        /**
         * Append values to JSON arrays based on specified path-values.
         *
         * @param jsonStr    The input JSON string.
         * @param elements   A list of path-values where the first item is the path and subsequent items are values to append.
         * @return The updated JSON string.
         */
        public String apply(String jsonStr, WrappedArray<String> elements) {
            if (jsonStr == null) {
                return null;
            }
            try {
                List<String> pathValues = JavaConverters.mutableSeqAsJavaList(elements);
                if (pathValues.isEmpty()) {
                    return jsonStr;
                }

                String path = pathValues.get(0);
                String[] pathParts = path.split("\\.");
                List<String> values = pathValues.subList(1, pathValues.size());

                // Parse the JSON string into a Map
                Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, Map.class);

                // Append each value at the specified path
                for (String value : values) {
                    Object parsedValue = parseValue(value); // Parse the value
                    appendNestedValue(jsonMap, pathParts, 0, parsedValue);
                }

                // Convert the updated map back to JSON
                return objectMapper.writeValueAsString(jsonMap);
            } catch (Exception e) {
                return null;
            }
        }

        private Object parseValue(String value) {
            // Try parsing the value as JSON, fallback to primitive if parsing fails
            try {
                return objectMapper.readValue(value, Object.class);
            } catch (Exception e) {
                // Primitive value, return as is
                return value;
            }
        }

        private void appendNestedValue(Object currentObj, String[] pathParts, int depth, Object valueToAppend) {
            if (currentObj == null || depth >= pathParts.length) {
                return;
            }

            if (currentObj instanceof Map) {
                Map<String, Object> currentMap = (Map<String, Object>) currentObj;
                String currentKey = pathParts[depth];

                if (depth == pathParts.length - 1) {
                    // If it's the last key, append to the array
                    currentMap.computeIfAbsent(currentKey, k -> new ArrayList<>()); // Create list if not present
                    Object existingValue = currentMap.get(currentKey);

                    if (existingValue instanceof List) {
                        List<Object> existingList = (List<Object>) existingValue;
                        existingList.add(valueToAppend);
                    }
                } else {
                    // Continue traversing
                    currentMap.computeIfAbsent(currentKey, k -> new LinkedHashMap<>()); // Create map if not present
                    appendNestedValue(currentMap.get(currentKey), pathParts, depth + 1, valueToAppend);
                }
            } else if (currentObj instanceof List) {
                // If the current object is a list, process each map in the list
                List<Object> list = (List<Object>) currentObj;
                for (Object item : list) {
                    if (item instanceof Map) {
                        appendNestedValue(item, pathParts, depth, valueToAppend);
                    }
                }
            }
        }
    };

    /**
     * Extend JSON arrays with new values based on specified path-value pairs.
     *
     * @param jsonStr         The input JSON string.
     * @param pathValuePairs  A list of path-value pairs to extend.
     * @return The updated JSON string.
     */
    Function2<String, List<Map.Entry<String, List<String>>>, String> jsonExtendFunction = new SerializableAbstractFunction2<>() {

        @Override
        public String apply(String jsonStr, List<Map.Entry<String, List<String>>> pathValuePairs) {
            if (jsonStr == null) {
                return null;
            }
            try {
                Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, Map.class);

                for (Map.Entry<String, List<String>> pathValuePair : pathValuePairs) {
                    String path = pathValuePair.getKey();
                    List<String> values = pathValuePair.getValue();

                    if (jsonMap.containsKey(path) && jsonMap.get(path) instanceof List) {
                        List<Object> existingList = (List<Object>) jsonMap.get(path);
                        existingList.addAll(values);
                    } else {
                        jsonMap.put(path, values);
                    }
                }

                return objectMapper.writeValueAsString(jsonMap);
            } catch (Exception e) {
                return null;
            }
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

    /**
     * get the function reference according to its name
     *
     * @param funcName
     * @return
     */
    static ScalaUDF visit(String funcName, List<Expression> expressions) {
        switch (funcName) {
            case "cidr":
                return new ScalaUDF(cidrFunction,
                        DataTypes.BooleanType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("cidr"),
                        false,
                        true);
            case "json_delete":
                return new ScalaUDF(jsonDeleteFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_delete"),
                        false,
                        true);
            case "json_extend":
                return new ScalaUDF(jsonExtendFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_extend"),
                        false,
                        true);
            case "json_append":
                return new ScalaUDF(jsonAppendFunction,
                        DataTypes.StringType,
                        seq(expressions),
                        seq(),
                        Option.empty(),
                        Option.apply("json_append"),
                        false,
                        true);
            default:
                return null;
        }
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.expression.function;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.opensearch.sql.expression.function.SerializableUdf.jsonAppendFunction;
import static org.opensearch.sql.expression.function.SerializableUdf.jsonDeleteFunction;
import static org.opensearch.sql.expression.function.SerializableUdf.jsonExtendFunction;

public class SerializableJsonUdfTest {

    @Test
    public void testJsonDeleteFunctionRemoveSingleKey() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
        String expectedJson = "{\"key1\":\"value1\",\"key3\":\"value3\"}";
        String result = jsonDeleteFunction.apply(jsonStr, singletonList("key2"));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveNestedKey() {
        // Correctly escape double quotes within the JSON string
        String jsonStr = "{\"key1\":\"value1\",\"key2\":{ \"key3\":\"value3\",\"key4\":\"value4\" }}";
        String expectedJson = "{\"key1\":\"value1\",\"key2\":{\"key4\":\"value4\"}}";
        String result = jsonDeleteFunction.apply(jsonStr, singletonList("key2.key3"));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveSingleArrayedKey() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\",\"keyArray\":[\"value1\",\"value2\"]}";
        String expectedJson = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String result = jsonDeleteFunction.apply(jsonStr, singletonList("keyArray"));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveMultipleKeys() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
        String expectedJson = "{\"key3\":\"value3\"}";
        String result = jsonDeleteFunction.apply(jsonStr, Arrays.asList("key1", "key2"));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveMultipleSomeAreNestedKeys() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":{ \"key3\":\"value3\",\"key4\":\"value4\" }}";
        String expectedJson = "{\"key2\":{\"key3\":\"value3\"}}";
        String result = jsonDeleteFunction.apply(jsonStr, Arrays.asList("key1", "key2.key4"));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionNoKeysRemoved() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String result = jsonDeleteFunction.apply(jsonStr, Collections.emptyList());
        assertEquals(jsonStr, result);
    }

    @Test
    public void testJsonDeleteFunctionNullJson() {
        String result = jsonDeleteFunction.apply(null, Collections.singletonList("key1"));
        assertNull(result);
    }

    @Test
    public void testJsonDeleteFunctionInvalidJson() {
        String invalidJson = "invalid_json";
        String result = jsonDeleteFunction.apply(invalidJson, Collections.singletonList("key1"));
        assertNull(result);
    }

    @Test
    public void testJsonAppendFunctionAppendToExistingArray() {
        String jsonStr = "{\"arrayKey\":[\"value1\",\"value2\"]}";
        String expectedJson = "{\"arrayKey\":[\"value1\",\"value2\",\"value3\"]}";
        Map.Entry<String, String> pair = Map.entry("arrayKey", "value3");
        String result = jsonAppendFunction.apply(jsonStr, Collections.singletonList(pair));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionAddNewArray() {
        String jsonStr = "{\"key1\":\"value1\"}";
        String expectedJson = "{\"key1\":\"value1\",\"newArray\":[\"newValue\"]}";
        Map.Entry<String, String> pair = Map.entry("newArray", "newValue");
        String result = jsonAppendFunction.apply(jsonStr, Collections.singletonList(pair));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionIgnoreNonArrayKey() {
        String jsonStr = "{\"key1\":\"value1\"}";
        String expectedJson = jsonStr;
        Map.Entry<String, String> pair = Map.entry("key1", "newValue");
        String result = jsonAppendFunction.apply(jsonStr, Collections.singletonList(pair));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionMultipleAppends() {
        String jsonStr = "{\"arrayKey\":[\"value1\"]}";
        String expectedJson = "{\"arrayKey\":[\"value1\",\"value2\",\"value3\"],\"newKey\":[\"newValue\"]}";
        List<Map.Entry<String, String>> pairs = Arrays.asList(
                Map.entry("arrayKey", "value2"),
                Map.entry("arrayKey", "value3"),
                Map.entry("newKey", "newValue")
        );
        String result = jsonAppendFunction.apply(jsonStr, pairs);
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionNullJson() {
        String result = jsonAppendFunction.apply(null, Collections.singletonList(Map.entry("key", "value")));
        assertNull(result);
    }

    @Test
    public void testJsonAppendFunctionInvalidJson() {
        String invalidJson = "invalid_json";
        String result = jsonAppendFunction.apply(invalidJson, Collections.singletonList(Map.entry("key", "value")));
        assertNull(result);
    }

    @Test
    public void testJsonExtendFunctionWithExistingPath() {
        String jsonStr = "{\"path1\": [\"value1\", \"value2\"]}";
        List<Map.Entry<String, List<String>>> pathValuePairs = new ArrayList<>();
        pathValuePairs.add(Map.entry("path1", asList("value3", "value4")));

        String result = jsonExtendFunction.apply(jsonStr, pathValuePairs);
        String expectedJson = "{\"path1\":[\"value1\",\"value2\",\"value3\",\"value4\"]}";

        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonExtendFunctionWithNewPath() {
        String jsonStr = "{\"path1\": [\"value1\"]}";
        List<Map.Entry<String, List<String>>> pathValuePairs = new ArrayList<>();
        pathValuePairs.add(Map.entry("path2", asList("newValue1", "newValue2")));

        String result = jsonExtendFunction.apply(jsonStr, pathValuePairs);
        String expectedJson = "{\"path1\":[\"value1\"],\"path2\":[\"newValue1\",\"newValue2\"]}";

        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonExtendFunctionWithNullInput() {
        String result = jsonExtendFunction.apply(null, Collections.emptyList());
        assertNull(result);
    }

    @Test
    public void testJsonExtendFunctionWithInvalidJson() {
        String result = jsonExtendFunction.apply("invalid json", Collections.emptyList());
        assertNull(result);
    }

    @Test
    public void testJsonExtendFunctionWithNonArrayPath() {
        String jsonStr = "{\"path1\":\"value1\"}";
        List<Map.Entry<String, List<String>>> pathValuePairs = new ArrayList<>();
        pathValuePairs.add(Map.entry("path1", asList("value2")));

        String result = jsonExtendFunction.apply(jsonStr, pathValuePairs);
        String expectedJson = "{\"path1\":[\"value2\"]}";

        assertEquals(expectedJson, result);
    }
}

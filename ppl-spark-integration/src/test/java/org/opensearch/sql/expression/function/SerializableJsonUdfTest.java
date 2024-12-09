/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.expression.function;

import org.junit.Test;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        String result = jsonDeleteFunction.apply(jsonStr,  WrappedArray.make(new String[]{"key2"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveNestedKey() {
        // Correctly escape double quotes within the JSON string
        String jsonStr = "{\"key1\":\"value1\",\"key2\":{ \"key3\":\"value3\",\"key4\":\"value4\" }}";
        String expectedJson = "{\"key1\":\"value1\",\"key2\":{\"key4\":\"value4\"}}";
        String result = jsonDeleteFunction.apply(jsonStr, WrappedArray.make(new String[]{"key2.key3"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveSingleArrayedKey() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\",\"keyArray\":[\"value1\",\"value2\"]}";
        String expectedJson = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String result = jsonDeleteFunction.apply(jsonStr, WrappedArray.make(new String[]{"keyArray"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveMultipleKeys() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}";
        String expectedJson = "{\"key3\":\"value3\"}";
        String result = jsonDeleteFunction.apply(jsonStr, WrappedArray.make(new String[]{"key1", "key2"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveMultipleSomeAreNestedKeys() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":{ \"key3\":\"value3\",\"key4\":\"value4\" }}";
        String expectedJson = "{\"key2\":{\"key3\":\"value3\"}}";
        String result = jsonDeleteFunction.apply(jsonStr,  WrappedArray.make(new String[]{"key1", "key2.key4"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionRemoveMultipleKeysNestedArrayKeys() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":[{ \"a\":\"valueA\",\"key3\":\"value3\"}, {\"a\":\"valueA\",\"key4\":\"value4\"}]}";
        String expectedJson = "{\"key2\":[{\"key3\":\"value3\"},{\"key4\":\"value4\"}]}";
        String result = jsonDeleteFunction.apply(jsonStr,  WrappedArray.make(new String[]{"key1", "key2.a"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonDeleteFunctionNoKeysRemoved() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String result = jsonDeleteFunction.apply(jsonStr, WrappedArray.make(new String[0]));
        assertEquals(jsonStr, result);
    }

    @Test
    public void testJsonDeleteFunctionNullJson() {
        String result = jsonDeleteFunction.apply(null,  WrappedArray.make(new String[]{"key1"}));
        assertNull(result);
    }

    @Test
    public void testJsonDeleteFunctionInvalidJson() {
        String invalidJson = "invalid_json";
        String result = jsonDeleteFunction.apply(invalidJson,  WrappedArray.make(new String[]{"key1"}));
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

    @Test
    public void testJsonExtendFunctionAddValuesToExistingArray() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":[\"value2\"]}";
        List<Map.Entry<String, List<String>>> pathValuePairs = new ArrayList<>();
        pathValuePairs.add( Map.entry("key2", Arrays.asList("value3", "value4")));

        String expectedJson = "{\"key1\":\"value1\",\"key2\":[\"value2\",\"value3\",\"value4\"]}";
        String result = jsonExtendFunction.apply(jsonStr, pathValuePairs);

        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonExtendFunctionAddNewArray() {
        String jsonStr = "{\"key1\":\"value1\"}";
        List<Map.Entry<String, List<String>>> pathValuePairs = new ArrayList<>();
        pathValuePairs.add( Map.entry("key2", Arrays.asList("value2", "value3")));

        String expectedJson = "{\"key1\":\"value1\",\"key2\":[\"value2\",\"value3\"]}";
        String result = jsonExtendFunction.apply(jsonStr, pathValuePairs);

        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonExtendFunctionHandleEmptyValues() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":[\"value2\"]}";
        List<Map.Entry<String, List<String>>> pathValuePairs = new ArrayList<>();
        pathValuePairs.add( Map.entry("key2", Collections.emptyList()));

        String expectedJson = "{\"key1\":\"value1\",\"key2\":[\"value2\"]}";
        String result = jsonExtendFunction.apply(jsonStr, pathValuePairs);

        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonExtendFunctionHandleNullInput() {
        String result = jsonExtendFunction.apply(null, Collections.singletonList( Map.entry("key2", List.of("value2"))));
        assertEquals(null, result);
    }
}

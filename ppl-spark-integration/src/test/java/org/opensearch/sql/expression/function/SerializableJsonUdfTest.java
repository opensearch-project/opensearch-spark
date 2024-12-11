/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.expression.function;

import org.junit.Test;
import scala.collection.mutable.WrappedArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.opensearch.sql.expression.function.SerializableUdf.jsonAppendFunction;
import static org.opensearch.sql.expression.function.SerializableUdf.jsonDeleteFunction;

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
        String result = jsonAppendFunction.apply(jsonStr, WrappedArray.make(new String[]{"arrayKey", "value3"}));
        assertEquals(expectedJson, result);
    }
    
    @Test
    public void testJsonAppendFunctionAppendObjectToExistingArray() {
        String jsonStr = "{\"key1\":\"value1\",\"key2\":[{\"a\":\"valueA\",\"key3\":\"value3\"}]}";
        String expectedJson = "{\"key1\":\"value1\",\"key2\":[{\"a\":\"valueA\",\"key3\":\"value3\"},{\"a\":\"valueA\",\"key4\":\"value4\"}]}";
        String result = jsonAppendFunction.apply(jsonStr, WrappedArray.make(new String[]{"key2", "{\"a\":\"valueA\",\"key4\":\"value4\"}"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionAddNewArray() {
        String jsonStr = "{\"key1\":\"value1\",\"newArray\":[]}";
        String expectedJson = "{\"key1\":\"value1\",\"newArray\":[\"newValue\"]}";
        String result = jsonAppendFunction.apply(jsonStr,  WrappedArray.make(new String[]{"newArray", "newValue"}));
        assertEquals(expectedJson, result);
    }
    @Test
    public void testJsonAppendFunctionNoSuchKey() {
        String jsonStr = "{\"key1\":\"value1\"}";
        String expectedJson = "{\"key1\":\"value1\",\"newKey\":[\"newValue\"]}";
        String result = jsonAppendFunction.apply(jsonStr,  WrappedArray.make(new String[]{"newKey", "newValue"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionIgnoreNonArrayKey() {
        String jsonStr = "{\"key1\":\"value1\"}";
        String expectedJson = jsonStr;
        String result = jsonAppendFunction.apply(jsonStr,  WrappedArray.make(new String[]{"key1", "newValue"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionWithNestedArrayKeys() {
        String jsonStr = "{\"key2\":[{\"a\":[\"Value1\"],\"key3\":\"Value3\"},{\"a\":[\"Value1\"],\"key4\":\"Value4\"}]}";
        String expectedJson = "{\"key2\":[{\"a\":[\"Value1\",\"Value2\"],\"key3\":\"Value3\"},{\"a\":[\"Value1\",\"Value2\"],\"key4\":\"Value4\"}]}";
        String result = jsonAppendFunction.apply(jsonStr,  WrappedArray.make(new String[]{"key2.a","Value2"}));
        assertEquals(expectedJson, result);
    }
    
    @Test
    public void testJsonAppendFunctionWithObjectKey() {
        String jsonStr = "{\"key2\":[{\"a\":[\"Value1\"],\"key3\":\"Value3\"},{\"a\":[\"Value1\"],\"key4\":\"Value4\"}]}";
        String expectedJson = "{\"key2\":[{\"a\":[\"Value1\"],\"key3\":\"Value3\"},{\"a\":[\"Value1\"],\"key4\":\"Value4\"},\"Value2\"]}";
        String result = jsonAppendFunction.apply(jsonStr,  WrappedArray.make(new String[]{"key2","Value2"}));
        assertEquals(expectedJson, result);
    }

    @Test
    public void testJsonAppendFunctionNullJson() {
        String result = jsonAppendFunction.apply(null, WrappedArray.make(new String[]{"key1", "newValue"}));
        assertNull(result);
    }

    @Test
    public void testJsonAppendFunctionInvalidJson() {
        String invalidJson = "invalid_json";
        String result = jsonAppendFunction.apply(invalidJson, WrappedArray.make(new String[]{"key1", "newValue"}));
        assertNull(result);
    }
}

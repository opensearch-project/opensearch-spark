/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;


import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.mutable.Seq;

import java.util.List;

import static scala.collection.JavaConverters.asScalaBufferConverter;

/**
 * translate the PPL ast expressions data-types into catalyst data-types 
 */
public interface DataTypeTransformer {
    static <T> Seq<T> seq(T element) {
        return seq(List.of(element));
    }
    static <T> Seq<T> seq(List<T> list) {
        return asScalaBufferConverter(list).asScala().seq();
    }
    
    static DataType translate(org.opensearch.sql.ast.expression.DataType source) {
        switch (source.getCoreType()) {
            case TIME:
                return DateType$.MODULE$;
            case INTEGER:
                return IntegerType$.MODULE$;
            case BYTE:
                return ByteType$.MODULE$;
            default:
                return StringType$.MODULE$;
        }
    }
    
    static Object translate(Object value, org.opensearch.sql.ast.expression.DataType source) {
        switch (source.getCoreType()) {
            case STRING:
                /* The regex ^'(.*)'$ matches strings that start and end with a single quote. The content inside the quotes is captured using the (.*).
                 * The $1 in the replaceAll method refers to the first captured group, which is the content inside the quotes. 
                 * If the string matches the pattern, the content inside the quotes is returned; otherwise, the original string is returned.
                 */
                return UTF8String.fromString(value.toString().replaceAll("^'(.*)'$", "$1"));
            default:
                return value;
        }
    }
}
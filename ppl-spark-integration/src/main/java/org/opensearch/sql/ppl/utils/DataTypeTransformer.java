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
import org.opensearch.sql.ast.expression.SpanUnit;
import scala.collection.mutable.Seq;

import java.util.List;

import static org.opensearch.sql.ast.expression.SpanUnit.DAY;
import static org.opensearch.sql.ast.expression.SpanUnit.HOUR;
import static org.opensearch.sql.ast.expression.SpanUnit.MILLISECOND;
import static org.opensearch.sql.ast.expression.SpanUnit.MINUTE;
import static org.opensearch.sql.ast.expression.SpanUnit.MONTH;
import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.QUARTER;
import static org.opensearch.sql.ast.expression.SpanUnit.SECOND;
import static org.opensearch.sql.ast.expression.SpanUnit.WEEK;
import static org.opensearch.sql.ast.expression.SpanUnit.YEAR;
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
    
    static String translate(SpanUnit unit) {
        switch (unit) {
            case UNKNOWN:
            case NONE:
                return NONE.name();
            case MILLISECOND:
            case MS:
                return MILLISECOND.name();
            case SECOND:
            case S:
                return SECOND.name();
            case MINUTE:
            case m:
                return MINUTE.name();
            case HOUR:
            case H:
                return HOUR.name();
            case DAY:
            case D:
                return DAY.name();
            case WEEK:
            case W:
                return WEEK.name();
            case MONTH:
            case M:
                return MONTH.name();
            case QUARTER:
            case Q:
                return QUARTER.name();
            case YEAR:
            case Y:
                return YEAR.name();
        }
        return "";
    }
}
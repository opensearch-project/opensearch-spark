/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.TimeWindow;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.StringType$;
import org.opensearch.sql.ast.expression.SpanUnit;

import static java.lang.String.format;
import static org.opensearch.sql.ast.expression.DataType.STRING;
import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.UNKNOWN;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;

public interface WindowSpecTransformer {

    /**
     * create a static window buckets based on the given value
     *
     * @param fieldExpression
     * @param valueExpression
     * @param unit
     * @return
     */
    static Expression window(Expression fieldExpression, Expression valueExpression, SpanUnit unit) {
        // In case the unit is time unit - use TimeWindowSpec if possible
        if (isTimeBased(unit)) {
            return new TimeWindow(fieldExpression,timeLiteral(valueExpression, unit));
        }
        // if the unit is not time base - create a math expression to bucket the span partitions
        return new Multiply(new Floor(new Divide(fieldExpression, valueExpression)), valueExpression);
    }

    static boolean isTimeBased(SpanUnit unit) {
        return !(unit == NONE || unit == UNKNOWN);
    }
    
    
    static org.apache.spark.sql.catalyst.expressions.Literal timeLiteral( Expression valueExpression, SpanUnit unit) {
        String format = format("%s %s", valueExpression.toString(), translate(unit));
        return new org.apache.spark.sql.catalyst.expressions.Literal(
                translate(format, STRING), translate(STRING));
    }
}

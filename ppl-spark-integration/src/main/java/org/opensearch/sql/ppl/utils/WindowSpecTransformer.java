/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.opensearch.sql.ast.expression.SpanUnit;

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
        // todo check can WindowSpec provide the same functionality as below
        // todo for time unit - use TimeWindowSpec if possible
        return new Multiply(new Floor(new Divide(fieldExpression, valueExpression)), valueExpression);
    } 
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.CurrentRow$;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.RowFrame$;
import org.apache.spark.sql.catalyst.expressions.RowNumber;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.SpecifiedWindowFrame;
import org.apache.spark.sql.catalyst.expressions.TimeWindow;
import org.apache.spark.sql.catalyst.expressions.UnboundedFollowing$;
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding$;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition;
import org.opensearch.sql.ast.expression.SpanUnit;
import scala.Option;
import scala.collection.Seq;

import java.util.ArrayList;

import static java.lang.String.format;
import static org.opensearch.sql.ast.expression.DataType.STRING;
import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.UNKNOWN;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;

public interface WindowSpecTransformer {

    String ROW_NUMBER_COLUMN_NAME = "_row_number_";

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

    static NamedExpression buildRowNumber(Seq<Expression> partitionSpec, Seq<SortOrder> orderSpec) {
        WindowExpression rowNumber = new WindowExpression(
            new RowNumber(),
            new WindowSpecDefinition(
                partitionSpec,
                orderSpec,
                new SpecifiedWindowFrame(RowFrame$.MODULE$, UnboundedPreceding$.MODULE$, CurrentRow$.MODULE$)));
        return org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(
            rowNumber,
            ROW_NUMBER_COLUMN_NAME,
            NamedExpression.newExprId(),
            seq(new ArrayList<String>()),
            Option.empty(),
            seq(new ArrayList<String>()));
    }

    static NamedExpression buildAggregateWindowFunction(Expression aggregator, Seq<Expression> partitionSpec, Seq<SortOrder> orderSpec) {
        Alias aggregatorAlias = (Alias) aggregator;
        WindowExpression aggWindowExpression = new WindowExpression(
            aggregatorAlias.child(),
            new WindowSpecDefinition(
                partitionSpec,
                orderSpec,
                new SpecifiedWindowFrame(RowFrame$.MODULE$, UnboundedPreceding$.MODULE$, UnboundedFollowing$.MODULE$)));
        return org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(
            aggWindowExpression,
            aggregatorAlias.name(),
            NamedExpression.newExprId(),
            seq(new ArrayList<String>()),
            Option.empty(),
            seq(new ArrayList<String>()));
    }
}

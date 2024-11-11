/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import java.util.List;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

/**
 * aggregator expression builder building a catalyst aggregation function from PPL's aggregation logical step
 *
 * @return
 */
public interface AggregatorTransformer {
    
    static Expression aggregator(org.opensearch.sql.ast.expression.AggregateFunction aggregateFunction, Expression arg) {
        if (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + aggregateFunction.getFuncName());

        boolean distinct = aggregateFunction.getDistinct();
        // Additional aggregation function operators will be added here
        BuiltinFunctionName functionName = BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).get();
        switch (functionName) {
            case MAX:
                return new UnresolvedFunction(seq("MAX"), seq(arg), distinct, empty(),false);
            case MIN:
                return new UnresolvedFunction(seq("MIN"), seq(arg), distinct, empty(),false);
            case MEAN:
                return new UnresolvedFunction(seq("MEAN"), seq(arg), distinct, empty(),false);
            case AVG:
                return new UnresolvedFunction(seq("AVG"), seq(arg), distinct, empty(),false);
            case COUNT:
                return new UnresolvedFunction(seq("COUNT"), seq(arg), distinct, empty(),false);
            case SUM:
                return new UnresolvedFunction(seq("SUM"), seq(arg), distinct, empty(),false);
            case STDDEV:
                return new UnresolvedFunction(seq("STDDEV"), seq(arg), distinct, empty(),false);
            case STDDEV_POP:
                return new UnresolvedFunction(seq("STDDEV_POP"), seq(arg), distinct, empty(),false);
            case STDDEV_SAMP:
                return new UnresolvedFunction(seq("STDDEV_SAMP"), seq(arg), distinct, empty(),false);
            case PERCENTILE:
                return new UnresolvedFunction(seq("PERCENTILE"), seq(arg, new Literal(getPercentDoubleValue(aggregateFunction), DataTypes.DoubleType)), distinct, empty(),false);
            case PERCENTILE_APPROX:
                return new UnresolvedFunction(seq("PERCENTILE_APPROX"), seq(arg, new Literal(getPercentDoubleValue(aggregateFunction), DataTypes.DoubleType)), distinct, empty(),false);
            case APPROX_COUNT_DISTINCT:
                return new UnresolvedFunction(seq("APPROX_COUNT_DISTINCT"), seq(arg), distinct, empty(),false);
        }
        throw new IllegalStateException("Not Supported value: " + aggregateFunction.getFuncName());
    }

    private static double getPercentDoubleValue(AggregateFunction aggregateFunction) {

        List<UnresolvedExpression> arguments = aggregateFunction.getArgList();

        if (arguments == null || arguments.size() != 1) {
            throw new IllegalStateException("Missing 'percent' argument");
        }

        org.opensearch.sql.ast.expression.Literal percentIntValue = ((Argument) aggregateFunction.getArgList().get(0)).getValue();

        if (percentIntValue.getType() != DataType.INTEGER) {
            throw new IllegalStateException("Unsupported datatype for 'percent': " + percentIntValue.getType() + " (expected: INTEGER)");
        }

        double percentDoubleValue = ((Integer) percentIntValue.getValue()) / 100d;

        if (percentDoubleValue < 0 || percentDoubleValue > 1) {
            throw new IllegalStateException("Unsupported value 'percent': " + percentIntValue.getValue() + " (expected: >= 0 <= 100))");
        }
        return percentDoubleValue;
    }
}

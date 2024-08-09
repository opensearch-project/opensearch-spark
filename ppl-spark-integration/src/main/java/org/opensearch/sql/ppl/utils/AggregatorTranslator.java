/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

/**
 * aggregator expression builder building a catalyst aggregation function from PPL's aggregation logical step
 *
 * @return
 */
public interface AggregatorTranslator {

    static Expression aggregator(org.opensearch.sql.ast.expression.AggregateFunction aggregateFunction, Expression arg) {
        if (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + aggregateFunction.getFuncName());

        // Additional aggregation function operators will be added here
        switch (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).get()) {
            case MAX:
                return new UnresolvedFunction(seq("MAX"), seq(arg),false, empty(),false);
            case MIN:
                return new UnresolvedFunction(seq("MIN"), seq(arg),false, empty(),false);
            case AVG:
                return new UnresolvedFunction(seq("AVG"), seq(arg),false, empty(),false);
            case COUNT:
                return new UnresolvedFunction(seq("COUNT"), seq(arg),false, empty(),false);
            case SUM:
                return new UnresolvedFunction(seq("SUM"), seq(arg),false, empty(),false);
            case PERCENTILE:
                org.opensearch.sql.ast.expression.Literal percentIntValue = ((Argument) aggregateFunction.getArgList().get(0)).getValue();

                if(percentIntValue.getType() != DataType.INTEGER) {
                    throw new IllegalStateException("Unsupported datatype for 'percent': " + percentIntValue.getType()+" (expected: INTEGER)");
                }

                double percentDoubleValue = ((Integer)percentIntValue.getValue())/100d;

                if(percentDoubleValue < 0 || percentDoubleValue > 1) {
                    throw new IllegalStateException("Unsupported value 'percent': " + percentIntValue.getValue()+" (expected: >= 0 <= 100))");
                }

                return new UnresolvedFunction(seq("PERCENTILE"), seq(arg, new Literal(percentDoubleValue, DataTypes.DoubleType)), aggregateFunction.getDistinct(), empty(),false);
        }
        throw new IllegalStateException("Not Supported value: " + aggregateFunction.getFuncName());
    }
}

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;

/**
 * aggregator expression builder building a catalyst aggregation function from PPL's aggregation logical step
 *
 * @return
 */
public interface AggregatorTranslator {

    static AggregateFunction aggregator(org.opensearch.sql.ast.expression.AggregateFunction aggregateFunction, CatalystPlanContext context) {
        if (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + aggregateFunction.getFuncName());

        // Additional aggregation function operators will be added here
        switch (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).get()) {
            case MAX:
                break;
            case MIN:
                break;
            case AVG:
                return new Average(context.getNamedParseExpressions().pop());
            case COUNT:
                break;
            case SUM:
                break;
            case STDDEV_POP:
                break;
            case STDDEV_SAMP:
                break;
            case TAKE:
                break;
            case VARPOP:
                break;
            case VARSAMP:
                break;
        }
        throw new IllegalStateException("Not Supported value: " + aggregateFunction.getFuncName());
    }
}

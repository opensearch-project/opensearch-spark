/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;

import static java.util.List.of;
import static scala.Option.empty;
import static scala.collection.JavaConverters.asScalaBuffer;

/**
 * aggregator expression builder building a catalyst aggregation function from PPL's aggregation logical step
 *
 * @return
 */
public interface AggregatorTranslator {

    static Expression aggregator(org.opensearch.sql.ast.expression.AggregateFunction aggregateFunction, CatalystPlanContext context) {
        if (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).isEmpty())
            throw new IllegalStateException("Unexpected value: " + aggregateFunction.getFuncName());

        // Additional aggregation function operators will be added here
        switch (BuiltinFunctionName.ofAggregation(aggregateFunction.getFuncName()).get()) {
            case MAX:
                return new UnresolvedFunction(asScalaBuffer(of("MAX")).toSeq(),
                        asScalaBuffer(of(context.getNamedParseExpressions().pop())).toSeq(),false, empty(),false);
            case MIN:
                return new UnresolvedFunction(asScalaBuffer(of("MIN")).toSeq(),
                        asScalaBuffer(of(context.getNamedParseExpressions().pop())).toSeq(),false, empty(),false);
            case AVG:
                return new UnresolvedFunction(asScalaBuffer(of("AVG")).toSeq(),
                        asScalaBuffer(of(context.getNamedParseExpressions().pop())).toSeq(),false, empty(),false);
            case COUNT:
                return new UnresolvedFunction(asScalaBuffer(of("COUNT")).toSeq(),
                        asScalaBuffer(of(context.getNamedParseExpressions().pop())).toSeq(),false, empty(),false);
            case SUM:
                return new UnresolvedFunction(asScalaBuffer(of("SUM")).toSeq(),
                        asScalaBuffer(of(context.getNamedParseExpressions().pop())).toSeq(),false, empty(),false);
        }
        throw new IllegalStateException("Not Supported value: " + aggregateFunction.getFuncName());
    }
}

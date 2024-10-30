/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.*;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;
import org.opensearch.sql.ppl.CatalystQueryPlanVisitor;
import scala.Option;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface TrendlineStrategy {

    static List<NamedExpression> visitTrendlineComputations(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, List<Trendline.TrendlineComputation> computations, CatalystPlanContext context) {
        return computations.stream()
                .map(computation -> visitTrendlineComputation(expressionAnalyzer, computation, context))
                .collect(Collectors.toList());
    }

    static NamedExpression visitTrendlineComputation(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, Trendline.TrendlineComputation node, CatalystPlanContext context) {
        //window lower boundary
        expressionAnalyzer.visitLiteral(new Literal(Math.negateExact(node.getNumberOfDataPoints() - 1), DataType.INTEGER), context);
        Expression windowLowerBoundary = context.popNamedParseExpressions().get();

        //window definition
        WindowSpecDefinition windowDefinition = new WindowSpecDefinition(
                seq(),
                seq(),
                new SpecifiedWindowFrame(RowFrame$.MODULE$, windowLowerBoundary, CurrentRow$.MODULE$));

        if (node.getComputationType() == Trendline.TrendlineType.SMA) {
            //calculate avg value of the data field
            expressionAnalyzer.visitAggregateFunction(new AggregateFunction(BuiltinFunctionName.AVG.name(), node.getDataField()), context);
            Expression avgFunction = context.popNamedParseExpressions().get();

            //sma window
            WindowExpression sma = new WindowExpression(
                    avgFunction,
                    windowDefinition);

            CaseWhen smaOrNull = trendlineOrNullWhenThereAreTooFewDataPoints(expressionAnalyzer, sma, node, context);

            return org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(smaOrNull,
                            node.getAlias(),
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>()));
        } else {
            throw new IllegalArgumentException(node.getComputationType()+" is not supported");
        }
    }

    private static CaseWhen trendlineOrNullWhenThereAreTooFewDataPoints(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, WindowExpression trendlineWindow, Trendline.TrendlineComputation node, CatalystPlanContext context) {
        //required number of data points
        expressionAnalyzer.visitLiteral(new Literal(node.getNumberOfDataPoints(), DataType.INTEGER), context);
        Expression requiredNumberOfDataPoints = context.popNamedParseExpressions().get();

        //count data points function
        expressionAnalyzer.visitAggregateFunction(new AggregateFunction(BuiltinFunctionName.COUNT.name(), new Literal(1, DataType.INTEGER)), context);
        Expression countDataPointsFunction = context.popNamedParseExpressions().get();
        //count data points window
        WindowExpression countDataPointsWindow = new WindowExpression(
                countDataPointsFunction,
                trendlineWindow.windowSpec());

        expressionAnalyzer.visitLiteral(new Literal(null, DataType.NULL), context);
        Expression nullLiteral = context.popNamedParseExpressions().get();
        Tuple2<Expression, Expression> nullWhenNumberOfDataPointsLessThenRequired = new Tuple2<>(
                new LessThan(countDataPointsWindow, requiredNumberOfDataPoints),
                nullLiteral
        );
        return new CaseWhen(seq(nullWhenNumberOfDataPointsLessThenRequired), Option.apply(trendlineWindow));
    }
}

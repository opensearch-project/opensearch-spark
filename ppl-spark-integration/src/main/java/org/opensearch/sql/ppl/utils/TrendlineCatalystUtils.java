/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.*;
import org.opensearch.sql.ast.expression.*;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystExpressionVisitor;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface TrendlineCatalystUtils {


    static List<NamedExpression> visitTrendlineComputations(CatalystExpressionVisitor expressionVisitor, List<Trendline.TrendlineComputation> computations, Optional<Field> sortField, CatalystPlanContext context) {
        return computations.stream()
                .map(computation -> visitTrendlineComputation(expressionVisitor, computation, sortField, context))
                .collect(Collectors.toList());
    }


    static NamedExpression visitTrendlineComputation(CatalystExpressionVisitor expressionVisitor, Trendline.TrendlineComputation node, Optional<Field> sortField, CatalystPlanContext context) {

        //window lower boundary
        expressionVisitor.visitLiteral(new Literal(Math.negateExact(node.getNumberOfDataPoints() - 1), DataType.INTEGER), context);
        Expression windowLowerBoundary = context.popNamedParseExpressions().get();

        //window definition
        WindowSpecDefinition windowDefinition = new WindowSpecDefinition(
                seq(),
                seq(),
                new SpecifiedWindowFrame(RowFrame$.MODULE$, windowLowerBoundary, CurrentRow$.MODULE$));

        switch (node.getComputationType()) {
            case SMA:
                //calculate avg value of the data field
                expressionVisitor.visitAggregateFunction(new AggregateFunction(BuiltinFunctionName.AVG.name(), node.getDataField()), context);
                Expression avgFunction = context.popNamedParseExpressions().get();

                //sma window
                WindowExpression sma = new WindowExpression(
                        avgFunction,
                        windowDefinition);

                CaseWhen smaOrNull = trendlineOrNullWhenThereAreTooFewDataPoints(expressionVisitor, sma, node, context);

                return getAlias(node.getAlias(), smaOrNull);
            case WMA:
                if (sortField.isPresent()) {
                    return getWMAComputationExpression(expressionVisitor, node, sortField.get(), context);
                } else {
                    throw new IllegalArgumentException(node.getComputationType()+" requires a sort field for computation");
                }
            default:
                throw new IllegalArgumentException(node.getComputationType()+" is not supported");
        }
    }

    private static CaseWhen trendlineOrNullWhenThereAreTooFewDataPoints(CatalystExpressionVisitor expressionVisitor, WindowExpression trendlineWindow, Trendline.TrendlineComputation node, CatalystPlanContext context) {
        //required number of data points
        expressionVisitor.visitLiteral(new Literal(node.getNumberOfDataPoints(), DataType.INTEGER), context);
        Expression requiredNumberOfDataPoints = context.popNamedParseExpressions().get();

        //count data points function
        expressionVisitor.visitAggregateFunction(new AggregateFunction(BuiltinFunctionName.COUNT.name(), new Literal(1, DataType.INTEGER)), context);
        Expression countDataPointsFunction = context.popNamedParseExpressions().get();
        //count data points window
        WindowExpression countDataPointsWindow = new WindowExpression(
                countDataPointsFunction,
                trendlineWindow.windowSpec());

        expressionVisitor.visitLiteral(new Literal(null, DataType.NULL), context);
        Expression nullLiteral = context.popNamedParseExpressions().get();
        Tuple2<Expression, Expression> nullWhenNumberOfDataPointsLessThenRequired = new Tuple2<>(
                new LessThan(countDataPointsWindow, requiredNumberOfDataPoints),
                nullLiteral
        );
        return new CaseWhen(seq(nullWhenNumberOfDataPointsLessThenRequired), Option.apply(trendlineWindow));
    }

    /**
     * Responsible to produce a Spark Logical Plan with given TrendLine command arguments, below is the sample logical plan
     * with configuration [dataField=salary, sortField=age, dataPoints=3]
     * -- +- 'Project [
     * -- (((('nth_value('salary, 1) windowspecdefinition(Field(field=age, fieldArgs=[]) ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1, currentrow$())) * 1) +
     * -- ('nth_value('salary, 2) windowspecdefinition(Field(field=age, fieldArgs=[]) ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1, currentrow$())) * 2)) +
     * -- ('nth_value('salary, 3) windowspecdefinition(Field(field=age, fieldArgs=[]) ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1, currentrow$())) * 3)) / 6)
     * -- AS WMA#702]
     *
     * @param visitor Visitor instance to process any UnresolvedExpression.
     * @param node Trendline command's arguments.
     * @param sortField Field used for window aggregation.
     * @param context Context instance to retrieved Expression in resolved form.
     * @return a NamedExpression instance which will calculate WMA with provided argument.
     */
    private static NamedExpression getWMAComputationExpression(CatalystExpressionVisitor visitor,
                                                               Trendline.TrendlineComputation node,
                                                               Field sortField,
                                                               CatalystPlanContext context) {

        //window lower boundary
        Expression windowLowerBoundary = getIntExpression(visitor, context,
                Math.negateExact(node.getNumberOfDataPoints() - 1));
        //window definition
        visitor.analyze(sortField, context);
        Expression sortDefinition = context.popNamedParseExpressions().get();
        WindowSpecDefinition windowDefinition = getCommonWindowDefinition(
                sortDefinition,
                SortUtils.isSortedAscending(sortField),
                windowLowerBoundary);
        // Divisor
        Expression divider = getIntExpression(visitor, context,
                (node.getNumberOfDataPoints() * (node.getNumberOfDataPoints()+1) / 2));
        // Aggregation
        Expression WMAExpression = getNthValueAggregations(visitor, node, context, windowDefinition,
                        node.getNumberOfDataPoints())
                        .stream()
                        .reduce(Add::new)
                        .orElse(null);

        return getAlias(node.getAlias(), new Divide(WMAExpression, divider));
    }

    /**
     * Helper method to produce an Alias Expression with provide value and name.
     * @param name The name for the Alias.
     * @param expression The expression which will be evaluated.
     * @return A Alias instance with logical plan representation of `expression AS name`.
     */
    private static NamedExpression getAlias(String name, Expression expression) {
        return org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(expression,
                name,
                NamedExpression.newExprId(),
                seq(Collections.emptyList()),
                Option.empty(),
                seq(Collections.emptyList()));
    }

    /**
     * Helper method to retrieve an Int in expression form for logical plan composition purpose.
     * @param expressionVisitor Visitor instance to process the incoming object.
     * @param context Context instance to retrieve the Expression instance.
     * @param i Target value for the expression.
     * @return An expression object which contain integer value i.
     */
    static Expression getIntExpression(CatalystExpressionVisitor expressionVisitor, CatalystPlanContext context,  int i) {
        expressionVisitor.visitLiteral(new Literal(i,
                DataType.INTEGER), context);
        return context.popNamedParseExpressions().get();
    }


    /**
     * Helper method to retrieve a WindowSpecDefinition with provided sorting condition.
     *  `windowspecdefinition('sortField ascending NULLS FIRST, specifiedwindowframe(RowFrame, windowLowerBoundary, currentrow$())`
     * @param sortField The field being used for the sorting operation.
     * @param ascending The boolean instance for the sorting order.
     * @param windowLowerBoundary The Integer expression instance which specify the even lookbehind / lookahead.
     * @return A WindowSpecDefinition instance which will be used to composite the WMA calculation.
     */
    static WindowSpecDefinition getCommonWindowDefinition(Expression sortField, boolean ascending, Expression windowLowerBoundary) {
        return new WindowSpecDefinition(
                seq(),
                seq(SortUtils.sortOrder(sortField, ascending)),
                new SpecifiedWindowFrame(RowFrame$.MODULE$, windowLowerBoundary, CurrentRow$.MODULE$));
    }

    /**
     * To produce a list of Expression with responsible to return appropriate lookbehind / lookahead value for WMA calculation, sample logical plan listed below.
     * (((('nth_value('salary, 1) windowspecdefinition(Field(field=age, fieldArgs=[]) ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2, currentrow$())) * 1) +
     *
     * @param visitor Visitor instance to resolve Expression.
     * @param node Treeline command instruction.
     * @param context Context instance to retrieve the resolved expression.
     * @param windowDefinition The windowDefinition for the individual datapoint lookbehind / lookahead.
     * @param dataPoints Number of data-points for WMA calculation, this will always equal to number of Expression being generated.
     * @return List instance which contain the SQL statement for WMA individual datapoint's calculations.
     */
    private static List<Expression> getNthValueAggregations(CatalystExpressionVisitor visitor,
                                                            Trendline.TrendlineComputation node,
                                                            CatalystPlanContext context,
                                                            WindowSpecDefinition windowDefinition,
                                                            int dataPoints) {

        List<Expression> expressions = new ArrayList<>();
        for (int i = 1; i <= dataPoints; i++) {
            // Get the offset parameter
            Expression offSetExpression = getIntExpression(visitor, context, i);

            // Composite the nth_value expression.
            Function func =  new Function(BuiltinFunctionName.NTH_VALUE.name(),
                    List.of(node.getDataField(), new Literal(i, DataType.INTEGER)));

            visitor.visitFunction(func, context);
            Expression nthValueExp = context.popNamedParseExpressions().get();

            expressions.add(new Multiply(
                    new WindowExpression(nthValueExp, windowDefinition), offSetExpression));
        }
        return expressions;
    }

}

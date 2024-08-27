/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.DataFrameDropColumns;
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.types.DataTypes;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ppl.CatalystPlanContext;
import org.opensearch.sql.ppl.CatalystQueryPlanVisitor.ExpressionAnalyzer;
import scala.collection.Seq;

import java.util.List;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface DedupeTransformer {

    /**
     * | dedup a, b keepempty=true
     * Union
     * :- Deduplicate ['a, 'b]
     * :  +- Filter (isnotnull('a) AND isnotnull('b))
     * :     +- ...
     * :        +- UnresolvedRelation
     * +- Filter (isnull('a) OR isnull('a))
     *    +- ...
     *       +- UnresolvedRelation
     */
    static LogicalPlan retainOneDuplicateEventAndKeepEmpty(
            Dedupe node,
            Seq<Attribute> dedupeFields,
            ExpressionAnalyzer expressionAnalyzer,
            CatalystPlanContext context) {
        context.apply(p -> {
            Expression isNullExpr = buildIsNullFilterExpression(node, expressionAnalyzer, context);
            LogicalPlan right = new org.apache.spark.sql.catalyst.plans.logical.Filter(isNullExpr, p);

            Expression isNotNullExpr = buildIsNotNullFilterExpression(node, expressionAnalyzer, context);
            LogicalPlan left =
                new Deduplicate(dedupeFields,
                    new org.apache.spark.sql.catalyst.plans.logical.Filter(isNotNullExpr, p));
            return new Union(seq(left, right), false, false);
        });
        return context.getPlan();
    }

    /**
     * | dedup a, b keepempty=false
     * Deduplicate ['a, 'b]
     * +- Filter (isnotnull('a) AND isnotnull('b))
     *    +- ...
     *       +- UnresolvedRelation
     */
    static LogicalPlan retainOneDuplicateEvent(
            Dedupe node,
            Seq<Attribute> dedupeFields,
            ExpressionAnalyzer expressionAnalyzer,
            CatalystPlanContext context) {
        Expression isNotNullExpr = buildIsNotNullFilterExpression(node, expressionAnalyzer, context);
        context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Filter(isNotNullExpr, p));
        // Todo DeduplicateWithinWatermark in streaming dataset?
        return context.apply(p -> new Deduplicate(dedupeFields, p));
    }

    /**
     * | dedup 2 a, b keepempty=true
     * Union
     * :- DataFrameDropColumns('_row_number_)
     * :  +- Filter ('_row_number_ <= 2)
     * :     +- Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC NULLS FIRST, 'b ASC NULLS FIRST]
     * :        +- Filter (isnotnull('a) AND isnotnull('b))
     * :           +- ...
     * :              +- UnresolvedRelation
     * +- Filter (isnull('a) OR isnull('b))
     *    +- ...
     *       +- UnresolvedRelation
     */
    static LogicalPlan retainMultipleDuplicateEventsAndKeepEmpty(
        Dedupe node,
        Integer allowedDuplication,
        ExpressionAnalyzer expressionAnalyzer,
        CatalystPlanContext context) {
        context.apply(p -> {
            // Build isnull Filter for right
            Expression isNullExpr = buildIsNullFilterExpression(node, expressionAnalyzer, context);
            LogicalPlan right = new org.apache.spark.sql.catalyst.plans.logical.Filter(isNullExpr, p);

            // Build isnotnull Filter
            Expression isNotNullExpr = buildIsNotNullFilterExpression(node, expressionAnalyzer, context);
            LogicalPlan isNotNullFilter = new org.apache.spark.sql.catalyst.plans.logical.Filter(isNotNullExpr, p);

            // Build Window
            visitFieldList(node.getFields(), expressionAnalyzer, context);
            Seq<Expression> partitionSpec = context.retainAllNamedParseExpressions(exp -> exp);
            visitFieldList(node.getFields(), expressionAnalyzer, context);
            Seq<SortOrder> orderSpec = context.retainAllNamedParseExpressions(exp -> SortUtils.sortOrder(exp, true));
            NamedExpression rowNumber = WindowSpecTransformer.buildRowNumber(partitionSpec, orderSpec);
            LogicalPlan window = new org.apache.spark.sql.catalyst.plans.logical.Window(
                seq(rowNumber),
                partitionSpec,
                orderSpec,
                isNotNullFilter);

            // Build deduplication Filter ('_row_number_ <= n)
            Expression filterExpr = new LessThanOrEqual(
                rowNumber.toAttribute(),
                new org.apache.spark.sql.catalyst.expressions.Literal(allowedDuplication, DataTypes.IntegerType));
            LogicalPlan deduplicationFilter = new org.apache.spark.sql.catalyst.plans.logical.Filter(filterExpr, window);

            // Build DataFrameDropColumns('_row_number_) for left
            LogicalPlan left = new DataFrameDropColumns(seq(rowNumber.toAttribute()), deduplicationFilter);

            // Build Union
            return new Union(seq(left, right), false, false);
        });
        return context.getPlan();
    }

    /**
     * | dedup 2 a, b keepempty=false
     * DataFrameDropColumns('_row_number_)
     * +- Filter ('_row_number_ <= n)
     *    +- Window [row_number() windowspecdefinition('a, 'b, 'a ASC NULLS FIRST, 'b ASC NULLS FIRST, specifiedwindowoundedpreceding$(), currentrow$())) AS _row_number_], ['a, 'b], ['a ASC NULLS FIRST, 'b ASC NULLS FIRST]
     *       +- Filter (isnotnull('a) AND isnotnull('b))
     *          +- ...
     *             +- UnresolvedRelation
     */
    static LogicalPlan retainMultipleDuplicateEvents(
        Dedupe node,
        Integer allowedDuplication,
        ExpressionAnalyzer expressionAnalyzer,
        CatalystPlanContext context) {
        // Build isnotnull Filter
        Expression isNotNullExpr = buildIsNotNullFilterExpression(node, expressionAnalyzer, context);
        context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Filter(isNotNullExpr, p));

        // Build Window
        visitFieldList(node.getFields(), expressionAnalyzer, context);
        Seq<Expression> partitionSpec = context.retainAllNamedParseExpressions(exp -> exp);
        visitFieldList(node.getFields(), expressionAnalyzer ,context);
        Seq<SortOrder> orderSpec = context.retainAllNamedParseExpressions(exp -> SortUtils.sortOrder(exp, true));
        NamedExpression rowNumber = WindowSpecTransformer.buildRowNumber(partitionSpec, orderSpec);
        context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Window(
            seq(rowNumber),
            partitionSpec,
            orderSpec, p));

        // Build deduplication Filter ('_row_number_ <= n)
        Expression filterExpr = new LessThanOrEqual(
            rowNumber.toAttribute(),
            new org.apache.spark.sql.catalyst.expressions.Literal(allowedDuplication, DataTypes.IntegerType));
        context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Filter(filterExpr, p));

        return context.apply(p -> new DataFrameDropColumns(seq(rowNumber.toAttribute()), p));
    }

    static Expression buildIsNotNullFilterExpression(Dedupe node, ExpressionAnalyzer expressionAnalyzer, CatalystPlanContext context) {
        visitFieldList(node.getFields(), expressionAnalyzer, context);
        Seq<Expression> isNotNullExpressions =
            context.retainAllNamedParseExpressions(
                org.apache.spark.sql.catalyst.expressions.IsNotNull$.MODULE$::apply);

        Expression isNotNullExpr;
        if (isNotNullExpressions.size() == 1) {
            isNotNullExpr = isNotNullExpressions.apply(0);
        } else {
            isNotNullExpr = isNotNullExpressions.reduce(
                (e1, e2) -> new org.apache.spark.sql.catalyst.expressions.And(e1, e2)
            );
        }
        return isNotNullExpr;
    }

    private static Expression buildIsNullFilterExpression(Dedupe node, ExpressionAnalyzer expressionAnalyzer, CatalystPlanContext context) {
        visitFieldList(node.getFields(), expressionAnalyzer, context);
        Seq<Expression> isNullExpressions =
            context.retainAllNamedParseExpressions(
                org.apache.spark.sql.catalyst.expressions.IsNull$.MODULE$::apply);

        Expression isNullExpr;
        if (isNullExpressions.size() == 1) {
            isNullExpr = isNullExpressions.apply(0);
        } else {
            isNullExpr = isNullExpressions.reduce(
                (e1, e2) -> new org.apache.spark.sql.catalyst.expressions.Or(e1, e2)
            );
        }
        return isNullExpr;
    }

    static void visitFieldList(List<Field> fieldList, ExpressionAnalyzer expressionAnalyzer, CatalystPlanContext context) {
        fieldList.forEach(field -> visitExpression(field, expressionAnalyzer, context));
    }

    static Expression visitExpression(UnresolvedExpression expression, ExpressionAnalyzer expressionAnalyzer, CatalystPlanContext context) {
        return expressionAnalyzer.analyze(expression, context);
    }
}

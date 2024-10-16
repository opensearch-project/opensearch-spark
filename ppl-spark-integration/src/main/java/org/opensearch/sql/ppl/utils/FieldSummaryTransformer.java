/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Alias$;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.tree.FieldSummary;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.AVG;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COUNT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.COUNT_DISTINCT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAX;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TYPEOF;
import static org.opensearch.sql.ppl.utils.AggregatorTranslator.aggregationAlias;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface FieldSummaryTransformer {

    String TOP_VALUES = "TopValues";
    String NULLS = "Nulls";
    String FIELD = "Field";

    /**
     * translate the field summary into the following query:
     * -----------------------------------------------------
     *  // for each column create statement:
     *  SELECT
     *      'columnA' AS Field,
     *      COUNT(columnA) AS Count,
     *      COUNT(DISTINCT columnA) AS Distinct,
     *      MIN(columnA) AS Min,
     *      MAX(columnA) AS Max,
     *      AVG(CAST(columnA AS DOUBLE)) AS Avg,
     *      typeof(columnA) AS Type,
     *      (SELECT COLLECT_LIST(STRUCT(columnA, count_status))
     *       FROM (
     *          SELECT columnA, COUNT(*) AS count_status
     *          FROM $testTable
     *          GROUP BY columnA
     *          ORDER BY count_status DESC
     *          LIMIT 5
     *      )) AS top_values,
     *      COUNT(*) - COUNT(columnA) AS Nulls
     *  FROM $testTable
     *  GROUP BY typeof(columnA)                       
     *  
     *  // union all queries
     *  UNION ALL
     *
     *  SELECT
     *      'columnB' AS Field,
     *      COUNT(columnB) AS Count,
     *      COUNT(DISTINCT columnB) AS Distinct,
     *      MIN(columnB) AS Min,
     *      MAX(columnB) AS Max,
     *      AVG(CAST(columnB AS DOUBLE)) AS Avg,
     *      typeof(columnB) AS Type,
     *      (SELECT COLLECT_LIST(STRUCT(columnB, count_columnB))
     *       FROM (
     *          SELECT column-, COUNT(*) AS count_column-
     *          FROM $testTable
     *          GROUP BY columnB
     *          ORDER BY count_column- DESC
     *          LIMIT 5
     *      )) AS top_values,
     *      COUNT(*) - COUNT(columnB) AS Nulls
     *  FROM $testTable
     *  GROUP BY typeof(columnB) 
     */
    static LogicalPlan translate(FieldSummary fieldSummary, CatalystPlanContext context) {
        List<Function<LogicalPlan, LogicalPlan>> aggBranches = fieldSummary.getIncludeFields().stream().map(field -> {
            Literal fieldNameLiteral = Literal.create(field.getField().toString(), StringType);
            UnresolvedAttribute fieldLiteral = new UnresolvedAttribute(seq(field.getField().getParts()));
            context.withProjectedFields(Collections.singletonList(field));

            // Alias for the field name as Field
            Alias fieldNameAlias = Alias$.MODULE$.apply(fieldNameLiteral,
                    FIELD,
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the count(field) as Count
            UnresolvedFunction count = new UnresolvedFunction(seq(COUNT.name()), seq(fieldLiteral), false, empty(), false);
            Alias countAlias = Alias$.MODULE$.apply(count,
                    aggregationAlias(COUNT, field.getField()),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the count(DISTINCT field) as CountDistinct
            UnresolvedFunction countDistinct = new UnresolvedFunction(seq(COUNT.name()), seq(fieldLiteral), true, empty(), false);
            Alias distinctCountAlias = Alias$.MODULE$.apply(countDistinct,
                    aggregationAlias(COUNT_DISTINCT, field.getField()),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the MAX(field) as MAX
            UnresolvedFunction max = new UnresolvedFunction(seq(MAX.name()), seq(fieldLiteral), false, empty(), false);
            Alias maxAlias = Alias$.MODULE$.apply(max,
                    aggregationAlias(MAX, field.getField()),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the MIN(field) as Min
            UnresolvedFunction min = new UnresolvedFunction(seq(MIN.name()), seq(fieldLiteral), false, empty(), false);
            Alias minAlias = Alias$.MODULE$.apply(min,
                    aggregationAlias(MIN, field.getField()),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the AVG(field) as Avg
            UnresolvedFunction avg = new UnresolvedFunction(seq(AVG.name()), seq(fieldLiteral), false, empty(), false);
            Alias avgAlias = Alias$.MODULE$.apply(avg,
                    aggregationAlias(AVG, field.getField()),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            if (fieldSummary.getTopValues() > 0) {
                // Alias COLLECT_LIST(STRUCT(field, COUNT(field))) AS top_values
                CreateNamedStruct structExpr = new CreateNamedStruct(seq(
                        fieldLiteral,
                        count
                ));
                UnresolvedFunction collectList = new UnresolvedFunction(
                        seq("COLLECT_LIST"),
                        seq(structExpr),
                        false,
                        empty(),
                        !fieldSummary.isIgnoreNull()
                );
                context.getNamedParseExpressions().push(
                        Alias$.MODULE$.apply(
                                collectList,
                                TOP_VALUES,
                                NamedExpression.newExprId(),
                                seq(),
                                empty(),
                                seq()
                        ));
            }

            if (!fieldSummary.isIgnoreNull()) {
                // Alias COUNT(*) - COUNT(column2) AS Nulls
                UnresolvedFunction countStar = new UnresolvedFunction(
                        seq(COUNT.name()),
                        seq(Literal.create(1, IntegerType)),
                        false,
                        empty(),
                        false
                );

                context.getNamedParseExpressions().push(
                        Alias$.MODULE$.apply(
                                new Subtract(countStar, count),
                                NULLS,
                                NamedExpression.newExprId(),
                                seq(),
                                empty(),
                                seq()
                        ));
            }

            //Alias for the typeOf(field) as Type
            UnresolvedFunction typeOf = new UnresolvedFunction(seq(TYPEOF.name()), seq(fieldLiteral), false, empty(), false);
            Alias typeOfAlias = Alias$.MODULE$.apply(typeOf,
                    aggregationAlias(TYPEOF, field.getField()),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Aggregation 
            return (Function<LogicalPlan, LogicalPlan>) p -> new Aggregate(seq(typeOfAlias), seq(fieldNameAlias, countAlias, distinctCountAlias, minAlias, maxAlias, avgAlias, typeOfAlias), p);
        }).collect(Collectors.toList());

        LogicalPlan plan = context.applyBranches(aggBranches);
        return plan;
    }
}

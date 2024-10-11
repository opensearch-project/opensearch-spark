/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.tree.FieldSummary;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import java.util.Collections;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface FieldSummaryTransformer {

    String COUNT = "Count";
    String COUNT_DISTINCT = "CountDistinct";
    String MAX = "Max";
    String MIN = "Min";
    String AVG = "Avg";
    String TYPE = "Type";
    String TOP_VALUES = "TopValues";
    String NULLS = "Nulls";

    String FIELD = "Field";

    /**
     * translate the field summary into the following query:
     * -----------------------------------------------------
     *  // for each column create statement:
     *  SELECT
     *      'column-1' AS Field,
     *      COUNT(column-1) AS Count,
     *      COUNT(DISTINCT column-1) AS Distinct,
     *      MIN(column-1) AS Min,
     *      MAX(column-1) AS Max,
     *      AVG(CAST(column-1 AS DOUBLE)) AS Avg,
     *      typeof(column-1) AS Type,
     *      (SELECT COLLECT_LIST(STRUCT(column-1, count_status))
     *       FROM (
     *          SELECT column-1, COUNT(*) AS count_status
     *          FROM $testTable
     *          GROUP BY column-1
     *          ORDER BY count_status DESC
     *          LIMIT 5
     *      )) AS top_values,
     *      COUNT(*) - COUNT(column-1) AS Nulls
     *  FROM $testTable
     *  GROUP BY typeof(column-1)                       
     *  
     *  // union all queries
     *  UNION ALL
     *
     *  SELECT
     *      'column-2' AS Field,
     *      COUNT(column-2) AS Count,
     *      COUNT(DISTINCT column-2) AS Distinct,
     *      MIN(column-2) AS Min,
     *      MAX(column-2) AS Max,
     *      AVG(CAST(column-2 AS DOUBLE)) AS Avg,
     *      typeof(column-2) AS Type,
     *      (SELECT COLLECT_LIST(STRUCT(column-2, count_column-2))
     *       FROM (
     *          SELECT column-, COUNT(*) AS count_column-
     *          FROM $testTable
     *          GROUP BY column-2
     *          ORDER BY count_column- DESC
     *          LIMIT 5
     *      )) AS top_values,
     *      COUNT(*) - COUNT(column-2) AS Nulls
     *  FROM $testTable
     *  GROUP BY typeof(column-2) 
     */
    static LogicalPlan translate(FieldSummary fieldSummary, CatalystPlanContext context) {
        fieldSummary.getIncludeFields().forEach(field -> {
            Literal fieldLiteral = org.apache.spark.sql.catalyst.expressions.Literal.create(field.getField().toString(), StringType);
            context.withProjectedFields(Collections.singletonList(field));
            //Alias for the field name as Field
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(fieldLiteral,
                            FIELD,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));

            //Alias for the count(field) as Count
            UnresolvedFunction count = new UnresolvedFunction(seq("COUNT"), seq(fieldLiteral), false, empty(), false);
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(count,
                            COUNT,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));

            //Alias for the count(DISTINCT field) as CountDistinct
            UnresolvedFunction countDistinct = new UnresolvedFunction(seq("COUNT"), seq(fieldLiteral), true, empty(), false);
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(countDistinct,
                            COUNT_DISTINCT,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));
            
            //Alias for the MAX(field) as MAX
            UnresolvedFunction max = new UnresolvedFunction(seq("MAX"), seq(fieldLiteral), false, empty(), false);
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(max,
                            MAX,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));

            //Alias for the MAX(field) as Min
            UnresolvedFunction min = new UnresolvedFunction(seq("MIN"), seq(fieldLiteral), false, empty(), false);
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(min,
                            MIN,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));

            //Alias for the AVG(field) as Avg
            UnresolvedFunction avg = new UnresolvedFunction(seq("AVG"), seq(fieldLiteral), false, empty(), false);
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(avg,
                            AVG,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));

            //Alias for the typeOf(field) as Type
            UnresolvedFunction type = new UnresolvedFunction(seq("TYPEOF"), seq(fieldLiteral), false, empty(), false);
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(type,
                            TYPE,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));

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
                    false
            );
            context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(
                            collectList,
                            TOP_VALUES,
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())
                    ));
            
            if (fieldSummary.isNulls()) {
                // Alias COUNT(*) - COUNT(column2) AS Nulls
                UnresolvedFunction countStar = new UnresolvedFunction(
                        seq("COUNT"),
                        seq(org.apache.spark.sql.catalyst.expressions.Literal.create(1, IntegerType)),
                        false,
                        empty(),
                        false
                );

                context.getNamedParseExpressions().push(
                        org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(
                                new Subtract(countStar, count),
                                NULLS,
                                NamedExpression.newExprId(),
                                seq(new java.util.ArrayList<String>()),
                                Option.empty(),
                                seq(new java.util.ArrayList<String>())
                        ));
            }
        });

        return context.getPlan();
    }
}

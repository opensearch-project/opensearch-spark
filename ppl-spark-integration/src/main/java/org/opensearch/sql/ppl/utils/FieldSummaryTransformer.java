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
     * SELECT
     *     -- For column1 ---
     *     'column1' AS Field,
     *     COUNT(column1) AS Count,
     *     COUNT(DISTINCT column1) AS Distinct,
     *     MIN(column1) AS Min,
     *     MAX(column1) AS Max,
     *     AVG(CAST(column1 AS DOUBLE)) AS Avg,
     *     typeof(column1) AS Type,
     *     COLLECT_LIST(STRUCT(column1, COUNT(column1))) AS top_values,
     *     COUNT(*) - COUNT(column1) AS Nulls,
     *
     *     -- For column2 ---
     *     'column2' AS Field,
     *     COUNT(column2) AS Count,
     *     COUNT(DISTINCT column2) AS Distinct,
     *     MIN(column2) AS Min,
     *     MAX(column2) AS Max,
     *     AVG(CAST(column2 AS DOUBLE)) AS Avg,
     *     typeof(column2) AS Type,
     *     COLLECT_LIST(STRUCT(column2, COUNT(column2))) AS top_values,
     *     COUNT(*) - COUNT(column2) AS Nulls
     *  FROM ...
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
        });

        return context.getPlan();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Alias$;
import org.apache.spark.sql.catalyst.expressions.Ascending$;
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.Descending$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery;
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery$;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LocalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.types.DataTypes;
import org.opensearch.sql.ast.tree.FieldSummary;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;

import java.util.ArrayList;
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
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MEAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MIN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.STDDEV;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TYPEOF;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.Option.empty;

public interface FieldSummaryTransformer {

    String TOP_VALUES = "TopValues";
    String NULLS = "Nulls";
    String FIELD = "Field";

    /**
     * translate the command into the aggregate statement group by the column name 
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
                    COUNT.name(),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the count(DISTINCT field) as CountDistinct
            UnresolvedFunction countDistinct = new UnresolvedFunction(seq(COUNT.name()), seq(fieldLiteral), true, empty(), false);
            Alias distinctCountAlias = Alias$.MODULE$.apply(countDistinct,
                    COUNT_DISTINCT.name(),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the MAX(field) as MAX
            UnresolvedFunction max = new UnresolvedFunction(seq(MAX.name()), seq(fieldLiteral), false, empty(), false);
            Alias maxAlias = Alias$.MODULE$.apply(max,
                    MAX.name(),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the MIN(field) as Min
            UnresolvedFunction min = new UnresolvedFunction(seq(MIN.name()), seq(fieldLiteral), false, empty(), false);
            Alias minAlias = Alias$.MODULE$.apply(min,
                    MIN.name(),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Alias for the AVG(field) as Avg
            Alias avgAlias = getAggMethodAlias(AVG, fieldSummary, fieldLiteral);

            //Alias for the MEAN(field) as Mean
            Alias meanAlias = getAggMethodAlias(MEAN, fieldSummary, fieldLiteral);

            //Alias for the STDDEV(field) as Stddev
            Alias stddevAlias = getAggMethodAlias(STDDEV, fieldSummary, fieldLiteral);

            // Alias COUNT(*) - COUNT(column2) AS Nulls
            UnresolvedFunction countStar = new UnresolvedFunction(seq(COUNT.name()), seq(Literal.create(1, IntegerType)), false, empty(), false);
            Alias nonNullAlias = Alias$.MODULE$.apply(
                    new Subtract(countStar, count),
                    NULLS,
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());


            //Alias for the typeOf(field) as Type
            UnresolvedFunction typeOf = new UnresolvedFunction(seq(TYPEOF.name()), seq(fieldLiteral), false, empty(), false);
            Alias typeOfAlias = Alias$.MODULE$.apply(typeOf,
                    TYPEOF.name(),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());

            //Aggregation 
            return (Function<LogicalPlan, LogicalPlan>) p ->
                        new Aggregate(seq(typeOfAlias), seq(fieldNameAlias, countAlias, distinctCountAlias, minAlias, maxAlias, avgAlias, meanAlias, stddevAlias, nonNullAlias, typeOfAlias), p);
                    }).collect(Collectors.toList());

        return context.applyBranches(aggBranches);
    }

    /**
     * Alias for aggregate function (if isIncludeNull use COALESCE to replace nulls with zeros)
     */
    private static Alias getAggMethodAlias(BuiltinFunctionName method, FieldSummary fieldSummary, UnresolvedAttribute fieldLiteral) {
        UnresolvedFunction avg = new UnresolvedFunction(seq(method.name()), seq(fieldLiteral), false, empty(), false);
        Alias avgAlias = Alias$.MODULE$.apply(avg,
                method.name(),
                NamedExpression.newExprId(),
                seq(),
                empty(),
                seq());

        if (fieldSummary.isIncludeNull()) {
            UnresolvedFunction coalesceExpr = new UnresolvedFunction(
                    seq("COALESCE"),
                    seq(fieldLiteral, Literal.create(0, DataTypes.IntegerType)),
                    false,
                    empty(),
                    false
            );
            avg = new UnresolvedFunction(seq(method.name()), seq(coalesceExpr), false, empty(), false);
            avgAlias = Alias$.MODULE$.apply(avg,
                    method.name(),
                    NamedExpression.newExprId(),
                    seq(),
                    empty(),
                    seq());
        }
        return avgAlias;
    }

    /**
     * top values sub-query
     */
    private static Alias topValuesSubQueryAlias(FieldSummary fieldSummary, CatalystPlanContext context, UnresolvedAttribute fieldLiteral, UnresolvedFunction count) {
        int topValues = 5;// this value should come from the FieldSummary definition
        CreateNamedStruct structExpr = new CreateNamedStruct(seq(
                fieldLiteral,
                count
        ));
        // Alias COLLECT_LIST(STRUCT(field, COUNT(field))) AS top_values
        UnresolvedFunction collectList = new UnresolvedFunction(
                seq("COLLECT_LIST"),
                seq(structExpr),
                false,
                empty(),
                !fieldSummary.isIncludeNull()
        );
        Alias topValuesAlias = Alias$.MODULE$.apply(
                collectList,
                TOP_VALUES,
                NamedExpression.newExprId(),
                seq(),
                empty(),
                seq()
        );
        Project subQueryProject = new Project(seq(topValuesAlias), buildTopValueSubQuery(topValues, fieldLiteral, context));
        ScalarSubquery scalarSubquery = ScalarSubquery$.MODULE$.apply(
                subQueryProject,
                seq(new ArrayList<Expression>()),
                NamedExpression.newExprId(),
                seq(new ArrayList<Expression>()),
                empty(),
                empty());

        return Alias$.MODULE$.apply(
                scalarSubquery,
                TOP_VALUES,
                NamedExpression.newExprId(),
                seq(),
                empty(),
                seq()
        );
    }

    /**
     * inner top values query
     * -----------------------------------------------------
     * :  :  +- 'Project [unresolvedalias('COLLECT_LIST(struct(status_code, count_status)), None)]
     * :  :        +- 'GlobalLimit 5
     * :  :           +- 'LocalLimit 5
     * :  :              +- 'Sort ['count_status DESC NULLS LAST], true
     * :  :                 +- 'Aggregate ['status_code], ['status_code, 'COUNT(1) AS count_status#27]
     * :  :                    +- 'UnresolvedRelation [spark_catalog, default, flint_ppl_test], [], false
     */
    private static LogicalPlan buildTopValueSubQuery(int topValues,UnresolvedAttribute fieldLiteral, CatalystPlanContext context ) {
        //Alias for the count(field) as Count
        UnresolvedFunction countFunc = new UnresolvedFunction(seq(COUNT.name()), seq(fieldLiteral), false, empty(), false);
        Alias countAlias = Alias$.MODULE$.apply(countFunc,
                COUNT.name(),
                NamedExpression.newExprId(),
                seq(),
                empty(),
                seq());
        Aggregate aggregate = new Aggregate(seq(fieldLiteral), seq(countAlias), context.getPlan());
        Project project = new Project(seq(fieldLiteral, countAlias), aggregate);
        SortOrder sortOrder = new SortOrder(countAlias, Descending$.MODULE$, Ascending$.MODULE$.defaultNullOrdering(), seq());
        Sort sort = new Sort(seq(sortOrder), true, project);
        GlobalLimit limit = new GlobalLimit(Literal.create(topValues, IntegerType), new LocalLimit(Literal.create(topValues, IntegerType), sort));
        return new SubqueryAlias(new AliasIdentifier(TOP_VALUES+"_subquery"), limit);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar;
import org.apache.spark.sql.catalyst.expressions.Alias$;
import org.apache.spark.sql.catalyst.expressions.Coalesce$;
import org.apache.spark.sql.catalyst.expressions.EqualTo$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ppl.CatalystExpressionVisitor;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface LookupTransformer {

    /** lookup mapping fields + input fields*/
    static List<NamedExpression> buildLookupRelationProjectList(
        Lookup node,
        CatalystExpressionVisitor expressionAnalyzer,
        CatalystPlanContext context) {
        List<Field> inputFields = new ArrayList<>(node.getInputFieldList());
        if (inputFields.isEmpty()) {
            // All fields will be applied to the output if no input field is specified.
            return Collections.singletonList(new UnresolvedStar(Option.empty()));
        }
        inputFields.addAll(node.getLookupMappingMap().keySet());
        return buildProjectListFromFields(inputFields, expressionAnalyzer, context);
    }

    static List<NamedExpression> buildProjectListFromFields(
        List<Field> fields,
        CatalystExpressionVisitor expressionAnalyzer,
        CatalystPlanContext context) {
        return fields.stream().map(field -> expressionAnalyzer.visitField(field, context))
            .map(NamedExpression.class::cast)
            .collect(Collectors.toList());
    }

    static Expression buildLookupMappingCondition(
        Lookup node,
        CatalystExpressionVisitor expressionAnalyzer,
        CatalystPlanContext context) {
        // only equi-join conditions are accepted in lookup command
        List<Expression> equiConditions = new ArrayList<>();
        for (Map.Entry<Field, Field> entry : node.getLookupMappingMap().entrySet()) {
            Expression lookupNamedExpression;
            Expression sourceNamedExpression;
            if (entry.getKey().getField() == entry.getValue().getField()) {
                Field lookupWithAlias = buildFieldWithLookupSubqueryAlias(node, entry.getKey());
                Field sourceWithAlias = buildFieldWithSourceSubqueryAlias(node, entry.getValue());
                lookupNamedExpression = expressionAnalyzer.visitField(lookupWithAlias, context);
                sourceNamedExpression = expressionAnalyzer.visitField(sourceWithAlias, context);
            } else {
                lookupNamedExpression = expressionAnalyzer.visitField(entry.getKey(), context);
                sourceNamedExpression = expressionAnalyzer.visitField(entry.getValue(), context);
            }

            Expression equalTo = EqualTo$.MODULE$.apply(lookupNamedExpression, sourceNamedExpression);
            equiConditions.add(equalTo);
        }
        context.resetNamedParseExpressions();
        return equiConditions.stream().reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
    }

    static List<NamedExpression> buildOutputProjectList(
        Lookup node,
        Lookup.OutputStrategy strategy,
        CatalystExpressionVisitor expressionAnalyzer,
        CatalystPlanContext context,
        LogicalPlan searchSide) {
        List<NamedExpression> outputProjectList = new ArrayList<>();
        for (Map.Entry<Alias, Field> entry : node.getOutputCandidateMap().entrySet()) {
            Alias inputFieldWithAlias = entry.getKey();
            Field inputField = (Field) inputFieldWithAlias.getDelegated();
            Field outputField = entry.getValue();
            // Always resolve the inputCol expression with alias: __auto_generated_subquery_name_l.<fieldName>
            // If the outputField existed in source table, resolve the outputCol expression with alias: __auto_generated_subquery_name_s.<fieldName>
            // If not, resolve the outputCol expression without alias: <fieldName> to avoid failure of unable to resolved attribute.
            Expression inputCol = expressionAnalyzer.visitField(buildFieldWithLookupSubqueryAlias(node, inputField), context);
            Expression outputCol;
            if (RelationUtils.columnExistsInCatalogTable(context.getSparkSession(), searchSide, outputField)) {
                outputCol = expressionAnalyzer.visitField(buildFieldWithSourceSubqueryAlias(node, outputField), context);
            } else {
                outputCol = expressionAnalyzer.visitField(outputField, context);
            }
            Expression child;
            if (strategy == Lookup.OutputStrategy.APPEND) {
                child = Coalesce$.MODULE$.apply(seq(outputCol, inputCol));
            } else {
                child = inputCol;
            }
            NamedExpression output = Alias$.MODULE$.apply(child,
                inputFieldWithAlias.getName(),
                NamedExpression.newExprId(),
                seq(new java.util.ArrayList<String>()),
                Option.empty(),
                seq(new java.util.ArrayList<String>()));
            outputProjectList.add(output);
        }
        context.resetNamedParseExpressions();
        return outputProjectList;
    }

    static Field buildFieldWithLookupSubqueryAlias(Lookup node, Field field) {
        return new Field(QualifiedName.of(node.getLookupSubqueryAliasName(), field.getField().toString()));
    }

    static Field buildFieldWithSourceSubqueryAlias(Lookup node, Field field) {
        return new Field(QualifiedName.of(node.getSourceSubqueryAliasName(), field.getField().toString()));
    }
}

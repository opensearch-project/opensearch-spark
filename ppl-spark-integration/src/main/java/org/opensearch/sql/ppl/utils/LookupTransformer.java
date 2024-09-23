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
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ppl.CatalystPlanContext;
import org.opensearch.sql.ppl.CatalystQueryPlanVisitor;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface LookupTransformer {

    /** lookup fields (left side join keys) + replace fields in lookup relation */
    static List<NamedExpression> buildLookupRelationProjectList(
        Lookup node,
        CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
        CatalystPlanContext context) {
        List<Field> lookupOutputFields = new ArrayList<>(node.getLookupOutputFieldList());
        if (lookupOutputFields.isEmpty()) {
            // All fields will be applied to the output if no lookup output field is specified.
            return Collections.singletonList(new UnresolvedStar(Option.empty()));
        }
        lookupOutputFields.addAll(node.getLookupMappingMap().keySet());
        return buildProjectListFromFields(lookupOutputFields, expressionAnalyzer, context);
    }

    static List<NamedExpression> buildProjectListFromFields(
        List<Field> fields,
        CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
        CatalystPlanContext context) {
        return fields.stream().map(field -> expressionAnalyzer.visitField(field, context))
            .map(NamedExpression.class::cast)
            .collect(Collectors.toList());
    }

    static Expression buildLookupMappingCondition(
        Lookup node,
        CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
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
        context.retainAllNamedParseExpressions(e -> e);
        return equiConditions.stream().reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
    }

    static List<NamedExpression> buildOutputProjectList(
        Lookup node,
        Lookup.OutputStrategy strategy,
        CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
        CatalystPlanContext context) {
        List<NamedExpression> outputProjectList = new ArrayList<>();
        for (Map.Entry<Alias, Field> entry : node.getOutputCandidateMap().entrySet()) {
            Alias lookupOutputFieldWithAlias = entry.getKey();
            Field lookupOutputField = (Field) lookupOutputFieldWithAlias.getDelegated();
            Field sourceOutputField = entry.getValue();
            Expression lookupOutputCol = expressionAnalyzer.visitField(lookupOutputField, context);
            Expression sourceOutputCol = expressionAnalyzer.visitField(sourceOutputField, context);
//            Expression lookupOutputCol;
//            Expression sourceOutputCol;
//            if (lookupOutputField.getField() == sourceOutputField.getField()) {
//                Field sourceWithAlias = buildFieldWithSourceSubqueryAlias(node, sourceOutputField);
//                Field lookupWithAlias = buildFieldWithLookupSubqueryAlias(node, lookupOutputField);
//                sourceOutputCol = expressionAnalyzer.visitField(sourceWithAlias, context);
//                lookupOutputCol = expressionAnalyzer.visitField(lookupWithAlias, context);
//            } else {
//                lookupOutputCol = expressionAnalyzer.visitField(lookupOutputField, context);
//                sourceOutputCol = expressionAnalyzer.visitField(sourceOutputField, context);
//            }

            Expression child;
            if (strategy == Lookup.OutputStrategy.APPEND) {
                child = Coalesce$.MODULE$.apply(seq(sourceOutputCol, lookupOutputCol));
            } else {
                child = lookupOutputCol;
            }
            // The result output project list we build here is used to replace the source output,
            // for the unmatched rows of left outer join, the outputs are null, so fall back to source output.
            Expression nullSafeOutput = Coalesce$.MODULE$.apply(seq(child, sourceOutputCol));
            NamedExpression nullSafeOutputCol = Alias$.MODULE$.apply(nullSafeOutput,
                lookupOutputFieldWithAlias.getName(),
                NamedExpression.newExprId(),
                seq(new java.util.ArrayList<String>()),
                Option.empty(),
                seq(new java.util.ArrayList<String>()));
            outputProjectList.add(nullSafeOutputCol);
        }
        context.retainAllNamedParseExpressions(p -> p);
        return outputProjectList;
    }

    static Field buildFieldWithLookupSubqueryAlias(Lookup node, Field field) {
        return new Field(QualifiedName.of(node.getLookupSubqueryAliasName(), field.getField().toString()));
    }

    static Field buildFieldWithSourceSubqueryAlias(Lookup node, Field field) {
        return new Field(QualifiedName.of(node.getSourceSubqueryAliasName(), field.getField().toString()));
    }
}

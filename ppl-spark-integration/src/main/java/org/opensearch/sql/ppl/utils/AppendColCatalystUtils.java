/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias$;
import org.apache.spark.sql.execution.CommandExecutionMode;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.collection.JavaConverters.seqAsJavaList;

/**
 * Util class to facilitate the logical plan composition for APPENDCOL command.
 */
public interface AppendColCatalystUtils {

    /**
     * Responsible to traverse given subSearch Node till the last child, then append the Relation clause,
     * in order to specify the data source || index for the subSearch.
     * @param subSearch User provided sub-search from APPENDCOL command.
     * @param relation Relation clause which represent the dataSource that this sub-search execute upon.
     */
    static void appendRelationClause(Node subSearch, List<LogicalPlan> relation) {

        final List<UnresolvedExpression> unresolvedExpressionList = relation.stream()
                .map(r -> {
                    UnresolvedRelation unresolvedRelation = (UnresolvedRelation) r;
                    List<String> multipartId = seqAsJavaList(unresolvedRelation.multipartIdentifier());
                    return (UnresolvedExpression) new QualifiedName(multipartId);
                })
                // To avoid stack overflow in the case of chained AppendCol.
                .distinct()
                .collect(Collectors.toList());
        final Relation table = new Relation(unresolvedExpressionList);
        while (subSearch != null) {
            try {
                subSearch = subSearch.getChild().get(0);
            } catch (NullPointerException ex) {
                ((UnresolvedPlan) subSearch).attach(table);
                break;
            }
        }
    }


    /**
     * Util method extract output fields from given LogicalPlan instance in non-recursive manner,
     * and return null in the case of non-supported LogicalPlan.
     * @param lp LogicalPlan instance to extract the projection fields from.
     * @param tableName the table || schema name being appended as part of the returned fields.
     * @return A list of Expression instances with alternated tableName || Schema information.
     */
    static List<Expression> getOverridedList(LogicalPlan lp, String tableName) {
        // Extract the output from supported LogicalPlan type.
        if (lp instanceof Project || lp instanceof Aggregate) {
            return seqAsJavaList(lp.output()).stream()
                    .map(attr -> new UnresolvedAttribute(seq(tableName, attr.name())))
                    .collect(Collectors.toList());
        }
        return null;
    }

    /**
     * Helper method to first add an additional projection clause to provide row_number, then wrap it SubqueryAlias and return.
     * @param context Context object of the current Parser.
     * @param lp The Logical Plan instance which contains the query.
     * @param alias The name of the Alias clause.
     * @return A subqueryAlias instance which has row_number for natural ordering purpose.
     */
    static SubqueryAlias getRowNumStarProjection(CatalystPlanContext context, LogicalPlan lp, String alias) {
        final SortOrder sortOrder = SortUtils.sortOrder(
                new Literal(
                        UTF8String.fromString("1"), DataTypes.StringType), false);

        final NamedExpression appendCol = WindowSpecTransformer.buildRowNumber(seq(), seq(sortOrder));
        final List<NamedExpression> projectList = (context.getNamedParseExpressions().isEmpty())
                ? List.of(appendCol, new UnresolvedStar(Option.empty()))
                : List.of(appendCol);

        final LogicalPlan lpWithProjection = new Project(seq(
                projectList), lp);
        return SubqueryAlias$.MODULE$.apply(alias, lpWithProjection);
    }
}

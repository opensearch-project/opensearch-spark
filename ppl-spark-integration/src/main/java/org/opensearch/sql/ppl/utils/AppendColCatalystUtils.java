package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias$;
import org.apache.spark.sql.execution.CommandExecutionMode;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;
import scala.collection.Seq;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.collection.JavaConverters.seqAsJavaList;

/**
 * Util class to facilitate the logical plan composition for APPENDCOL command.
 */
public interface AppendColCatalystUtils {

    /**
     * Response to traverse given subSearch Node till the last child, then append the Relation clause,
     * in order to specify the data source || index.
     * @param subSearch User provided sub-search from APPENDCOL command.
     * @param relation Relation clause which represent the dataSource that this sub-search execute upon.
     */
    static void appendRelationClause(Node subSearch, Relation relation) {
        Relation table = new Relation(relation.getTableNames());
        // Replace it with a function to look up the search command and extract the index name.
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
     * Util method to traverse a given Node object and return the first occurrence of a Relation clause.
     * @param node The Node object that this util method search upon.
     * @return The first occurrence of Relation object from the given Node.
     */
    static Relation retrieveRelationClause(Node node) {
        while (node != null) {
            if (node instanceof Relation) {
                return (Relation) node;
            } else {
                try {
                    node = node.getChild().get(0);
                } catch (NullPointerException ex) {
                    // Base on the current implementation of Flint,
                    // NPE will be thrown by certain type of Node implementation,
                    // when node.getChild() being called.
                    break;
                }
            }
        }
        return null;
    }


    /**
     * Util method to perform analyzed() call against the given LogicalPlan to exact all fields
     * that will be projected upon the execution in the form of Java List with user provided schema prefix.
     * @param lp LogicalPlan instance to extract the projection fields from.
     * @param tableName the table || schema name being appended as part of the returned fields.
     * @return A list of Expression instances with alternated tableName || Schema information.
     */
    static List<Expression> getoverridedlist(LogicalPlan lp, String tableName) {
        // When override option present, extract fields to project from sub-search,
        // then apply a dfDropColumns on main-search to avoid duplicate fields.
        final SparkSession sparkSession = SparkSession.getActiveSession().get();
        final QueryExecution queryExecutionSub = sparkSession.sessionState()
                .executePlan(lp, CommandExecutionMode.SKIP());
        final Seq<Attribute> output = queryExecutionSub.analyzed().output();
        final List<Attribute> attributes = seqAsJavaList(output);
        return attributes.stream()
                .map(attr ->
                        new UnresolvedAttribute(seq(tableName, attr.name())))
                .collect(Collectors.toList());
    }

    /**
     * Helper method to first add an additional project clause to provide row_number, then wrap it SubqueryAlias and return.
     * @param context Context object of the current Parser.
     * @param lp The Logical Plan instance which contains the query.
     * @param alias The name of the Alias clause.
     * @return A subqeuryAlias instance which has row_number for natural ordering purpose.
     */
    static SubqueryAlias getRowNumStarProjection(CatalystPlanContext context, LogicalPlan lp, String alias) {
        final SortOrder sortOrder = SortUtils.sortOrder(
                new org.apache.spark.sql.catalyst.expressions.Literal(
                        UTF8String.fromString("1"), DataTypes.StringType), false);

        final NamedExpression appendCol = WindowSpecTransformer.buildRowNumber(seq(), seq(sortOrder));
        final List<NamedExpression> projectList = (context.getNamedParseExpressions().isEmpty())
                ? List.of(appendCol, new UnresolvedStar(Option.empty()))
                : List.of(appendCol);

        final LogicalPlan lpWithProjection = new org.apache.spark.sql.catalyst.plans.logical.Project(seq(
                projectList), lp);
        return SubqueryAlias$.MODULE$.apply(alias, lpWithProjection);
    }
}

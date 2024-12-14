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

    static Relation retrieveRelationClause(Node node) {
        while (node != null) {
            if (node instanceof Relation) {
                return (Relation) node;
            } else {
                try {
                    node = node.getChild().get(0);
                } catch (NullPointerException ex) {
                    // NPE will be thrown by some node.getChild() call.
                    break;
                }
            }
        }
        return null;
    }

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

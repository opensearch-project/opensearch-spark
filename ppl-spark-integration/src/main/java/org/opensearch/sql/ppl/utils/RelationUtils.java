package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.expression.QualifiedName;

import java.util.List;
import java.util.Optional;

public interface RelationUtils {
    /**
     * attempt resolving if the field is relating to the given relation
     * if name doesnt contain table prefix - add the current relation prefix to the fields name - returns true
     * if name does contain table prefix - verify field's table name corresponds to the current contextual relation
     *
     * @param relations
     * @param node
     * @param contextRelations
     * @return
     */
    static Optional<QualifiedName> resolveField(List<UnresolvedRelation> relations, QualifiedName node, List<LogicalPlan> tables) {
        //when is only a single tables in the query - return the node as is to be resolved by the schema itself
        if(tables.size()==1) return Optional.of(node);
        //when is more than one table in the query (union or join) - filter out nodes that dont apply to the current relation
        return relations.stream()
                .filter(rel -> node.getPrefix().isEmpty() ||
                        node.getPrefix().map(prefix -> prefix.toString().equals(rel.tableName()))
                                .orElse(false))
                .findFirst()
                .map(rel -> node);
    }
}

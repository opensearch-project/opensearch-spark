/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.expression.QualifiedName;
import scala.Option$;

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
     * @param tables
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

    static TableIdentifier getTableIdentifier(QualifiedName qualifiedName) {
        TableIdentifier identifier;
        if (qualifiedName.getParts().isEmpty()) {
            throw new IllegalArgumentException("Empty table name is invalid");
        } else if (qualifiedName.getParts().size() == 1) {
            identifier = new TableIdentifier(qualifiedName.getParts().get(0));
        } else if (qualifiedName.getParts().size() == 2) {
            identifier = new TableIdentifier(
                qualifiedName.getParts().get(1),
                Option$.MODULE$.apply(qualifiedName.getParts().get(0)));
        } else if (qualifiedName.getParts().size() == 3) {
            identifier = new TableIdentifier(
                qualifiedName.getParts().get(2),
                Option$.MODULE$.apply(qualifiedName.getParts().get(1)),
                Option$.MODULE$.apply(qualifiedName.getParts().get(0)));
        } else {
            throw new IllegalArgumentException("Invalid table name: " + qualifiedName
                + " Syntax: [ database_name. ] table_name");
        }
        return identifier;
    }
}

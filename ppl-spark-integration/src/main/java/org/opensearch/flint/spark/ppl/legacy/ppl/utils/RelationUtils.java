/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ppl.utils;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.flint.spark.ppl.PPLSparkUtils;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.Field;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.QualifiedName;
import scala.Option$;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface RelationUtils {
    Logger LOG = LogManager.getLogger(RelationUtils.class);

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
            // TODO Do not support 4+ parts table identifier in future (may be reverted this PR in 0.8.0)
            // qualifiedName.getParts().size() > 3
            // A Spark TableIdentifier should only contain 3 parts: tableName, databaseName and catalogName.
            // If the qualifiedName has more than 3 parts,
            // we merge all parts from 3 to last parts into the tableName as one whole
            identifier = new TableIdentifier(
                String.join(".", qualifiedName.getParts().subList(2, qualifiedName.getParts().size())),
                Option$.MODULE$.apply(qualifiedName.getParts().get(1)),
                Option$.MODULE$.apply(qualifiedName.getParts().get(0)));
        }
        return identifier;
    }

    static boolean columnExistsInCatalogTable(SparkSession spark, LogicalPlan plan, Field field) {
        return getFieldsFromCatalogTable(spark, plan).stream().anyMatch(f -> f.getField().equals(field.getField()));
    }

    static List<Field> getFieldsFromCatalogTable(SparkSession spark, LogicalPlan plan) {
        UnresolvedRelation relation = PPLSparkUtils.findLogicalRelations(plan).head();
        QualifiedName tableQualifiedName = QualifiedName.of(Arrays.asList(relation.tableName().split("\\.")));
        TableIdentifier tableIdentifier = getTableIdentifier(tableQualifiedName);
        boolean tableExists = spark.sessionState().catalog().tableExists(tableIdentifier);
        if (tableExists) {
            try {
                CatalogTable table = spark.sessionState().catalog().getTableMetadata(getTableIdentifier(tableQualifiedName));
                return Arrays.stream(table.dataSchema().fields()).map(f -> new Field(QualifiedName.of(f.name()))).collect(Collectors.toList());
            } catch (NoSuchDatabaseException | NoSuchTableException e) {
                LOG.info("Table or database {} not found", tableIdentifier);
                return ImmutableList.of();
            }
        } else {
            LOG.info("Table {} not found", tableIdentifier);
            return ImmutableList.of();
        }
    }
}

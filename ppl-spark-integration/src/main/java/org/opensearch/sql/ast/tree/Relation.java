/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ppl.utils.RelationUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Logical plan node of Relation, the interface for building the searching sources. */
@ToString
@EqualsAndHashCode(callSuper = false)
public class Relation extends UnresolvedPlan {
  private static final String COMMA = ",";
  private final List<UnresolvedExpression> tableNames;

  @Setter @Getter private Optional<RelationUtils.TablesampleContext> tablesampleContext;
  /** Optional alias name for the relation. */
  @Setter @Getter private String alias;

  public Relation(List<UnresolvedExpression> tableNames) {
    this(tableNames, null, null);
  }

  public Relation(List<UnresolvedExpression> tableNames, Optional<RelationUtils.TablesampleContext> tablesampleContext) {
    this(tableNames, null, tablesampleContext);
  }

  public Relation(List<UnresolvedExpression> tableNames, String alias) {
    this.tableNames = tableNames;
    this.alias = alias;
  }

  public Relation(List<UnresolvedExpression> tableNames, String alias, Optional<RelationUtils.TablesampleContext> tablesampleContext) {
    this.tableNames = tableNames;
    this.alias = alias;
    this.tablesampleContext = tablesampleContext;
  }


  /**
   * Return table name.
   *
   * @return table name
   */
  public List<String> getTableName() {
    return tableNames.stream().map(Object::toString).collect(Collectors.toList());
  }

  public List<QualifiedName> getQualifiedNames() {
    return tableNames.stream().map(t -> (QualifiedName) t).collect(Collectors.toList());
  }

  /**
   * Get Qualified name preservs parts of the user given identifiers. This can later be utilized to
   * determine DataSource,Schema and Table Name during Analyzer stage. So Passing QualifiedName
   * directly to Analyzer Stage.
   *
   * @return TableQualifiedName.
   */
  public QualifiedName getTableQualifiedName() {
    if (tableNames.size() == 1) {
      return (QualifiedName) tableNames.get(0);
    } else {
      return new QualifiedName(
              tableNames.stream()
              .map(UnresolvedExpression::toString)
              .collect(Collectors.joining(COMMA)));
    }
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelation(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }
}

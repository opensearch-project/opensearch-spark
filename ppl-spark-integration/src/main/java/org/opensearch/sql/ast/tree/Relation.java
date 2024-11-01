/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.stream.Collectors;

/** Logical plan node of Relation, the interface for building the searching sources. */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Relation extends UnresolvedPlan {
  private static final String COMMA = ",";

  private final List<UnresolvedExpression> tableName;

  public List<QualifiedName> getQualifiedNames() {
    return tableName.stream().map(t -> (QualifiedName) t).collect(Collectors.toList());
  }

  /**
   * Get Qualified name preservs parts of the user given identifiers. This can later be utilized to
   * determine DataSource,Schema and Table Name during Analyzer stage. So Passing QualifiedName
   * directly to Analyzer Stage.
   *
   * @return TableQualifiedName.
   */
  public QualifiedName getTableQualifiedName() {
    if (tableName.size() == 1) {
      return (QualifiedName) tableName.get(0);
    } else {
      return new QualifiedName(
          tableName.stream()
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

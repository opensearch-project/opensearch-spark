/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

import java.util.List;

/** AST node that represents CASE clause similar as Switch statement in programming language. */
public class Case extends UnresolvedExpression {

  /** Value to be compared by WHEN statements. Null in the case of CASE WHEN conditions. */
  private UnresolvedExpression caseValue;

  /**
   * Expression list that represents WHEN statements. Each is a mapping from condition to its
   * result.
   */
  private List<When> whenClauses;

  /** Expression that represents ELSE statement result. */
  private UnresolvedExpression elseClause;

  public Case(UnresolvedExpression caseValue, List<When> whenClauses, UnresolvedExpression elseClause) {
    this.caseValue =caseValue;
    this.whenClauses = whenClauses;
    this.elseClause = elseClause;
  }

  @Override
  public List<? extends Node> getChild() {
    ImmutableList.Builder<Node> children = ImmutableList.builder();
    if (caseValue != null) {
      children.add(caseValue);
    }
    children.addAll(whenClauses);

    if (elseClause != null) {
      children.add(elseClause);
    }
    return children.build();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCase(this, context);
  }
}

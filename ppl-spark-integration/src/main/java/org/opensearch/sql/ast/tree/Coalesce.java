/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

/** AST node represent Coalesce operation. */

public class Coalesce extends UnresolvedPlan {
  /** Expressions. */
  private List<UnresolvedExpression> expressions;

  /** Child Plan. */
  private UnresolvedPlan child;

  public Coalesce(List<UnresolvedExpression> expressions, UnresolvedPlan child) {
    this.expressions = expressions;
    this.child = child;
  }

  public Coalesce(List<UnresolvedExpression> expressions) {
    this.expressions = expressions;
  }

  public List<UnresolvedExpression> getExpressions() {
    return expressions;
  }

  @Override
  public Coalesce attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCoalesce(this, context);
  }
}

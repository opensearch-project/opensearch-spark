/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Let;

import java.util.List;

/** AST node represent Eval operation. */
public class Eval extends UnresolvedPlan {
  private List<Let> expressionList;
  private UnresolvedPlan child;

  public Eval(List<Let> expressionList) {
    this.expressionList = expressionList;
  }

  public List<Let> getExpressionList() {
    return expressionList;
  }

  @Override
  public Eval attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitEval(this, context);
  }
}

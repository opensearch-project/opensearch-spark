/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Expression node of logic AND. */
public class And extends UnresolvedExpression {
  private UnresolvedExpression left;
  private UnresolvedExpression right;

  public And(UnresolvedExpression left, UnresolvedExpression right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(left, right);
  }

  public UnresolvedExpression getLeft() {
    return left;
  }

  public UnresolvedExpression getRight() {
    return right;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAnd(this, context);
  }
}

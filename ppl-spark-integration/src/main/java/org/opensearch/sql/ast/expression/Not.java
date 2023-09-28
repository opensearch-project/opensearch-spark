/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Expression node of the logic NOT. */

public class Not extends UnresolvedExpression {
  private UnresolvedExpression expression;

  public Not(UnresolvedExpression expression) {
    this.expression = expression;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(expression);
  }

  public UnresolvedExpression getExpression() {
    return expression;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitNot(this, context);
  }
}

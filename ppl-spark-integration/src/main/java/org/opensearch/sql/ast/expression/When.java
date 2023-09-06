/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

import java.util.List;

/** AST node that represents WHEN clause. */
public class When extends UnresolvedExpression {

  /** WHEN condition, either a search condition or compare value if case value present. */
  private UnresolvedExpression condition;

  /** Result to return if condition matched. */
  private UnresolvedExpression result;

  public When(UnresolvedExpression condition, UnresolvedExpression result) {
    this.condition = condition;
    this.result = result;
  }

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of(condition, result);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitWhen(this, context);
  }
}

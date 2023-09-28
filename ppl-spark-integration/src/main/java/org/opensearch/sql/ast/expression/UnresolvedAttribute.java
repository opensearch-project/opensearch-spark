/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/**
 * Expression node, representing the syntax that is not resolved to any other expression nodes yet
 * but non-negligible This expression is often created as the index name, field name etc.
 */

public class UnresolvedAttribute extends UnresolvedExpression {
  private String attr;

  public UnresolvedAttribute(String attr) {
    this.attr = attr;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnresolvedAttribute(this, context);
  }
}

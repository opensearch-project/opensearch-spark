/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

/** Logical plan node of Filter, the interface for building filters in queries. */

public class Filter extends UnresolvedPlan {
  private UnresolvedExpression condition;
  private UnresolvedPlan child;

  public Filter(UnresolvedExpression condition) {
    this.condition = condition;
  }

  @Override
  public Filter attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  public UnresolvedExpression getCondition() {
    return condition;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitFilter(this, context);
  }
}

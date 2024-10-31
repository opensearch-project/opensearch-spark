/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;

/** Logical plan node of Expand */
@ToString
@EqualsAndHashCode(callSuper = false)
@Getter
public class Expand extends UnresolvedPlan {
  private UnresolvedExpression field;
  private UnresolvedPlan child;

  public Expand(UnresolvedExpression field) {
    this.field = field;
  }

  @Override
  public Expand attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitExpand(this, context);
  }
}

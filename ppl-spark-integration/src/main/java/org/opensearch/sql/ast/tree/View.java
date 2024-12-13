/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.Collections;
import java.util.List;

/** Logical plan node of Project, the interface for building the list of searching fields. */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class View extends UnresolvedPlan {
  @Setter private List<UnresolvedExpression> viewList;
  private List<Argument> argExprList;
  private UnresolvedPlan child;

  public View(List<UnresolvedExpression> viewList) {
    this.viewList = viewList;
    this.argExprList = Collections.emptyList();
  }

  public View(List<UnresolvedExpression> viewList, List<Argument> argExprList) {
    this.viewList = viewList;
    this.argExprList = argExprList;
  }

  public boolean hasArgument() {
    return !argExprList.isEmpty();
  }

  /** The Project could been used to exclude fields from the source. */
  public boolean isExcluded() {
    if (hasArgument()) {
      Argument argument = argExprList.get(0);
      return (Boolean) argument.getValue().getValue();
    }
    return false;
  }

  @Override
  public View attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {

    return nodeVisitor.visitView(this, context);
  }
}

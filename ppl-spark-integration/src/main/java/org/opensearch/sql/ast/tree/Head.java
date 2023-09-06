/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/** AST node represent Head operation. */

public class Head extends UnresolvedPlan {

  private UnresolvedPlan child;
  private Integer size;
  private Integer from;

  public Head(UnresolvedPlan child, Integer size, Integer from) {
    this.child = child;
    this.size = size;
    this.from = from;
  }

  public Head(Integer size, Integer from) {
    this.size = size;
    this.from = from;
  }

  @Override
  public Head attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  public Integer getSize() {
    return size;
  }

  public Integer getFrom() {
    return from;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitHead(this, context);
  }
}

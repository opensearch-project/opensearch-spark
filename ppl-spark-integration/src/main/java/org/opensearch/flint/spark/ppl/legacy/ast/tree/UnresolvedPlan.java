/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.tree;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;
import org.opensearch.flint.spark.ppl.legacy.ast.Node;

/** Abstract unresolved plan. */
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class UnresolvedPlan extends Node {
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }

  public abstract UnresolvedPlan attach(UnresolvedPlan child);
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Expression node of one-to-one mapping relation. */

public class Map extends UnresolvedExpression {
  private UnresolvedExpression origin;
  private UnresolvedExpression target;

  public Map(UnresolvedExpression origin, UnresolvedExpression target) {
    this.origin = origin;
    this.target = target;
  }

  public UnresolvedExpression getOrigin() {
    return origin;
  }

  public UnresolvedExpression getTarget() {
    return target;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(origin, target);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitMap(this, context);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;

import java.util.Collections;
import java.util.List;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Interval extends UnresolvedExpression {

  private final UnresolvedExpression value;
  private final IntervalUnit unit;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.singletonList(value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitInterval(this, context);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Collections;
import java.util.List;


public class Interval extends UnresolvedExpression {

  private final UnresolvedExpression value;
  private final IntervalUnit unit;

  public Interval(UnresolvedExpression value, IntervalUnit unit) {
    this.value = value;
    this.unit = unit;
  }
  public Interval(UnresolvedExpression value, String unit) {
    this.value = value;
    this.unit = IntervalUnit.of(unit);
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.singletonList(value);
  }

  public UnresolvedExpression getValue() {
    return value;
  }

  public IntervalUnit getUnit() {
    return unit;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitInterval(this, context);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/** Span expression node. Params include field expression and the span value. */
public class Span extends UnresolvedExpression {
  private UnresolvedExpression field;
  private UnresolvedExpression value;
  private SpanUnit unit;

  public Span(UnresolvedExpression field, UnresolvedExpression value, SpanUnit unit) {
    this.field = field;
    this.value = value;
    this.unit = unit;
  }

  public UnresolvedExpression getField() {
    return field;
  }

  public UnresolvedExpression getValue() {
    return value;
  }

  public SpanUnit getUnit() {
    return unit;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of(field, value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitSpan(this, context);
  }
}

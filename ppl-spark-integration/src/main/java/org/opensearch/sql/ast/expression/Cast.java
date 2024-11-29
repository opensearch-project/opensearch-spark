/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Expression node of cast
 */
@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Cast extends UnresolvedExpression {
  private final UnresolvedExpression expression;
  private final DataType dataType;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.singletonList(expression);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitCast(this, context);
  }

  @Override
  public String toString() {
    return String.format("CAST(%s AS %s)", expression, dataType);
  }
}

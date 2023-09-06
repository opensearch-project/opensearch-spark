/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/**
 * Expression node of one-to-many mapping relation IN. Params include the field expression and/or
 * wildcard field expression, nested field expression (@field). And the values that the field is
 * mapped to (@valueList).
 */

public class In extends UnresolvedExpression {
  private UnresolvedExpression field;
  private List<UnresolvedExpression> valueList;

  public In(UnresolvedExpression field, List<UnresolvedExpression> valueList) {
    this.field = field;
    this.valueList = valueList;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(field);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitIn(this, context);
  }
}

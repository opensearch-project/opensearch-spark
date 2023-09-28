/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Argument. */

public class UnresolvedArgument extends UnresolvedExpression {
  private final String argName;
  private final UnresolvedExpression value;

  public UnresolvedArgument(String argName, UnresolvedExpression value) {
    this.argName = argName;
    this.value = value;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnresolvedArgument(this, context);
  }
}

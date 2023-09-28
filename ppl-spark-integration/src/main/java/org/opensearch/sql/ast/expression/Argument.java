/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Argument. */
public class Argument extends UnresolvedExpression {
  private final String name;
  private String argName;
  private Literal value;

  public Argument(String name, Literal value) {
    this.name = name;
    this.value = value;
  }

  //    private final DataType valueType;
  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(value);
  }

  public String getArgName() {
    return argName;
  }

  public Literal getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitArgument(this, context);
  }
}

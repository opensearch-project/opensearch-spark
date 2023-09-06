/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Expression node of scalar function. Params include function name (@funcName) and function
 * arguments (@funcArgs)
 */

public class Function extends UnresolvedExpression {
  private String funcName;
  private List<UnresolvedExpression> funcArgs;

  public Function(String funcName, List<UnresolvedExpression> funcArgs) {
    this.funcName = funcName;
    this.funcArgs = funcArgs;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.unmodifiableList(funcArgs);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitFunction(this, context);
  }

  public String getFuncName() {
    return funcName;
  }

  public List<UnresolvedExpression> getFuncArgs() {
    return funcArgs;
  }

  @Override
  public String toString() {
    return String.format(
        "%s(%s)",
        funcName, funcArgs.stream().map(Object::toString).collect(Collectors.joining(", ")));
  }
}

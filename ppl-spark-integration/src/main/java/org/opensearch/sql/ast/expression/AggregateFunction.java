/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

/**
 * Expression node of aggregate functions. Params include aggregate function name (AVG, SUM, MAX
 * etc.) and the field to aggregate.
 */
public class AggregateFunction extends UnresolvedExpression {
  private final String funcName;
  private final UnresolvedExpression field;
  private final List<UnresolvedExpression> argList;

  private UnresolvedExpression condition;

  private Boolean distinct = false;

  /**
   * Constructor.
   *
   * @param funcName function name.
   * @param field {@link UnresolvedExpression}.
   */
  public AggregateFunction(String funcName, UnresolvedExpression field) {
    this.funcName = funcName;
    this.field = field;
    this.argList = Collections.emptyList();
  }

  /**
   * Constructor.
   *
   * @param funcName function name.
   * @param field {@link UnresolvedExpression}.
   * @param distinct whether distinct field is specified or not.
   */
  public AggregateFunction(String funcName, UnresolvedExpression field, Boolean distinct) {
    this.funcName = funcName;
    this.field = field;
    this.argList = Collections.emptyList();
    this.distinct = distinct;
  }

  public AggregateFunction(String funcName, UnresolvedExpression field, List<UnresolvedExpression> argList) {
    this.funcName = funcName;
    this.field = field;
    this.argList = argList;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return Collections.singletonList(field);
  }

  public String getFuncName() {
    return funcName;
  }

  public UnresolvedExpression getField() {
    return field;
  }

  public List<UnresolvedExpression> getArgList() {
    return argList;
  }

  public UnresolvedExpression getCondition() {
    return condition;
  }

  public Boolean getDistinct() {
    return distinct;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAggregateFunction(this, context);
  }

  @Override
  public String toString() {
    return format("%s(%s)", funcName, field);
  }

  public UnresolvedExpression condition(UnresolvedExpression condition) {
    this.condition = condition;
    return this;
  }
}

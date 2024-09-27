/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

import java.util.Arrays;
import java.util.List;

/** Unresolved expression for BETWEEN. */
@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class Between extends UnresolvedExpression {

  /** Value for range check. */
  private UnresolvedExpression value;

  /** Lower bound of the range (inclusive). */
  private UnresolvedExpression lowerBound;

  /** Upper bound of the range (inclusive). */
  private UnresolvedExpression upperBound;

  @Override
  public List<? extends Node> getChild() {
    return Arrays.asList(value, lowerBound, upperBound);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitBetween(this, context);
  }
}

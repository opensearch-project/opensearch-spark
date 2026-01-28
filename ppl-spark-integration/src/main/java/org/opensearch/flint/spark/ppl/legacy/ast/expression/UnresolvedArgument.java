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

import java.util.Arrays;
import java.util.List;

/** Argument. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class UnresolvedArgument extends UnresolvedExpression {
  private final String argName;
  private final UnresolvedExpression value;

  @Override
  public List<UnresolvedExpression> getChild() {
    return Arrays.asList(value);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitUnresolvedArgument(this, context);
  }
}

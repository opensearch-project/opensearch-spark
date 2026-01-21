/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;

/** Expression node of logic AND. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class And extends BinaryExpression {

  public And(UnresolvedExpression left, UnresolvedExpression right) {
    super(left, right);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAnd(this, context);
  }
}

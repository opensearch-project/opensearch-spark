/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl.legacy.ast.expression;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;

import java.util.Arrays;
import java.util.List;

/** Expression node of the logic XOR. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class Xor extends BinaryExpression {

  public Xor(UnresolvedExpression left, UnresolvedExpression right) {
    super(left, right);
  }
  
  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitXor(this, context);
  }
}

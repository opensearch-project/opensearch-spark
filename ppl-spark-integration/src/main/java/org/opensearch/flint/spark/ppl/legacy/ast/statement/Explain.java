/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flint.spark.ppl.legacy.ast.statement;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.flint.spark.ppl.legacy.ast.AbstractNodeVisitor;

/** Explain Statement. */
@Data
@EqualsAndHashCode(callSuper = false)
public class Explain extends Statement {

  private final Statement statement;
  private final ExplainMode explainMode;

  public Explain(Query statement, String explainMode) {
    this.statement = statement;
    this.explainMode = ExplainMode.valueOf(explainMode);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }

  public enum ExplainMode {
    formatted,
    cost,
    codegen,
    extended,
    simple
  }
}

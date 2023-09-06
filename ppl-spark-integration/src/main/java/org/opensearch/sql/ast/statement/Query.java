/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ast.statement;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/** Query Statement. */
public class Query extends Statement {

  protected UnresolvedPlan plan;
  protected int fetchSize;

  public Query(UnresolvedPlan plan, int fetchSize) {
    this.plan = plan;
    this.fetchSize = fetchSize;
  }

  public UnresolvedPlan getPlan() {
    return plan;
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }
}

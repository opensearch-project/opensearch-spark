/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Logical plan node of Top (Aggregation) command, the interface for building aggregation actions in queries. */
public class TopAggregation extends Aggregation {
  private final Optional<Literal> results;

  /** Aggregation Constructor without span and argument. */
  public TopAggregation(
      Optional<Literal> results,
      List<UnresolvedExpression> aggExprList,
      List<UnresolvedExpression> sortExprList,
      List<UnresolvedExpression> groupExprList) {
    super(aggExprList, sortExprList, groupExprList, null, Collections.emptyList());
    this.results = results;
  }

  public Optional<Literal> getResults() {
    return results;
  }
}

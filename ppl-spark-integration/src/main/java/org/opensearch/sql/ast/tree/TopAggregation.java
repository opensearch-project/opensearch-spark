/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.Collections;
import java.util.List;

/** Logical plan node of Top (Aggregation) command, the interface for building aggregation actions in queries. */
public class TopAggregation extends Aggregation {
  /** Aggregation Constructor without span and argument. */
  public TopAggregation(
      List<UnresolvedExpression> aggExprList,
      List<UnresolvedExpression> sortExprList,
      List<UnresolvedExpression> groupExprList) {
    super(aggExprList, sortExprList, groupExprList, null, Collections.emptyList());
  }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * The context used for Catalyst logical plan.
 */
public class CatalystPlanContext {
    /**
     * Catalyst evolving logical plan
     **/
    private LogicalPlan plan;

    /**
     * NamedExpression contextual parameters
     **/
    private final List<NamedExpression> namedParseExpressions;

    public LogicalPlan getPlan() {
        return plan;
    }

    public List<NamedExpression> getNamedParseExpressions() {
        return namedParseExpressions;
    }

    public CatalystPlanContext() {
        this.namedParseExpressions = new ArrayList<>();
    }


    /**
     * update context with evolving plan
     *
     * @param plan
     */
    public void plan(LogicalPlan plan) {
        this.plan = plan;
    }
}

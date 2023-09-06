/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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
    private final Stack<Expression> namedParseExpressions;

    public LogicalPlan getPlan() {
        return plan;
    }

    public Stack<Expression> getNamedParseExpressions() {
        return namedParseExpressions;
    }

    public CatalystPlanContext() {
        this.namedParseExpressions = new Stack<>();
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

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;

import java.util.Stack;
import java.util.function.Function;

import static scala.collection.JavaConverters.asScalaBuffer;

/**
 * The context used for Catalyst logical plan.
 */
public class CatalystPlanContext {
    /**
     * Catalyst evolving logical plan
     **/
    private Stack<LogicalPlan> planBranches = new Stack<>();

    /**
     * NamedExpression contextual parameters
     **/
    private final Stack<org.apache.spark.sql.catalyst.expressions.Expression> namedParseExpressions = new Stack<>();

    public LogicalPlan getPlan() {
        if (this.planBranches.size() == 1) {
            return planBranches.peek();
        }
        //default unify sub-plans
        return new Union(asScalaBuffer(this.planBranches).toSeq(), true, true);
    }

    public Stack<org.apache.spark.sql.catalyst.expressions.Expression> getNamedParseExpressions() {
        return namedParseExpressions;
    }

    /**
     * append context with evolving plan
     *
     * @param plan
     */
    public void with(LogicalPlan plan) {
        this.planBranches.push(plan);
    }

    public void plan(Function<LogicalPlan, LogicalPlan> transformFunction) {
        this.planBranches.replaceAll(transformFunction::apply);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import scala.collection.Seq;

import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
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

    /**
     * Grouping NamedExpression contextual parameters
     **/
    private final Stack<org.apache.spark.sql.catalyst.expressions.Expression> groupingParseExpressions = new Stack<>();

    public Stack<LogicalPlan> getPlanBranches() {
        return planBranches;
    }

    public LogicalPlan getPlan() {
        if (this.planBranches.size() == 1) {
            return planBranches.peek();
        }
        //default unify sub-plans
        return new Union(asScalaBuffer(this.planBranches), true, true);
    }

    public Stack<Expression> getNamedParseExpressions() {
        return namedParseExpressions;
    }

    public Stack<Expression> getGroupingParseExpressions() {
        return groupingParseExpressions;
    }

    /**
     * append context with evolving plan
     *
     * @param plan
     * @return
     */
    public LogicalPlan with(LogicalPlan plan) {
        return this.planBranches.push(plan);
    }

    public LogicalPlan plan(Function<LogicalPlan, LogicalPlan> transformFunction) {
        this.planBranches.replaceAll(transformFunction::apply);
        return getPlan();
    }
 
     /**
     * retain all logical plans branches
     * @return
     */
    public <T> Seq<T> retainAllPlans(Function<LogicalPlan, T> transformFunction) {
        Seq<T> plans = seq(getPlanBranches().stream().map(transformFunction).collect(Collectors.toList()));
        getPlanBranches().retainAll(emptyList());
        return plans;
    }
     /**
      * 
     * retain all expressions and clear expression stack
     * @return
     */
    public <T> Seq<T> retainAllNamedParseExpressions(Function<Expression, T> transformFunction) {
        Seq<T> aggregateExpressions = seq(getNamedParseExpressions().stream()
                .map(transformFunction).collect(Collectors.toList()));
        getNamedParseExpressions().retainAll(emptyList());
        return aggregateExpressions;
    }

    /**
     * retain all aggregate expressions and clear expression stack
     * @return
     */
    public <T> Seq<T> retainAllGroupingNamedParseExpressions(Function<Expression, T> transformFunction) {
        Seq<T> aggregateExpressions = seq(getGroupingParseExpressions().stream()
                .map(transformFunction).collect(Collectors.toList()));
        getGroupingParseExpressions().retainAll(emptyList());
        return aggregateExpressions;
    }
}

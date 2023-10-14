/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static scala.collection.JavaConverters.asJavaCollection;
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
     * The current traversal context the visitor is going threw
     */
    private Stack<LogicalPlan> planTraversalContext = new Stack<>();

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

    /**
     * get the current traversals visitor context
     *
     * @return
     */
    public Stack<LogicalPlan> traversalContext() {
        return planTraversalContext;
    }

    public Stack<Expression> getNamedParseExpressions() {
        return namedParseExpressions;
    }

    public Optional<Expression> popNamedParseExpressions() {
        return namedParseExpressions.isEmpty() ? Optional.empty() : Optional.of(namedParseExpressions.pop());
    }

    public Stack<Expression> getGroupingParseExpressions() {
        return groupingParseExpressions;
    }

    /**
     * append plan with evolving plans branches
     *
     * @param plan
     * @return
     */
    public LogicalPlan with(LogicalPlan plan) {
        return this.planBranches.push(plan);
    }
    /**
     * append plans collection with evolving plans branches
     *
     * @param plans
     * @return
     */
    public LogicalPlan withAll(Collection<LogicalPlan> plans) {
        this.planBranches.addAll(plans);
        return getPlan();
    }

    /**
     * reduce all plans with the given reduce function
     * @param transformFunction
     * @return
     */
    public LogicalPlan reduce(BiFunction<LogicalPlan, LogicalPlan, LogicalPlan> transformFunction) {
        return with(asJavaCollection(retainAllPlans(p -> p)).stream().reduce((left, right) -> {
            planTraversalContext.push(left);
            planTraversalContext.push(right);
            LogicalPlan result = transformFunction.apply(left, right);
            planTraversalContext.pop();
            planTraversalContext.pop();
            return result;
        }).orElse(getPlan()));
    }

    /**
     * apply for each plan with the given function 
     * @param transformFunction
     * @return
     */
    public LogicalPlan apply(Function<LogicalPlan, LogicalPlan> transformFunction) {
        return withAll(asJavaCollection(retainAllPlans(p -> p)).stream().map(p -> {
            planTraversalContext.push(p);
            LogicalPlan result = transformFunction.apply(p);
            planTraversalContext.pop();
            return result;
        }).collect(Collectors.toList()));
    }

    /**
     * retain all logical plans branches
     *
     * @return
     */
    public <T> Seq<T> retainAllPlans(Function<LogicalPlan, T> transformFunction) {
        Seq<T> plans = seq(getPlanBranches().stream().map(transformFunction).collect(Collectors.toList()));
        getPlanBranches().retainAll(emptyList());
        return plans;
    }

    /**
     * retain all expressions and clear expression stack
     *
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
     *
     * @return
     */
    public <T> Seq<T> retainAllGroupingNamedParseExpressions(Function<Expression, T> transformFunction) {
        Seq<T> aggregateExpressions = seq(getGroupingParseExpressions().stream()
                .map(transformFunction).collect(Collectors.toList()));
        getGroupingParseExpressions().retainAll(emptyList());
        return aggregateExpressions;
    }

    public static List<UnresolvedRelation> findRelation(Stack<LogicalPlan> plan) {
        return plan.stream()
                .map(CatalystPlanContext::findRelation)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }
   
    public static Optional<UnresolvedRelation> findRelation(LogicalPlan plan) {
        // Check if the current node is an UnresolvedRelation
            if (plan instanceof UnresolvedRelation) {
                return Optional.of((UnresolvedRelation) plan);
            }
    
            // Traverse the children of the current node
            Iterator<LogicalPlan> children = plan.children().iterator();
            while (children.hasNext()) {
                Optional<UnresolvedRelation> result = findRelation(children.next());
                if (result.isPresent()) {
                    return result;
                }
            }
    
            // Return null if no UnresolvedRelation is found
            return Optional.empty();
        }
}

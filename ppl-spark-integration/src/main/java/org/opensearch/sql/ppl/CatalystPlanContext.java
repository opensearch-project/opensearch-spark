/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.ArrayList;
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

    @Getter private SparkSession sparkSession;
    /**
     * Catalyst relations list
     **/
    @Getter private List<UnresolvedExpression> projectedFields = new ArrayList<>();
    /**
     * Catalyst relations list
     **/
    @Getter private List<LogicalPlan> relations = new ArrayList<>();
    /**
     * Catalyst SubqueryAlias list
     **/
    @Getter private List<LogicalPlan> subqueryAlias = new ArrayList<>();
    /**
     * Catalyst evolving logical plan
     **/
    @Getter private Stack<LogicalPlan> planBranches = new Stack<>();
    /**
     * The current traversal context the visitor is going threw
     */
    private Stack<LogicalPlan> planTraversalContext = new Stack<>();

    /**
     * NamedExpression contextual parameters
     **/
    @Getter private final Stack<org.apache.spark.sql.catalyst.expressions.Expression> namedParseExpressions = new Stack<>();

    /**
     * Grouping NamedExpression contextual parameters
     **/
    @Getter private final Stack<org.apache.spark.sql.catalyst.expressions.Expression> groupingParseExpressions = new Stack<>();

    public LogicalPlan getPlan() {
        if (this.planBranches.isEmpty()) return null;
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

    public void setNamedParseExpressions(Stack<org.apache.spark.sql.catalyst.expressions.Expression> namedParseExpressions) {
        this.namedParseExpressions.clear();
        this.namedParseExpressions.addAll(namedParseExpressions);
    }

    public Optional<Expression> popNamedParseExpressions() {
        return namedParseExpressions.isEmpty() ? Optional.empty() : Optional.of(namedParseExpressions.pop());
    }

    /**
     * define new field
     *
     * @param symbol
     * @return
     */
    public LogicalPlan define(Expression symbol) {
        namedParseExpressions.push(symbol);
        return getPlan();
    }

    /**
     * append relation to relations list
     *
     * @param relation
     * @return
     */
    public LogicalPlan withRelation(UnresolvedRelation relation) {
        this.relations.add(relation);
        return with(relation);
    }

    public void withSubqueryAlias(SubqueryAlias subqueryAlias) {
        this.subqueryAlias.add(subqueryAlias);
    }

    /**
     * append projected fields
     *
     * @param projectedFields
     * @return
     */
    public LogicalPlan withProjectedFields(List<UnresolvedExpression> projectedFields) {
        this.projectedFields.addAll(projectedFields);
        return getPlan();
    }

    public LogicalPlan applyBranches(List<Function<LogicalPlan, LogicalPlan>> plans) {
        plans.forEach(plan -> with(plan.apply(planBranches.get(0))));
        planBranches.remove(0);
        return getPlan();
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
     *
     * @param transformFunction
     * @return
     */
    public LogicalPlan reduce(BiFunction<LogicalPlan, LogicalPlan, LogicalPlan> transformFunction) {
        Collection<LogicalPlan> logicalPlans = asJavaCollection(retainAllPlans(p -> p));
        // in case it is a self join - single table - apply the same plan
        if (logicalPlans.size() < 2) {
            return with(logicalPlans.stream().map(plan -> {
                        planTraversalContext.push(plan);
                        LogicalPlan result = transformFunction.apply(plan, plan);
                        planTraversalContext.pop();
                        return result;
                    }).findAny()
                    .orElse(getPlan()));
        }
        // in case there are multiple join tables - reduce the tables
        return with(logicalPlans.stream().reduce((left, right) -> {
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
     *
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
     * Reset all expressions in stack,
     * generally use it after calling visitFirstChild() in visit methods.
     */
    public void resetNamedParseExpressions() {
        getNamedParseExpressions().retainAll(emptyList());
    }

    /**
     * retain all expressions and clear expression stack
     *
     * @return
     */
    public <T> Seq<T> retainAllNamedParseExpressions(Function<Expression, T> transformFunction) {
        Seq<T> aggregateExpressions = seq(getNamedParseExpressions().stream()
                .map(transformFunction).collect(Collectors.toList()));
        resetNamedParseExpressions();
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

    @Getter private boolean isResolvingJoinCondition = false;

    /**
     * Resolve the join condition with the given function.
     * A flag will be set to true ahead expression resolving, then false after resolving.
     * @param expr
     * @param transformFunction
     * @return
     */
    public Expression resolveJoinCondition(
            UnresolvedExpression expr,
            BiFunction<UnresolvedExpression, CatalystPlanContext, Expression> transformFunction) {
        isResolvingJoinCondition = true;
        Expression result = transformFunction.apply(expr, this);
        isResolvingJoinCondition = false;
        return result;
    }

    public void withSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }
}

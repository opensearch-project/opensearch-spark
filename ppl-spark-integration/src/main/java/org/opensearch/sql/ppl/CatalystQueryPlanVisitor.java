/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Predicate;
import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Limit;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ppl.utils.AggregatorTranslator;
import org.opensearch.sql.ppl.utils.ComparatorTransformer;
import org.opensearch.sql.ppl.utils.SortUtils;
import scala.Option;
import scala.collection.Seq;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.List.of;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;
import static org.opensearch.sql.ppl.utils.WindowSpecTransformer.window;

/**
 * Utility class to traverse PPL logical plan and translate it into catalyst logical plan
 */
public class CatalystQueryPlanVisitor extends AbstractNodeVisitor<LogicalPlan, CatalystPlanContext> {

    private final ExpressionAnalyzer expressionAnalyzer;

    public CatalystQueryPlanVisitor() {
        this.expressionAnalyzer = new ExpressionAnalyzer();
    }

    public LogicalPlan visit(Statement plan, CatalystPlanContext context) {
        return plan.accept(this, context);
    }

    /**
     * Handle Query Statement.
     */
    @Override
    public LogicalPlan visitQuery(Query node, CatalystPlanContext context) {
        return node.getPlan().accept(this, context);
    }

    @Override
    public LogicalPlan visitExplain(Explain node, CatalystPlanContext context) {
        return node.getStatement().accept(this, context);
    }

    @Override
    public LogicalPlan visitRelation(Relation node, CatalystPlanContext context) {
        node.getTableName().forEach(t -> {
            // Resolving the qualifiedName which is composed of a datasource.schema.table
            context.with(new UnresolvedRelation(seq(of(t.split("\\."))), CaseInsensitiveStringMap.empty(), false));
        });
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitFilter(Filter node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        Expression conditionExpression = visitExpression(node.getCondition(), context);
        Expression innerConditionExpression = context.getNamedParseExpressions().pop();
        return context.plan(p -> new org.apache.spark.sql.catalyst.plans.logical.Filter(innerConditionExpression, p));
    }

    @Override
    public LogicalPlan visitAggregation(Aggregation node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        List<Expression> aggsExpList = visitExpressionList(node.getAggExprList(), context);
        List<Expression> groupExpList = visitExpressionList(node.getGroupExprList(), context);

        if (!groupExpList.isEmpty()) {
            //add group by fields to context
            context.getGroupingParseExpressions().addAll(groupExpList);
        }

        UnresolvedExpression span = node.getSpan();
        if (!Objects.isNull(span)) {
            span.accept(this, context);
            //add span's group alias field (most recent added expression)
            context.getGroupingParseExpressions().add(context.getNamedParseExpressions().peek());
        }
        // build the aggregation logical step
        return extractedAggregation(context);
    }
    
    private static LogicalPlan extractedAggregation(CatalystPlanContext context) {
        Seq<Expression> groupingExpression = context.retainAllGroupingNamedParseExpressions(p -> p);
        Seq<NamedExpression> aggregateExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        return context.plan(p -> new Aggregate(groupingExpression, aggregateExpressions, p));
    }

    @Override
    public LogicalPlan visitAlias(Alias node, CatalystPlanContext context) {
        expressionAnalyzer.visitAlias(node, context);
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitProject(Project node, CatalystPlanContext context) {
        LogicalPlan child = node.getChild().get(0).accept(this, context);
        List<Expression> expressionList = visitExpressionList(node.getProjectList(), context);

        // Create a projection list from the existing expressions
        Seq<?> projectList = seq(context.getNamedParseExpressions());
        if (!projectList.isEmpty()) {
            Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
            // build the plan with the projection step
            child = context.plan(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
        }
        if (node.hasArgument()) {
            Argument argument = node.getArgExprList().get(0);
            //todo exclude the argument from the projected arguments list
        }
        return child;
    }
    
    @Override
    public LogicalPlan visitSort(Sort node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        visitFieldList(node.getSortList(), context);
        Seq<SortOrder> sortElements = context.retainAllNamedParseExpressions(exp -> SortUtils.getSortDirection(node, (NamedExpression) exp));
        return context.plan(p -> (LogicalPlan) new org.apache.spark.sql.catalyst.plans.logical.Sort(sortElements, true, p));
    }

    @Override
    public LogicalPlan visitHead(Head node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        return context.plan(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
                node.getSize(), DataTypes.IntegerType), p));
    }

    private void visitFieldList(List<Field> fieldList, CatalystPlanContext context) {
        fieldList.forEach(field -> visitExpression(field, context));
    }

    private List<Expression> visitExpressionList(List<UnresolvedExpression> expressionList, CatalystPlanContext context) {
        return expressionList.isEmpty()
                ? emptyList()
                : expressionList.stream().map(field -> visitExpression(field, context))
                .collect(Collectors.toList());
    }

    private Expression visitExpression(UnresolvedExpression expression, CatalystPlanContext context) {
        return expressionAnalyzer.analyze(expression, context);
    }

    @Override
    public LogicalPlan visitEval(Eval node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Eval");
    }

    @Override
    public LogicalPlan visitKmeans(Kmeans node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Kmeans");
    }

    @Override
    public LogicalPlan visitIn(In node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : In");
    }

    @Override
    public LogicalPlan visitCase(Case node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Case");
    }

    @Override
    public LogicalPlan visitRareTopN(RareTopN node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : RareTopN");
    }

    @Override
    public LogicalPlan visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : WindowFunction");
    }

    @Override
    public LogicalPlan visitDedupe(Dedupe node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : dedupe ");
    }

    /**
     * Expression Analyzer.
     */
    private static class ExpressionAnalyzer extends AbstractNodeVisitor<Expression, CatalystPlanContext> {

        public Expression analyze(UnresolvedExpression unresolved, CatalystPlanContext context) {
            return unresolved.accept(this, context);
        }

        @Override
        public Expression visitLiteral(Literal node, CatalystPlanContext context) {
            return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Literal(
                    translate(node.getValue(), node.getType()), translate(node.getType())));
        }

        @Override
        public Expression visitAnd(And node, CatalystPlanContext context) {
            node.getLeft().accept(this, context);
            Expression left = (Expression) context.getNamedParseExpressions().pop();
            node.getRight().accept(this, context);
            Expression right = (Expression) context.getNamedParseExpressions().pop();
            return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.And(left, right));
        }

        @Override
        public Expression visitOr(Or node, CatalystPlanContext context) {
            node.getLeft().accept(this, context);
            Expression left = (Expression) context.getNamedParseExpressions().pop();
            node.getRight().accept(this, context);
            Expression right = (Expression) context.getNamedParseExpressions().pop();
            return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Or(left, right));
        }

        @Override
        public Expression visitXor(Xor node, CatalystPlanContext context) {
            node.getLeft().accept(this, context);
            Expression left = (Expression) context.getNamedParseExpressions().pop();
            node.getRight().accept(this, context);
            Expression right = (Expression) context.getNamedParseExpressions().pop();
            return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.BitwiseXor(left, right));
        }

        @Override
        public Expression visitNot(Not node, CatalystPlanContext context) {
            node.getExpression().accept(this, context);
            Expression arg = (Expression) context.getNamedParseExpressions().pop();
            return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Not(arg));
        }

        @Override
        public Expression visitSpan(Span node, CatalystPlanContext context) {
            node.getField().accept(this, context);
            Expression field = (Expression) context.getNamedParseExpressions().pop();
            node.getValue().accept(this, context);
            Expression value = (Expression) context.getNamedParseExpressions().pop();
            return context.getNamedParseExpressions().push(window(field, value, node.getUnit()));
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction node, CatalystPlanContext context) {
            node.getField().accept(this, context);
            Expression arg = (Expression) context.getNamedParseExpressions().pop();
            Expression aggregator = AggregatorTranslator.aggregator(node, arg);
            return context.getNamedParseExpressions().push(aggregator);
        }

        @Override
        public Expression visitCompare(Compare node, CatalystPlanContext context) {
            analyze(node.getLeft(), context);
            Expression left = (Expression) context.getNamedParseExpressions().pop();
            analyze(node.getRight(), context);
            Expression right = (Expression) context.getNamedParseExpressions().pop();
            Predicate comparator = ComparatorTransformer.comparator(node, left, right);
            return context.getNamedParseExpressions().push((org.apache.spark.sql.catalyst.expressions.Expression) comparator);
        }

        @Override
        public Expression visitField(Field node, CatalystPlanContext context) {
            return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getField().toString())));
        }

        @Override
        public Expression visitAllFields(AllFields node, CatalystPlanContext context) {
            // Case of aggregation step - no start projection can be added
            if (context.getNamedParseExpressions().isEmpty()) {
                // Create an UnresolvedStar for all-fields projection
                context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
            }
            return context.getNamedParseExpressions().peek();
        }

        @Override
        public Expression visitAlias(Alias node, CatalystPlanContext context) {
            node.getDelegated().accept(this, context);
            Expression arg = context.getNamedParseExpressions().pop();
            return context.getNamedParseExpressions().push(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(arg,
                            node.getAlias() != null ? node.getAlias() : node.getName(),
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>())));
        }

        @Override
        public Expression visitEval(Eval node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Eval");
        }

        @Override
        public Expression visitFunction(Function node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Function");
        }

        @Override
        public Expression visitInterval(Interval node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Interval");
        }

        @Override
        public Expression visitDedupe(Dedupe node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Dedupe");
        }

        @Override
        public Expression visitIn(In node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : In");
        }

        @Override
        public Expression visitKmeans(Kmeans node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Kmeans");
        }

        @Override
        public Expression visitCase(Case node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Case");
        }

        @Override
        public Expression visitRareTopN(RareTopN node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : RareTopN");
        }

        @Override
        public Expression visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : WindowFunction");
        }
    }
}

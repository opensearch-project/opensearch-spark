/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Floor;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Predicate;
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
import org.opensearch.sql.ast.expression.Let;
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

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.List.of;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;
import static scala.collection.JavaConverters.asScalaBuffer;
import static scala.collection.JavaConverters.asScalaBufferConverter;

/**
 * Utility class to traverse PPL logical plan and translate it into catalyst logical plan
 */
public class CatalystQueryPlanVisitor extends AbstractNodeVisitor<String, CatalystPlanContext> {

    private final ExpressionAnalyzer expressionAnalyzer;

    public CatalystQueryPlanVisitor() {
        this.expressionAnalyzer = new ExpressionAnalyzer();
    }

    public String visit(Statement plan, CatalystPlanContext context) {
        //build plan 
        String planDesc = plan.accept(this, context);
        //add limit statement
        visitLimit(context);
        //add order statement
        visitSort(context);
        return planDesc;
    }

    /**
     * Handle Query Statement.
     */
    @Override
    public String visitQuery(Query node, CatalystPlanContext context) {
        return node.getPlan().accept(this, context);
    }

    @Override
    public String visitExplain(Explain node, CatalystPlanContext context) {
        return node.getStatement().accept(this, context);
    }

    @Override
    public String visitRelation(Relation node, CatalystPlanContext context) {
        node.getTableName().forEach(t -> {
            // Resolving the qualifiedName which is composed of a datasource.schema.table
            context.with(new UnresolvedRelation(asScalaBuffer(of(t.split("\\."))).toSeq(), CaseInsensitiveStringMap.empty(), false));
        });
        return format("source=%s", node.getTableName());
    }

    @Override
    public String visitFilter(Filter node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        String innerCondition = visitExpression(node.getCondition(), context);
        Expression innerConditionExpression = context.getNamedParseExpressions().pop();
        context.plan(p -> new org.apache.spark.sql.catalyst.plans.logical.Filter(innerConditionExpression, p));
        return format("%s | where %s", child, innerCondition);
    }

    @Override
    public String visitAggregation(Aggregation node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        final String visitExpressionList = visitExpressionList(node.getAggExprList(), context);
        final String group = visitExpressionList(node.getGroupExprList(), context);
        
        if (!isNullOrEmpty(group)) {
            //add group by fields to context
            extractedGroupBy(node.getGroupExprList().size(),context);
        }
        
        UnresolvedExpression span = node.getSpan();
        if (!Objects.isNull(span)) {
            span.accept(this, context);
            //add span's group by field to context
            extractedGroupBy(1,context);
        }
        // build the aggregation logical step
        extractedAggregation(context);
        return format(
                "%s | stats %s",
                child, String.join(" ", visitExpressionList, groupBy(group)).trim());
    }

    private static void extractedGroupBy(int groupByElementsCount, CatalystPlanContext context) {
        //copy the group by aliases from the namedExpressionList to the groupByExpressionList  
        for (int i = 1; i <= groupByElementsCount; i++) {
            context.getGroupingParseExpressions().add(context.getNamedParseExpressions().get(context.getNamedParseExpressions().size()-i));
        }
    }

    private static void extractedAggregation(CatalystPlanContext context) {
        Seq<Expression> groupingExpression = context.retainAllGroupingNamedParseExpressions(p -> p);
        Seq<NamedExpression> aggregateExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
        context.plan(p -> new Aggregate(groupingExpression, aggregateExpressions, p));
    }

    @Override
    public String visitAlias(Alias node, CatalystPlanContext context) {
        return expressionAnalyzer.visitAlias(node, context);
    }

    @Override
    public String visitProject(Project node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        String arg = "+";
        String fields = visitExpressionList(node.getProjectList(), context);

        // Create a projection list from the existing expressions
        Seq<?> projectList = asScalaBuffer(context.getNamedParseExpressions()).toSeq();
        if (!projectList.isEmpty()) {
            Seq<NamedExpression> projectExpressions = context.retainAllNamedParseExpressions(p -> (NamedExpression) p);
            // build the plan with the projection step
            context.plan(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
        }
        if (node.hasArgument()) {
            Argument argument = node.getArgExprList().get(0);
            Boolean exclude = (Boolean) argument.getValue().getValue();
            if (exclude) {
                arg = "-";
            }
        }
        return format("%s | fields %s %s", child, arg, fields);
    }

    private static void visitSort(CatalystPlanContext context) {
        if (!context.getSortOrders().isEmpty()) {
            context.plan(p -> (LogicalPlan) new org.apache.spark.sql.catalyst.plans.logical.Sort(context.getSortOrders(), true, p));
        }
    }

    private static void visitLimit(CatalystPlanContext context) {
        if (context.getLimit() > 0) {
            context.plan(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
                    context.getLimit(), DataTypes.IntegerType), p));
        }
    }

    @Override
    public String visitEval(Eval node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        ImmutableList.Builder<Pair<String, String>> expressionsBuilder = new ImmutableList.Builder<>();
        for (Let let : node.getExpressionList()) {
            String expression = visitExpression(let.getExpression(), context);
            String target = let.getVar().getField().toString();
            expressionsBuilder.add(ImmutablePair.of(target, expression));
        }
        String expressions =
                expressionsBuilder.build().stream()
                        .map(pair -> format("%s" + "=%s", pair.getLeft(), pair.getRight()))
                        .collect(Collectors.joining(" "));
        return format("%s | eval %s", child, expressions);
    }

    @Override
    public String visitSort(Sort node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        String sortList = visitFieldList(node.getSortList(), context);
        context.sort(context.retainAllNamedParseExpressions(exp -> SortUtils.getSortDirection(node, (NamedExpression) exp)));
        return format("%s | sort %s", child, sortList);
    }

    @Override
    public String visitHead(Head node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        Integer size = node.getSize();
        context.limit(size);
        return format("%s | head %d", child, size);
    }

    private String visitFieldList(List<Field> fieldList, CatalystPlanContext context) {
        return fieldList.stream().map(field -> visitExpression(field, context)).collect(Collectors.joining(","));
    }

    private String visitExpressionList(List<UnresolvedExpression> expressionList, CatalystPlanContext context) {
        return expressionList.isEmpty()
                ? ""
                : expressionList.stream().map(field -> visitExpression(field, context))
                .collect(Collectors.joining(","));
    }

    private String visitExpression(UnresolvedExpression expression, CatalystPlanContext context) {
        return expressionAnalyzer.analyze(expression, context);
    }

    private String groupBy(String groupBy) {
        return isNullOrEmpty(groupBy) ? "" : format("by %s", groupBy);
    }

    @Override
    public String visitKmeans(Kmeans node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Kmeans" );
    }

    @Override
    public String visitIn(In node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : In" );
    }

    @Override
    public String visitCase(Case node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Case" );
    }

    @Override
    public String visitRareTopN(RareTopN node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : RareTopN" );
    }

    @Override
    public String visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : WindowFunction" );
    }

    @Override
    public String visitDedupe(Dedupe node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : dedupe " );
    }
    /**
     * Expression Analyzer.
     */
    private static class ExpressionAnalyzer extends AbstractNodeVisitor<String, CatalystPlanContext> {

        public String analyze(UnresolvedExpression unresolved, CatalystPlanContext context) {
            return unresolved.accept(this, context);
        }

        @Override
        public String visitLiteral(Literal node, CatalystPlanContext context) {
            context.getNamedParseExpressions().add(new org.apache.spark.sql.catalyst.expressions.Literal(
                    translate(node.getValue(), node.getType()), translate(node.getType())));
            return node.toString();
        }

        @Override
        public String visitInterval(Interval node, CatalystPlanContext context) {
            String value = node.getValue().accept(this, context);
            String unit = node.getUnit().name();
            return format("INTERVAL %s %s", value, unit);
        }

        @Override
        public String visitAnd(And node, CatalystPlanContext context) {
            String left = node.getLeft().accept(this, context);
            String right = node.getRight().accept(this, context);
            context.getNamedParseExpressions().add(new org.apache.spark.sql.catalyst.expressions.And(
                    (Expression) context.getNamedParseExpressions().pop(), context.getNamedParseExpressions().pop()));
            return format("%s and %s", left, right);
        }

        @Override
        public String visitOr(Or node, CatalystPlanContext context) {
            String left = node.getLeft().accept(this, context);
            String right = node.getRight().accept(this, context);
            context.getNamedParseExpressions().add(new org.apache.spark.sql.catalyst.expressions.Or(
                    (Expression) context.getNamedParseExpressions().pop(), context.getNamedParseExpressions().pop()));
            return format("%s or %s", left, right);
        }

        @Override
        public String visitXor(Xor node, CatalystPlanContext context) {
            String left = node.getLeft().accept(this, context);
            String right = node.getRight().accept(this, context);
            context.getNamedParseExpressions().add(new org.apache.spark.sql.catalyst.expressions.BitwiseXor(
                    (Expression) context.getNamedParseExpressions().pop(), context.getNamedParseExpressions().pop()));
            return format("%s xor %s", left, right);
        }

        @Override
        public String visitNot(Not node, CatalystPlanContext context) {
            String expr = node.getExpression().accept(this, context);
            context.getNamedParseExpressions().add(new org.apache.spark.sql.catalyst.expressions.Not(
                    (Expression) context.getNamedParseExpressions().pop()));
            return format("not %s", expr);
        }

        @Override
        public String visitSpan(Span node, CatalystPlanContext context) {
            String field = node.getField().accept(this, context);
            String value = node.getValue().accept(this, context);
            String unit = node.getUnit().name();

            Expression valueExpression = context.getNamedParseExpressions().pop();
            Expression fieldExpression = context.getNamedParseExpressions().pop();
            context.getNamedParseExpressions().push(new Multiply(new Floor(new Divide(fieldExpression, valueExpression)), valueExpression));
            return format("span (%s,%s,%s)", field, value, unit);
        }

        @Override
        public String visitAggregateFunction(AggregateFunction node, CatalystPlanContext context) {
            String arg = node.getField().accept(this, context);
            org.apache.spark.sql.catalyst.expressions.Expression aggregator = AggregatorTranslator.aggregator(node, context);
            context.getNamedParseExpressions().add(aggregator);
            return format("%s(%s)", node.getFuncName(), arg);
        }

        @Override
        public String visitFunction(Function node, CatalystPlanContext context) {
            String arguments =
                    node.getFuncArgs().stream()
                            .map(unresolvedExpression -> analyze(unresolvedExpression, context))
                            .collect(Collectors.joining(","));
            return format("%s(%s)", node.getFuncName(), arguments);
        }

        @Override
        public String visitCompare(Compare node, CatalystPlanContext context) {
            String left = analyze(node.getLeft(), context);
            String right = analyze(node.getRight(), context);
            Predicate comparator = ComparatorTransformer.comparator(node, context);
            context.getNamedParseExpressions().add((org.apache.spark.sql.catalyst.expressions.Expression) comparator);
            return format("%s %s %s", left, node.getOperator(), right);
        }

        @Override
        public String visitField(Field node, CatalystPlanContext context) {
            context.getNamedParseExpressions().add(UnresolvedAttribute$.MODULE$.apply(asScalaBuffer(singletonList(node.getField().toString()))));
            return node.getField().toString();
        }

        @Override
        public String visitAllFields(AllFields node, CatalystPlanContext context) {
            // Case of aggregation step - no start projection can be added
            if (!context.getNamedParseExpressions().isEmpty()) {
                // if named expression exist - just return their names
                return context.getNamedParseExpressions().peek().toString();
            } else {
                // Create an UnresolvedStar for all-fields projection
                context.getNamedParseExpressions().add(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
                return "*";
            }
        }

        @Override
        public String visitAlias(Alias node, CatalystPlanContext context) {
            String expr = node.getDelegated().accept(this, context);
            Expression expression = (Expression) context.getNamedParseExpressions().pop();
            context.getNamedParseExpressions().add(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply((Expression) expression,
                            node.getAlias()!=null ? node.getAlias() : expr,
                            NamedExpression.newExprId(),
                            asScalaBufferConverter(new java.util.ArrayList<String>()).asScala().seq(),
                            Option.empty(),
                            asScalaBufferConverter(new java.util.ArrayList<String>()).asScala().seq()));
            return format("%s", expr);
        }

        @Override
        public String visitDedupe(Dedupe node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Dedupe" );
        }

        @Override
        public String visitIn(In node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : In");
        }

        @Override
        public String visitKmeans(Kmeans node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Kmeans");
        }

        @Override
        public String visitCase(Case node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : Case" );
        }

        @Override
        public String visitRareTopN(RareTopN node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : RareTopN");
        }

        @Override
        public String visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
            throw new IllegalStateException("Not Supported operation : WindowFunction");
        }
    }
}

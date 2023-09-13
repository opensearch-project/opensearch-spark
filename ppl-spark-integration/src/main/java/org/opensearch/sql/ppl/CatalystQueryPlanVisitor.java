/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
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
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ppl.utils.AggregatorTranslator;
import org.opensearch.sql.ppl.utils.ComparatorTransformer;
import scala.Option;
import scala.collection.Seq;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
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
        return plan.accept(this, context);
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
            // todo - how to resolve the qualifiedName is its composed of a datasource + schema
            // QualifiedName qualifiedName = node.getTableQualifiedName();
            // Create an UnresolvedTable node for a table named "qualifiedName" in the default namespace
            context.with(new UnresolvedRelation(asScalaBuffer(of(t.split("\\."))).toSeq(), CaseInsensitiveStringMap.empty(), false));
        });
        return format("source=%s", node.getTableName());
    }

    @Override
    public String visitTableFunction(TableFunction node, CatalystPlanContext context) {
        String arguments =
                node.getArguments().stream()
                        .map(
                                unresolvedExpression ->
                                        this.expressionAnalyzer.analyze(unresolvedExpression, context))
                        .collect(Collectors.joining(","));
        return format("source=%s(%s)", node.getFunctionName().toString(), arguments);
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
    public String visitRename(Rename node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        ImmutableMap.Builder<String, String> renameMapBuilder = new ImmutableMap.Builder<>();
        for (Map renameMap : node.getRenameList()) {
            renameMapBuilder.put(
                    visitExpression(renameMap.getOrigin(), context),
                    ((Field) renameMap.getTarget()).getField().toString());
        }
        String renames =
                renameMapBuilder.build().entrySet().stream()
                        .map(entry -> format("%s as %s", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(","));
        return format("%s | rename %s", child, renames);
    }

    @Override
    public String visitAggregation(Aggregation node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        final String visitExpressionList = visitExpressionList(node.getAggExprList(), context);
        final String group = visitExpressionList(node.getGroupExprList(), context);

        if (!isNullOrEmpty(group)) {
            extractedAggregation(context);
        }
        UnresolvedExpression span = node.getSpan();
        if (!Objects.isNull(span)) {
            span.accept(this, context);
            extractedAggregation(context);
        }
        return format(
                "%s | stats %s",
                child, String.join(" ", visitExpressionList, groupBy(group)).trim());
    }

    private static void extractedAggregation(CatalystPlanContext context) {
        NamedExpression namedExpression = (NamedExpression) context.getNamedParseExpressions().peek();
        Seq<NamedExpression> namedExpressionSeq = asScalaBuffer(context.getNamedParseExpressions().stream()
                .map(v -> (NamedExpression) v).collect(Collectors.toList())).toSeq();
        //now remove all context.getNamedParseExpressions() 
        context.getNamedParseExpressions().retainAll(emptyList());
        context.plan(p -> new Aggregate(asScalaBuffer(singletonList((Expression) namedExpression)), namedExpressionSeq, p));
    }

    @Override
    public String visitAlias(Alias node, CatalystPlanContext context) {
        return expressionAnalyzer.visitAlias(node, context);
    }
    
    @Override
    public String visitRareTopN(RareTopN node, CatalystPlanContext context) {
        final String child = node.getChild().get(0).accept(this, context);
        List<Argument> options = node.getNoOfResults();
        Integer noOfResults = (Integer) options.get(0).getValue().getValue();
        String fields = visitFieldList(node.getFields(), context);
        String group = visitExpressionList(node.getGroupExprList(), context);
        return format(
                "%s | %s %d %s",
                child,
                node.getCommandType().name().toLowerCase(),
                noOfResults,
                String.join(" ", fields, groupBy(group)).trim());
    }


    @Override
    public String visitProject(Project node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        String arg = "+";
        String fields = visitExpressionList(node.getProjectList(), context);

        // Create a projection list from the existing expressions
        Seq<?> projectList = asScalaBuffer(context.getNamedParseExpressions()).toSeq();
        if (!projectList.isEmpty()) {
            // build the plan with the projection step
            context.plan(p -> new org.apache.spark.sql.catalyst.plans.logical.Project((Seq<NamedExpression>) projectList, p));
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
        // the first options is {"count": "integer"}
        String sortList = visitFieldList(node.getSortList(), context);
        return format("%s | sort %s", child, sortList);
    }

    @Override
    public String visitDedupe(Dedupe node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        String fields = visitFieldList(node.getFields(), context);
        List<Argument> options = node.getOptions();
        Integer allowedDuplication = (Integer) options.get(0).getValue().getValue();
        Boolean keepEmpty = (Boolean) options.get(1).getValue().getValue();
        Boolean consecutive = (Boolean) options.get(2).getValue().getValue();

        return format(
                "%s | dedup %s %d keepempty=%b consecutive=%b",
                child, fields, allowedDuplication, keepEmpty, consecutive);
    }

    @Override
    public String visitHead(Head node, CatalystPlanContext context) {
        String child = node.getChild().get(0).accept(this, context);
        Integer size = node.getSize();
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
        public String visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
            return super.visitWindowFunction(node, context);
        }

        @Override
        public String visitAlias(Alias node, CatalystPlanContext context) {
            String expr = node.getDelegated().accept(this, context);
            Expression expression = (Expression) context.getNamedParseExpressions().pop();
            context.getNamedParseExpressions().add(
                    org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply((Expression) expression,
                            expr,
                            NamedExpression.newExprId(),
                            asScalaBufferConverter(new java.util.ArrayList<String>()).asScala().seq(),
                            Option.empty(),
                            asScalaBufferConverter(new java.util.ArrayList<String>()).asScala().seq()));
            return format("%s", expr);
        }
    }
}

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
import org.opensearch.sql.ast.expression.BinaryExpression;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.FieldsMapping;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Correlation;
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
import org.opensearch.sql.ppl.utils.BuiltinFunctionTranslator;
import org.opensearch.sql.ppl.utils.ComparatorTransformer;
import org.opensearch.sql.ppl.utils.SortUtils;
import scala.Option;
import scala.collection.Seq;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.List.of;
import static org.opensearch.sql.ppl.CatalystPlanContext.findRelation;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;
import static org.opensearch.sql.ppl.utils.JoinSpecTransformer.join;
import static org.opensearch.sql.ppl.utils.RelationUtils.resolveField;
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
        node.getTableName().forEach(t ->
                // Resolving the qualifiedName which is composed of a datasource.schema.table
                context.with(new UnresolvedRelation(seq(of(t.split("\\."))), CaseInsensitiveStringMap.empty(), false))
        );
        return context.getPlan();
    }

    @Override
    public LogicalPlan visitFilter(Filter node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        return context.apply(p -> {
            Expression conditionExpression = visitExpression(node.getCondition(), context);
            Optional<Expression> innerConditionExpression = context.popNamedParseExpressions();
            return innerConditionExpression.map(expression -> new org.apache.spark.sql.catalyst.plans.logical.Filter(innerConditionExpression.get(), p)).orElse(null);
        });
    }

    @Override
    public LogicalPlan visitCorrelation(Correlation node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        context.reduce((left,right) -> {
            visitFieldList(node.getFieldsList().stream().map(Field::new).collect(Collectors.toList()), context);
            Seq<Expression> fields = context.retainAllNamedParseExpressions(e -> e);
            if(!Objects.isNull(node.getScope())) {
                // scope - this is a time base expression that timeframes the join to a specific period : (Time-field-name, value, unit)
                expressionAnalyzer.visitSpan(node.getScope(), context);
                context.popNamedParseExpressions().get();
            }
            expressionAnalyzer.visitCorrelationMapping(node.getMappingListContext(), context);
            Seq<Expression> mapping = context.retainAllNamedParseExpressions(e -> e);
            return join(node.getCorrelationType(), fields, mapping, left, right);
        });
        return context.getPlan();
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
        return context.apply(p -> new Aggregate(groupingExpression, aggregateExpressions, p));
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
            child = context.apply(p -> new org.apache.spark.sql.catalyst.plans.logical.Project(projectExpressions, p));
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
        return context.apply(p -> (LogicalPlan) new org.apache.spark.sql.catalyst.plans.logical.Sort(sortElements, true, p));
    }

    @Override
    public LogicalPlan visitHead(Head node, CatalystPlanContext context) {
        node.getChild().get(0).accept(this, context);
        return context.apply(p -> (LogicalPlan) Limit.apply(new org.apache.spark.sql.catalyst.expressions.Literal(
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

        /**
         * generic binary (And, Or, Xor , ...) arithmetic expression resolver
         * @param node
         * @param transformer
         * @param context
         * @return
         */
        public Expression visitBinaryArithmetic(BinaryExpression node, BiFunction<Expression, Expression, Expression> transformer, CatalystPlanContext context) {
            node.getLeft().accept(this, context);
            Optional<Expression> left = context.popNamedParseExpressions();
            node.getRight().accept(this, context);
            Optional<Expression> right = context.popNamedParseExpressions();
            if(left.isPresent() && right.isPresent()) {
                return transformer.apply(left.get(),right.get());
            } else if(left.isPresent()) {
                return context.getNamedParseExpressions().push(left.get());
            } else if(right.isPresent()) {
                return context.getNamedParseExpressions().push(right.get());
            }
            return null;

        }

        @Override
        public Expression visitAnd(And node, CatalystPlanContext context) {
            return visitBinaryArithmetic(node,
                    (left,right)-> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.And(left, right)), context);
        }

        @Override
        public Expression visitOr(Or node, CatalystPlanContext context) {
            return visitBinaryArithmetic(node,
                    (left,right)-> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Or(left, right)), context);
        }

        @Override
        public Expression visitXor(Xor node, CatalystPlanContext context) {
            return visitBinaryArithmetic(node,
                    (left,right)-> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.BitwiseXor(left, right)), context);
        }

        @Override
        public Expression visitNot(Not node, CatalystPlanContext context) {
            node.getExpression().accept(this, context);
            Optional<Expression> arg =  context.popNamedParseExpressions();
            return arg.map(expression -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Not(expression))).orElse(null);
        }

        @Override
        public Expression visitSpan(Span node, CatalystPlanContext context) {
            node.getField().accept(this, context);
            Expression field = (Expression) context.popNamedParseExpressions().get();
            node.getValue().accept(this, context);
            Expression value = (Expression) context.popNamedParseExpressions().get();
            return context.getNamedParseExpressions().push(window(field, value, node.getUnit()));
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction node, CatalystPlanContext context) {
            node.getField().accept(this, context);
            Expression arg = (Expression) context.popNamedParseExpressions().get();
            Expression aggregator = AggregatorTranslator.aggregator(node, arg);
            return context.getNamedParseExpressions().push(aggregator);
        }

        @Override
        public Expression visitCompare(Compare node, CatalystPlanContext context) {
            analyze(node.getLeft(), context);
            Optional<Expression> left = context.popNamedParseExpressions();
            analyze(node.getRight(), context);
            Optional<Expression> right = context.popNamedParseExpressions();
            if (left.isPresent() && right.isPresent()) {
                Predicate comparator = ComparatorTransformer.comparator(node, left.get(), right.get());
                return context.getNamedParseExpressions().push((org.apache.spark.sql.catalyst.expressions.Expression) comparator);
            }
            return null;
        }

        @Override
        public Expression visitQualifiedName(QualifiedName node, CatalystPlanContext context) {
            List<UnresolvedRelation> relation = findRelation(context.traversalContext());
            if (!relation.isEmpty()) {
                Optional<QualifiedName> resolveField = resolveField(relation, node);
                return resolveField.map(qualifiedName -> context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(qualifiedName.getParts()))))
                        .orElse(null);
            }
            return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
        }
        
        @Override
        public Expression visitCorrelationMapping(FieldsMapping node, CatalystPlanContext context) {
            return node.getChild().stream().map(expression ->
                    visitCompare((Compare) expression, context)
            ).reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
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
            Expression arg = context.popNamedParseExpressions().get();
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
            List<Expression> arguments =
                node.getFuncArgs().stream()
                    .map(
                        unresolvedExpression -> {
                            var ret = analyze(unresolvedExpression, context);
                            if (ret == null) {
                                throw new UnsupportedOperationException(
                                    String.format("Invalid use of expression %s", unresolvedExpression));
                            } else {
                                return context.popNamedParseExpressions().get();
                            }
                        })
                    .collect(Collectors.toList());
            Expression function = BuiltinFunctionTranslator.builtinFunction(node, arguments);
            return context.getNamedParseExpressions().push(function);
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

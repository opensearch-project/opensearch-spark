/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Cast$;
import org.apache.spark.sql.catalyst.expressions.Exists$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.In$;
import org.apache.spark.sql.catalyst.expressions.InSubquery$;
import org.apache.spark.sql.catalyst.expressions.LambdaFunction$;
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual;
import org.apache.spark.sql.catalyst.expressions.ListQuery$;
import org.apache.spark.sql.catalyst.expressions.MakeInterval$;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.Predicate;
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery$;
import org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable;
import org.apache.spark.sql.catalyst.expressions.UnresolvedNamedLambdaVariable$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.BinaryExpression;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.FieldsMapping;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.IsEmpty;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.expression.function.SerializableUdf;
import org.opensearch.sql.ppl.utils.AggregatorTransformer;
import org.opensearch.sql.ppl.utils.BuiltinFunctionTransformer;
import org.opensearch.sql.ppl.utils.ComparatorTransformer;
import org.opensearch.sql.ppl.utils.JavaToScalaTransformer;
import scala.Option;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.List.of;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CIDR;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.EQUAL;
import static org.opensearch.sql.ppl.CatalystPlanContext.findRelation;
import static org.opensearch.sql.ppl.utils.BuiltinFunctionTransformer.createIntervalArgs;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.translate;
import static org.opensearch.sql.ppl.utils.RelationUtils.resolveField;
import static org.opensearch.sql.ppl.utils.WindowSpecTransformer.window;

/**
 * Class of building catalyst AST Expression nodes.
 */
public class CatalystExpressionVisitor extends AbstractNodeVisitor<Expression, CatalystPlanContext> {

    private final AbstractNodeVisitor<LogicalPlan, CatalystPlanContext> planVisitor;
    
    public CatalystExpressionVisitor(AbstractNodeVisitor<LogicalPlan, CatalystPlanContext> planVisitor) {
        this.planVisitor = planVisitor;    
    }

    public Expression analyze(UnresolvedExpression unresolved, CatalystPlanContext context) {
        return unresolved.accept(this, context);
    }

    /** This method is only for analyze the join condition expression */
    public Expression analyzeJoinCondition(UnresolvedExpression unresolved, CatalystPlanContext context) {
        return context.resolveJoinCondition(unresolved, this::analyze);
    }

    @Override
    public Expression visitLiteral(Literal node, CatalystPlanContext context) {
        return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Literal(
                translate(node.getValue(), node.getType()), translate(node.getType())));
    }

    /**
     * generic binary (And, Or, Xor , ...) arithmetic expression resolver
     *
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
        if (left.isPresent() && right.isPresent()) {
            return transformer.apply(left.get(), right.get());
        } else if (left.isPresent()) {
            return context.getNamedParseExpressions().push(left.get());
        } else if (right.isPresent()) {
            return context.getNamedParseExpressions().push(right.get());
        }
        return null;

    }

    @Override
    public Expression visitAnd(And node, CatalystPlanContext context) {
        return visitBinaryArithmetic(node,
                (left, right) -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.And(left, right)), context);
    }

    @Override
    public Expression visitOr(Or node, CatalystPlanContext context) {
        return visitBinaryArithmetic(node,
                (left, right) -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.Or(left, right)), context);
    }

    @Override
    public Expression visitXor(Xor node, CatalystPlanContext context) {
        return visitBinaryArithmetic(node,
                (left, right) -> context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.BitwiseXor(left, right)), context);
    }

    @Override
    public Expression visitNot(Not node, CatalystPlanContext context) {
        node.getExpression().accept(this, context);
        Optional<Expression> arg = context.popNamedParseExpressions();
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
        Expression aggregator = AggregatorTransformer.aggregator(node, arg);
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
        // When the qualified name is part of join condition, for example: table1.id = table2.id
        // findRelation(context.traversalContext() only returns relation table1 which cause table2.id fail to resolve
        if (context.isResolvingJoinCondition()) {
            return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
        }
        List<UnresolvedRelation> relation = findRelation(context.traversalContext());
        if (!relation.isEmpty()) {
            Optional<QualifiedName> resolveField = resolveField(relation, node, context.getRelations());
            return resolveField.map(qualifiedName -> context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(qualifiedName.getParts()))))
                    .orElse(resolveQualifiedNameWithSubqueryAlias(node, context));
        }
        return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
    }

    /**
     * Resolve the qualified name with subquery alias: <br/>
     * - subqueryAlias1.joinKey = subqueryAlias2.joinKey <br/>
     * - tableName1.joinKey = subqueryAlias2.joinKey <br/>
     * - subqueryAlias1.joinKey = tableName2.joinKey <br/>
     */
    private Expression resolveQualifiedNameWithSubqueryAlias(QualifiedName node, CatalystPlanContext context) {
        if (node.getPrefix().isPresent() &&
                context.traversalContext().peek() instanceof org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias) {
            if (context.getSubqueryAlias().stream().map(p -> (org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias) p)
                    .anyMatch(a -> a.alias().equalsIgnoreCase(node.getPrefix().get().toString()))) {
                return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
            } else if (context.getRelations().stream().map(p -> (UnresolvedRelation) p)
                    .anyMatch(a -> a.tableName().equalsIgnoreCase(node.getPrefix().get().toString()))) {
                return context.getNamedParseExpressions().push(UnresolvedAttribute$.MODULE$.apply(seq(node.getParts())));
            }
        }
        return null;
    }

    @Override
    public Expression visitCorrelationMapping(FieldsMapping node, CatalystPlanContext context) {
        return node.getChild().stream().map(expression ->
                visitCompare((Compare) expression, context)
        ).reduce(org.apache.spark.sql.catalyst.expressions.And::new).orElse(null);
    }

    @Override
    public Expression visitAllFields(AllFields node, CatalystPlanContext context) {
        context.getNamedParseExpressions().push(UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()));
        return context.getNamedParseExpressions().peek();
    }

    @Override
    public Expression visitAlias(Alias node, CatalystPlanContext context) {
        node.getDelegated().accept(this, context);
        Expression arg = context.popNamedParseExpressions().get();
        return context.getNamedParseExpressions().push(
                org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(arg,
                        node.getName(),
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
        Expression function = BuiltinFunctionTransformer.builtinFunction(node, arguments);
        return context.getNamedParseExpressions().push(function);
    }

    @Override
    public Expression visitIsEmpty(IsEmpty node, CatalystPlanContext context) {
        Stack<Expression> namedParseExpressions = new Stack<>();
        namedParseExpressions.addAll(context.getNamedParseExpressions());
        Expression expression = visitCase(node.getCaseValue(), context);
        namedParseExpressions.add(expression);
        context.setNamedParseExpressions(namedParseExpressions);
        return expression;
    }

    @Override
    public Expression visitFillNull(FillNull fillNull, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : FillNull");
    }

    @Override
    public Expression visitInterval(Interval node, CatalystPlanContext context) {
        node.getValue().accept(this, context);
        Expression value = context.getNamedParseExpressions().pop();
        Expression[] intervalArgs = createIntervalArgs(node.getUnit(), value);
        Expression interval = MakeInterval$.MODULE$.apply(
                intervalArgs[0], intervalArgs[1], intervalArgs[2], intervalArgs[3],
                intervalArgs[4], intervalArgs[5], intervalArgs[6], true);
        return context.getNamedParseExpressions().push(interval);
    }

    @Override
    public Expression visitDedupe(Dedupe node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Dedupe");
    }

    @Override
    public Expression visitIn(In node, CatalystPlanContext context) {
        node.getField().accept(this, context);
        Expression value = context.popNamedParseExpressions().get();
        List<Expression> list = node.getValueList().stream().map( expression -> {
            expression.accept(this, context);
            return context.popNamedParseExpressions().get();
        }).collect(Collectors.toList());
        return context.getNamedParseExpressions().push(In$.MODULE$.apply(value, seq(list)));
    }

    @Override
    public Expression visitKmeans(Kmeans node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : Kmeans");
    }

    @Override
    public Expression visitCase(Case node, CatalystPlanContext context) {
        Stack<Expression> initialNameExpressions = new Stack<>();
        initialNameExpressions.addAll(context.getNamedParseExpressions());
        analyze(node.getElseClause(), context);
        Expression elseValue = context.getNamedParseExpressions().pop();
        List<Tuple2<Expression, Expression>> whens = new ArrayList<>();
        for (When when : node.getWhenClauses()) {
            if (node.getCaseValue() == null) {
                whens.add(
                        new Tuple2<>(
                                analyze(when.getCondition(), context),
                                analyze(when.getResult(), context)
                        )
                );
            } else {
                // Merge case value and condition (compare value) into a single equal condition
                Compare compare = new Compare(EQUAL.getName().getFunctionName(), node.getCaseValue(), when.getCondition());
                whens.add(
                        new Tuple2<>(
                                analyze(compare, context), analyze(when.getResult(), context)
                        )
                );
            }
            context.resetNamedParseExpressions();
        }
        context.setNamedParseExpressions(initialNameExpressions);
        return context.getNamedParseExpressions().push(new CaseWhen(seq(whens), Option.apply(elseValue)));
    }

    @Override
    public Expression visitRareTopN(RareTopN node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : RareTopN");
    }

    @Override
    public Expression visitWindowFunction(WindowFunction node, CatalystPlanContext context) {
        throw new IllegalStateException("Not Supported operation : WindowFunction");
    }

    @Override
    public Expression visitInSubquery(InSubquery node, CatalystPlanContext outerContext) {
        CatalystPlanContext innerContext = new CatalystPlanContext();
        innerContext.withSparkSession(outerContext.getSparkSession());
        visitExpressionList(node.getChild(), innerContext);
        Seq<Expression> values = innerContext.retainAllNamedParseExpressions(p -> p);
        UnresolvedPlan outerPlan = node.getQuery();
        LogicalPlan subSearch = outerPlan.accept(planVisitor, innerContext);
        Expression inSubQuery = InSubquery$.MODULE$.apply(
                values,
                ListQuery$.MODULE$.apply(
                        subSearch,
                        seq(new java.util.ArrayList<Expression>()),
                        NamedExpression.newExprId(),
                        -1,
                        seq(new java.util.ArrayList<Expression>()),
                        Option.empty()));
        return outerContext.getNamedParseExpressions().push(inSubQuery);
    }

    @Override
    public Expression visitScalarSubquery(ScalarSubquery node, CatalystPlanContext context) {
        CatalystPlanContext innerContext = new CatalystPlanContext();
        innerContext.withSparkSession(context.getSparkSession());
        UnresolvedPlan outerPlan = node.getQuery();
        LogicalPlan subSearch = outerPlan.accept(planVisitor, innerContext);
        Expression scalarSubQuery = ScalarSubquery$.MODULE$.apply(
                subSearch,
                seq(new java.util.ArrayList<Expression>()),
                NamedExpression.newExprId(),
                seq(new java.util.ArrayList<Expression>()),
                Option.empty(),
                Option.empty());
        return context.getNamedParseExpressions().push(scalarSubQuery);
    }

    @Override
    public Expression visitExistsSubquery(ExistsSubquery node, CatalystPlanContext context) {
        CatalystPlanContext innerContext = new CatalystPlanContext();
        innerContext.withSparkSession(context.getSparkSession());
        UnresolvedPlan outerPlan = node.getQuery();
        LogicalPlan subSearch = outerPlan.accept(planVisitor, innerContext);
        Expression existsSubQuery = Exists$.MODULE$.apply(
                subSearch,
                seq(new java.util.ArrayList<Expression>()),
                NamedExpression.newExprId(),
                seq(new java.util.ArrayList<Expression>()),
                Option.empty());
        return context.getNamedParseExpressions().push(existsSubQuery);
    }

    @Override
    public Expression visitBetween(Between node, CatalystPlanContext context) {
        Expression value = analyze(node.getValue(), context);
        Expression lower = analyze(node.getLowerBound(), context);
        Expression upper = analyze(node.getUpperBound(), context);
        context.resetNamedParseExpressions();
        return context.getNamedParseExpressions().push(new org.apache.spark.sql.catalyst.expressions.And(new GreaterThanOrEqual(value, lower), new LessThanOrEqual(value, upper)));
    }

    @Override
    public Expression visitCidr(org.opensearch.sql.ast.expression.Cidr node, CatalystPlanContext context) {
        analyze(node.getIpAddress(), context);
        Expression ipAddressExpression = context.getNamedParseExpressions().pop();
        analyze(node.getCidrBlock(), context);
        Expression cidrBlockExpression = context.getNamedParseExpressions().pop();
        return context.getNamedParseExpressions().push(SerializableUdf.visit(CIDR, of(ipAddressExpression,cidrBlockExpression)));
    }

    @Override
    public Expression visitLambdaFunction(LambdaFunction node, CatalystPlanContext context) {
        PartialFunction<Expression, Expression> transformer = JavaToScalaTransformer.toPartialFunction(
            expr -> expr instanceof UnresolvedAttribute,
            expr -> {
                UnresolvedAttribute attr = (UnresolvedAttribute) expr;
                return new UnresolvedNamedLambdaVariable(attr.nameParts());
            }
        );
        Expression functionResult = node.getFunction().accept(this, context).transformUp(transformer);
        context.popNamedParseExpressions();
        List<NamedExpression> argsResult = node.getFuncArgs().stream()
            .map(arg -> UnresolvedNamedLambdaVariable$.MODULE$.apply(seq(arg.getParts())))
            .collect(Collectors.toList());
        return context.getNamedParseExpressions().push(LambdaFunction$.MODULE$.apply(functionResult, seq(argsResult), false));
    }

    @Override
    public Expression visitCast(Cast node, CatalystPlanContext context) {
        analyze(node.getExpression(), context);
        Optional<Expression> ret = context.popNamedParseExpressions();
        if (ret.isEmpty()) {
            throw new UnsupportedOperationException(
                String.format("Invalid use of expression %s", node.getExpression()));
        }
        return context.getNamedParseExpressions().push(Cast$.MODULE$.apply(ret.get(), translate(node.getDataType()), false));
    }

    private List<Expression> visitExpressionList(List<UnresolvedExpression> expressionList, CatalystPlanContext context) {
        return expressionList.isEmpty()
                ? emptyList()
                : expressionList.stream().map(field -> analyze(field, context))
                .collect(Collectors.toList());
    }
}

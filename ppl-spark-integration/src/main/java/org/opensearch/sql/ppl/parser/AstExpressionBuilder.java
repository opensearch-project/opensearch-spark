/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ppl.utils.ArgumentFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POSITION;


/**
 * Class of building AST Expression nodes.
 */
public class AstExpressionBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedExpression> {

    private static final int DEFAULT_TAKE_FUNCTION_SIZE_VALUE = 10;

    /**
     * The function name mapping between fronted and core engine.
     */
    private static Map<String, String> FUNCTION_NAME_MAPPING =
            new ImmutableMap.Builder<String, String>()
                    .put("isnull", IS_NULL.getName().getFunctionName())
                    .put("isnotnull", IS_NOT_NULL.getName().getFunctionName())
                    .build();

    /**
     * Eval clause.
     */
    @Override
    public UnresolvedExpression visitEvalClause(OpenSearchPPLParser.EvalClauseContext ctx) {
        return new Let((Field) visit(ctx.fieldExpression()), visit(ctx.expression()));
    }

    /**
     * Logical expression excluding boolean, comparison.
     */
    @Override
    public UnresolvedExpression visitLogicalNot(OpenSearchPPLParser.LogicalNotContext ctx) {
        return new Not(visit(ctx.logicalExpression()));
    }

    @Override
    public UnresolvedExpression visitLogicalOr(OpenSearchPPLParser.LogicalOrContext ctx) {
        return new Or(visit(ctx.left), visit(ctx.right));
    }

    @Override
    public UnresolvedExpression visitLogicalAnd(OpenSearchPPLParser.LogicalAndContext ctx) {
        return new And(visit(ctx.left), visit(ctx.right));
    }

    @Override
    public UnresolvedExpression visitLogicalXor(OpenSearchPPLParser.LogicalXorContext ctx) {
        return new Xor(visit(ctx.left), visit(ctx.right));
    }

    /**
     * Comparison expression.
     */
    @Override
    public UnresolvedExpression visitCompareExpr(OpenSearchPPLParser.CompareExprContext ctx) {
        return new Compare(ctx.comparisonOperator().getText(), visit(ctx.left), visit(ctx.right));
    }

    /**
     * Value Expression.
     */
    @Override
    public UnresolvedExpression visitBinaryArithmetic(OpenSearchPPLParser.BinaryArithmeticContext ctx) {
        return new Function(
                ctx.binaryOperator.getText(), Arrays.asList(visit(ctx.left), visit(ctx.right)));
    }

    @Override
    public UnresolvedExpression visitParentheticValueExpr(OpenSearchPPLParser.ParentheticValueExprContext ctx) {
        return visit(ctx.valueExpression()); // Discard parenthesis around
    }

    /**
     * Field expression.
     */
    @Override
    public UnresolvedExpression visitFieldExpression(OpenSearchPPLParser.FieldExpressionContext ctx) {
        return new Field((QualifiedName) visit(ctx.qualifiedName()));
    }

    @Override
    public UnresolvedExpression visitWcFieldExpression(OpenSearchPPLParser.WcFieldExpressionContext ctx) {
        return new Field((QualifiedName) visit(ctx.wcQualifiedName()));
    }

    @Override
    public UnresolvedExpression visitSortField(OpenSearchPPLParser.SortFieldContext ctx) {
        return new Field(
                visit(ctx.sortFieldExpression().fieldExpression().qualifiedName()),
                ArgumentFactory.getArgumentList(ctx));
    }

    /**
     * Aggregation function.
     */
    @Override
    public UnresolvedExpression visitStatsFunctionCall(OpenSearchPPLParser.StatsFunctionCallContext ctx) {
        return new AggregateFunction(ctx.statsFunctionName().getText(), visit(ctx.valueExpression()));
    }

    @Override
    public UnresolvedExpression visitCountAllFunctionCall(OpenSearchPPLParser.CountAllFunctionCallContext ctx) {
        return new AggregateFunction("count", AllFields.of());
    }

    @Override
    public UnresolvedExpression visitDistinctCountFunctionCall(OpenSearchPPLParser.DistinctCountFunctionCallContext ctx) {
        return new AggregateFunction("count", visit(ctx.valueExpression()), true);
    }

    @Override
    public UnresolvedExpression visitPercentileAggFunction(OpenSearchPPLParser.PercentileAggFunctionContext ctx) {
        return new AggregateFunction(
                ctx.PERCENTILE().getText(),
                visit(ctx.aggField),
                Collections.singletonList(new Argument("rank", (Literal) visit(ctx.value))));
    }

    @Override
    public UnresolvedExpression visitTakeAggFunctionCall(
            OpenSearchPPLParser.TakeAggFunctionCallContext ctx) {
        ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
        builder.add(
                new UnresolvedArgument(
                        "size",
                        ctx.takeAggFunction().size != null
                                ? visit(ctx.takeAggFunction().size)
                                : new Literal(DEFAULT_TAKE_FUNCTION_SIZE_VALUE, DataType.INTEGER)));
        return new AggregateFunction(
                "take", visit(ctx.takeAggFunction().fieldExpression()), builder.build());
    }

    /**
     * Eval function.
     */
    @Override
    public UnresolvedExpression visitBooleanFunctionCall(OpenSearchPPLParser.BooleanFunctionCallContext ctx) {
        final String functionName = ctx.conditionFunctionBase().getText();
        return buildFunction(
                FUNCTION_NAME_MAPPING.getOrDefault(functionName, functionName),
                ctx.functionArgs().functionArg());
    }

    /**
     * Eval function.
     */
    @Override
    public UnresolvedExpression visitEvalFunctionCall(OpenSearchPPLParser.EvalFunctionCallContext ctx) {
        return buildFunction(ctx.evalFunctionName().getText(), ctx.functionArgs().functionArg());
    }

    @Override
    public UnresolvedExpression visitConvertedDataType(OpenSearchPPLParser.ConvertedDataTypeContext ctx) {
        return new Literal(ctx.getText(), DataType.STRING);
    }

    private Function buildFunction(
            String functionName, List<OpenSearchPPLParser.FunctionArgContext> args) {
        return new Function(
                functionName, args.stream().map(this::visitFunctionArg).collect(Collectors.toList()));
    }

    public AstExpressionBuilder() {
    }

    @Override
    public UnresolvedExpression visitMultiFieldRelevanceFunction(
            OpenSearchPPLParser.MultiFieldRelevanceFunctionContext ctx) {
        return new Function(
                ctx.multiFieldRelevanceFunctionName().getText().toLowerCase(),
                multiFieldRelevanceArguments(ctx));
    }

    @Override
    public UnresolvedExpression visitTableSource(OpenSearchPPLParser.TableSourceContext ctx) {
        if (ctx.getChild(0) instanceof OpenSearchPPLParser.IdentsAsTableQualifiedNameContext) {
            return visitIdentsAsTableQualifiedName((OpenSearchPPLParser.IdentsAsTableQualifiedNameContext) ctx.getChild(0));
        } else {
            return visitIdentifiers(Arrays.asList(ctx));
        }
    }

    @Override
    public UnresolvedExpression visitPositionFunction(
            OpenSearchPPLParser.PositionFunctionContext ctx) {
        return new Function(
                POSITION.getName().getFunctionName(),
                Arrays.asList(visitFunctionArg(ctx.functionArg(0)), visitFunctionArg(ctx.functionArg(1))));
    }

    @Override
    public UnresolvedExpression visitExtractFunctionCall(
            OpenSearchPPLParser.ExtractFunctionCallContext ctx) {
        return new Function(
                ctx.extractFunction().EXTRACT().toString(), getExtractFunctionArguments(ctx));
    }

    private List<UnresolvedExpression> getExtractFunctionArguments(
            OpenSearchPPLParser.ExtractFunctionCallContext ctx) {
        List<UnresolvedExpression> args =
                Arrays.asList(
                        new Literal(ctx.extractFunction().datetimePart().getText(), DataType.STRING),
                        visitFunctionArg(ctx.extractFunction().functionArg()));
        return args;
    }

    @Override
    public UnresolvedExpression visitGetFormatFunctionCall(
            OpenSearchPPLParser.GetFormatFunctionCallContext ctx) {
        return new Function(
                ctx.getFormatFunction().GET_FORMAT().toString(), getFormatFunctionArguments(ctx));
    }

    private List<UnresolvedExpression> getFormatFunctionArguments(
            OpenSearchPPLParser.GetFormatFunctionCallContext ctx) {
        List<UnresolvedExpression> args =
                Arrays.asList(
                        new Literal(ctx.getFormatFunction().getFormatType().getText(), DataType.STRING),
                        visitFunctionArg(ctx.getFormatFunction().functionArg()));
        return args;
    }

    @Override
    public UnresolvedExpression visitTimestampFunctionCall(
            OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
        return new Function(
                ctx.timestampFunction().timestampFunctionName().getText(), timestampFunctionArguments(ctx));
    }

    private List<UnresolvedExpression> timestampFunctionArguments(
            OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
        List<UnresolvedExpression> args =
                Arrays.asList(
                        new Literal(ctx.timestampFunction().simpleDateTimePart().getText(), DataType.STRING),
                        visitFunctionArg(ctx.timestampFunction().firstArg),
                        visitFunctionArg(ctx.timestampFunction().secondArg));
        return args;
    }

    /**
     * Literal and value.
     */
    @Override
    public UnresolvedExpression visitIdentsAsQualifiedName(OpenSearchPPLParser.IdentsAsQualifiedNameContext ctx) {
        return visitIdentifiers(ctx.ident());
    }

    @Override
    public UnresolvedExpression visitIdentsAsTableQualifiedName(
            OpenSearchPPLParser.IdentsAsTableQualifiedNameContext ctx) {
        return visitIdentifiers(
                Stream.concat(Stream.of(ctx.tableIdent()), ctx.ident().stream())
                        .collect(Collectors.toList()));
    }

    @Override
    public UnresolvedExpression visitIdentsAsWildcardQualifiedName(
            OpenSearchPPLParser.IdentsAsWildcardQualifiedNameContext ctx) {
        return visitIdentifiers(ctx.wildcard());
    }

    @Override
    public UnresolvedExpression visitIntervalLiteral(OpenSearchPPLParser.IntervalLiteralContext ctx) {
        return new Interval(
                visit(ctx.valueExpression()), IntervalUnit.of(ctx.intervalUnit().getText()));
    }

    @Override
    public UnresolvedExpression visitStringLiteral(OpenSearchPPLParser.StringLiteralContext ctx) {
        return new Literal(ctx.getText(), DataType.STRING);
    }

    @Override
    public UnresolvedExpression visitIntegerLiteral(OpenSearchPPLParser.IntegerLiteralContext ctx) {
        long number = Long.parseLong(ctx.getText());
        if (Integer.MIN_VALUE <= number && number <= Integer.MAX_VALUE) {
            return new Literal((int) number, DataType.INTEGER);
        }
        return new Literal(number, DataType.LONG);
    }

    @Override
    public UnresolvedExpression visitDecimalLiteral(OpenSearchPPLParser.DecimalLiteralContext ctx) {
        return new Literal(Double.valueOf(ctx.getText()), DataType.DOUBLE);
    }

    @Override
    public UnresolvedExpression visitBooleanLiteral(OpenSearchPPLParser.BooleanLiteralContext ctx) {
        return new Literal(Boolean.valueOf(ctx.getText()), DataType.BOOLEAN);
    }

    @Override
    public UnresolvedExpression visitBySpanClause(OpenSearchPPLParser.BySpanClauseContext ctx) {
        String name = ctx.spanClause().getText();
        return ctx.alias != null
                ? new Alias(
                name, visit(ctx.spanClause()), ctx.alias.getText())
                : new Alias(name, visit(ctx.spanClause()));
    }

    @Override
    public UnresolvedExpression visitSpanClause(OpenSearchPPLParser.SpanClauseContext ctx) {
        String unit = ctx.unit != null ? ctx.unit.getText() : "";
        return new Span(visit(ctx.fieldExpression()), visit(ctx.value), SpanUnit.of(unit));
    }

    private QualifiedName visitIdentifiers(List<? extends ParserRuleContext> ctx) {
        return new QualifiedName(
                ctx.stream()
                        .map(RuleContext::getText)
                        .collect(Collectors.toList()));
    }

    private List<UnresolvedExpression> singleFieldRelevanceArguments(
            OpenSearchPPLParser.SingleFieldRelevanceFunctionContext ctx) {
        // all the arguments are defaulted to string values
        // to skip environment resolving and function signature resolving
        ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
        builder.add(
                new UnresolvedArgument(
                        "field", new QualifiedName(ctx.field.getText())));
        builder.add(
                new UnresolvedArgument(
                        "query", new Literal(ctx.query.getText(), DataType.STRING)));
        ctx.relevanceArg()
                .forEach(
                        v ->
                                builder.add(
                                        new UnresolvedArgument(
                                                v.relevanceArgName().getText().toLowerCase(),
                                                new Literal(
                                                        v.relevanceArgValue().getText(),
                                                        DataType.STRING))));
        return builder.build();
    }

    private List<UnresolvedExpression> multiFieldRelevanceArguments(
            OpenSearchPPLParser.MultiFieldRelevanceFunctionContext ctx) {
        throw new RuntimeException("ML Command is not supported ");

    }
}

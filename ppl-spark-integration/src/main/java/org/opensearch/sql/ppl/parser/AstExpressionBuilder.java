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
import org.opensearch.sql.ast.expression.AttributeList;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Cidr;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.IntervalUnit;
import org.opensearch.sql.ast.expression.IsEmpty;
import org.opensearch.sql.ast.expression.LambdaFunction;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.utils.ArgumentFactory;
import org.opensearch.sql.ppl.utils.GeoIpCatalystLogicalPlanTranslator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.EQUAL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NOT_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.IS_NULL;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.LENGTH;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.POSITION;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.TRIM;


/**
 * Class of building AST Expression nodes.
 */
public class AstExpressionBuilder extends OpenSearchPPLParserBaseVisitor<UnresolvedExpression> {

    /**
     * The function name mapping between fronted and core engine.
     */
    private static final Map<String, String> FUNCTION_NAME_MAPPING =
            new ImmutableMap.Builder<String, String>()
                    .put("isnull", IS_NULL.getName().getFunctionName())
                    .put("isnotnull", IS_NOT_NULL.getName().getFunctionName())
                    .put("ispresent", IS_NOT_NULL.getName().getFunctionName())
                    .build();
    private AstBuilder astBuilder;

    public AstExpressionBuilder(AstBuilder astBuilder) {
        this.astBuilder = astBuilder;
    }
    
    @Override
    public UnresolvedExpression visitMappingCompareExpr(OpenSearchPPLParser.MappingCompareExprContext ctx) {
        return new Compare(ctx.comparisonOperator().getText(), visit(ctx.left), visit(ctx.right));
    }

    @Override
    public UnresolvedExpression visitMappingList(OpenSearchPPLParser.MappingListContext ctx) {
        return super.visitMappingList(ctx);
    }

    /**
     * Lookup.
     */
    @Override
    public UnresolvedExpression visitLookupPair(OpenSearchPPLParser.LookupPairContext ctx) {
        Field inputField = (Field) visitFieldExpression(ctx.inputField);
        if (ctx.AS() != null) {
            Field outputField = (Field) visitFieldExpression(ctx.outputField);
            return new And(new Alias(ctx.outputField.getText(), inputField), outputField);
        } else {
            return new And(new Alias(ctx.inputField.getText(), inputField), inputField);
        }
    }

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
    public UnresolvedExpression visitParentheticLogicalExpr(OpenSearchPPLParser.ParentheticLogicalExprContext ctx) {
        return visit(ctx.logicalExpression()); // Discard parenthesis around
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

        // TODO #963: Implement 'num', 'str', and 'ip' sort syntax
        return new Field((QualifiedName)
                visit(ctx.sortFieldExpression().fieldExpression().qualifiedName()),
                ArgumentFactory.getArgumentList(ctx));
    }

    @Override
    public UnresolvedExpression visitFieldsummaryIncludeFields(OpenSearchPPLParser.FieldsummaryIncludeFieldsContext ctx) {
        List<UnresolvedExpression> list = ctx.fieldList().fieldExpression().stream()
                .map(this::visitFieldExpression)
                .collect(Collectors.toList());
        return new AttributeList(list);
    }

    @Override
    public UnresolvedExpression visitFieldsummaryNulls(OpenSearchPPLParser.FieldsummaryNullsContext ctx) {
        return new Argument("NULLS",(Literal)visitBooleanLiteral(ctx.booleanLiteral()));
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
        String funcName = ctx.DISTINCT_COUNT_APPROX()!=null ? "approx_count_distinct" :"count";  
        return new AggregateFunction(funcName, visit(ctx.valueExpression()), true);
    }

    @Override
    public UnresolvedExpression visitPercentileFunctionCall(OpenSearchPPLParser.PercentileFunctionCallContext ctx) {
        return new AggregateFunction(
                ctx.percentileFunctionName.getText(),
                visit(ctx.valueExpression()),
                Collections.singletonList(new Argument("percent", (Literal) visit(ctx.percent))));
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

    @Override
    public UnresolvedExpression visitCaseExpr(OpenSearchPPLParser.CaseExprContext ctx) {
        List<When> whens = IntStream.range(0, ctx.caseFunction().logicalExpression().size())
                .mapToObj(index -> {
                    OpenSearchPPLParser.LogicalExpressionContext logicalExpressionContext = ctx.caseFunction().logicalExpression(index);
                    OpenSearchPPLParser.ValueExpressionContext valueExpressionContext = ctx.caseFunction().valueExpression(index);
                    UnresolvedExpression condition = visit(logicalExpressionContext);
                    UnresolvedExpression result = visit(valueExpressionContext);
                    return new When(condition, result);
                })
                .collect(Collectors.toList());
        UnresolvedExpression elseValue = new Literal(null, DataType.NULL);
        if (ctx.caseFunction().valueExpression().size() > ctx.caseFunction().logicalExpression().size()) {
            // else value is present
            elseValue = visit(ctx.caseFunction().valueExpression(ctx.caseFunction().valueExpression().size() - 1));
        }
        return new Case(new Literal(true, DataType.BOOLEAN), whens, elseValue);
    }

    @Override
    public UnresolvedExpression visitIsEmptyExpression(OpenSearchPPLParser.IsEmptyExpressionContext ctx) {
        Function trimFunction = new Function(TRIM.getName().getFunctionName(), Collections.singletonList(this.visitFunctionArg(ctx.functionArg())));
        Function lengthFunction = new Function(LENGTH.getName().getFunctionName(), Collections.singletonList(trimFunction));
        Compare lengthEqualsZero = new Compare(EQUAL.getName().getFunctionName(), lengthFunction, new Literal(0, DataType.INTEGER));
        Literal whenCompareValue = new Literal(0, DataType.INTEGER);
        Literal isEmptyFalse = new Literal(false, DataType.BOOLEAN);
        Literal isEmptyTrue = new Literal(true, DataType.BOOLEAN);
        When when = new When(whenCompareValue, isEmptyTrue);
        Case caseWhen = new Case(lengthFunction, Collections.singletonList(when), isEmptyFalse);
        return new IsEmpty(caseWhen);
    }

    /**
     * Eval function.
     */
    @Override
    public UnresolvedExpression visitEvalFunctionCall(OpenSearchPPLParser.EvalFunctionCallContext ctx) {
        return buildFunction(ctx.evalFunctionName().getText(), ctx.functionArgs().functionArg());
    }

    @Override public UnresolvedExpression visitDataTypeFunctionCall(OpenSearchPPLParser.DataTypeFunctionCallContext ctx) {
        // TODO: for long term consideration, needs to implement DataTypeBuilder/Visitor to parse all data types
        return new Cast(this.visit(ctx.expression()), DataType.fromString(ctx.convertedDataType().getText()));
    }

    @Override
    public UnresolvedExpression visitBetween(OpenSearchPPLParser.BetweenContext ctx) {
        UnresolvedExpression betweenExpr = new Between(visit(ctx.expr1),visit(ctx.expr2),visit(ctx.expr3));
        return ctx.NOT() != null ? new Not(betweenExpr) : betweenExpr;
    }

    private Function buildFunction(
            String functionName, List<OpenSearchPPLParser.FunctionArgContext> args) {
        return new Function(
                functionName, args.stream().map(this::visitFunctionArg).collect(Collectors.toList()));
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
            return visitIdentifiers(List.of(ctx));
        }
    }

    @Override
    public UnresolvedExpression visitPositionFunction(
            OpenSearchPPLParser.PositionFunctionContext ctx) {
        return new Function(
                POSITION.getName().getFunctionName(),
                Arrays.asList(visitFunctionArg(ctx.functionArg(0)), visitFunctionArg(ctx.functionArg(1))));
    }

    /**
     * Literal and value.
     */
    @Override
    public UnresolvedExpression visitIdentsAsQualifiedName(OpenSearchPPLParser.IdentsAsQualifiedNameContext ctx) {
        return visitIdentifiers(ctx.ident());
    }

    @Override
    public UnresolvedExpression visitIdentsAsQualifiedNameSeq(OpenSearchPPLParser.IdentsAsQualifiedNameSeqContext ctx) {
        return new AttributeList(ctx.qualifiedName().stream().map(this::visit).collect(Collectors.toList()));
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
        return new Literal(StringUtils.unquoteText(ctx.getText()), DataType.STRING);
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
                ? new Alias(StringUtils.unquoteIdentifier(ctx.alias.getText()), visit(ctx.spanClause()))
                : new Alias(name, visit(ctx.spanClause()));
    }

    @Override
    public UnresolvedExpression visitSpanClause(OpenSearchPPLParser.SpanClauseContext ctx) {
        String unit = ctx.unit != null ? ctx.unit.getText() : "";
        return new Span(visit(ctx.fieldExpression()), visit(ctx.value), SpanUnit.of(unit));
    }

    @Override
    public UnresolvedExpression visitLeftHint(OpenSearchPPLParser.LeftHintContext ctx) {
        return new EqualTo(new Literal(ctx.leftHintKey.getText(), DataType.STRING), visit(ctx.leftHintValue));
    }

    @Override
    public UnresolvedExpression visitRightHint(OpenSearchPPLParser.RightHintContext ctx) {
        return new EqualTo(new Literal(ctx.rightHintKey.getText(), DataType.STRING), visit(ctx.rightHintValue));
    }

    @Override
    public UnresolvedExpression visitInSubqueryExpr(OpenSearchPPLParser.InSubqueryExprContext ctx) {
        UnresolvedExpression expr = new InSubquery(
                ctx.valueExpressionList().valueExpression().stream()
                        .map(this::visit).collect(Collectors.toList()),
                astBuilder.visitSubSearch(ctx.subSearch()));
        return ctx.NOT() != null ? new Not(expr) : expr;
    }

    @Override
    public UnresolvedExpression visitScalarSubqueryExpr(OpenSearchPPLParser.ScalarSubqueryExprContext ctx) {
        return new ScalarSubquery(astBuilder.visitSubSearch(ctx.subSearch()));
    }

    @Override
    public UnresolvedExpression visitExistsSubqueryExpr(OpenSearchPPLParser.ExistsSubqueryExprContext ctx) {
        return new ExistsSubquery(astBuilder.visitSubSearch(ctx.subSearch()));
    }

    @Override
    public UnresolvedExpression visitInExpr(OpenSearchPPLParser.InExprContext ctx) {
        UnresolvedExpression expr = new In(visit(ctx.valueExpression()),
            ctx.valueList().literalValue().stream().map(this::visit).collect(Collectors.toList()));
        return ctx.NOT() != null ? new Not(expr) : expr;
    }

    @Override
    public UnresolvedExpression visitCidrMatchFunctionCall(OpenSearchPPLParser.CidrMatchFunctionCallContext ctx) {
        return new Cidr(visit(ctx.ipAddress), visit(ctx.cidrBlock));
    }

    @Override
    public UnresolvedExpression visitTimestampFunctionCall(
            OpenSearchPPLParser.TimestampFunctionCallContext ctx) {
        return new Function(
            ctx.timestampFunction().timestampFunctionName().getText(), timestampFunctionArguments(ctx));
    }

    @Override
    public UnresolvedExpression visitLambda(OpenSearchPPLParser.LambdaContext ctx) {

        List<QualifiedName> arguments = ctx.ident().stream().map(x -> this.visitIdentifiers(Collections.singletonList(x))).collect(
            Collectors.toList());
        UnresolvedExpression function = visitExpression(ctx.expression());
        return new LambdaFunction(function, arguments);
    }

    @Override
    public UnresolvedExpression visitGeoIpPropertyList(OpenSearchPPLParser.GeoIpPropertyListContext ctx) {
        ImmutableList.Builder<UnresolvedExpression> properties = ImmutableList.builder();
        if (ctx != null) {
            for (OpenSearchPPLParser.GeoIpPropertyContext property : ctx.geoIpProperty()) {
                String propertyName = property.getText().toUpperCase();
                GeoIpCatalystLogicalPlanTranslator.validateGeoIpProperty(propertyName);
                properties.add(new Literal(propertyName, DataType.STRING));
            }
        }

        return new AttributeList(properties.build());
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

    private QualifiedName visitIdentifiers(List<? extends ParserRuleContext> ctx) {
        return new QualifiedName(
                ctx.stream()
                        .map(RuleContext::getText)
                        .map(StringUtils::unquoteIdentifier)
                        .collect(Collectors.toList()));
    }

    private List<UnresolvedExpression> singleFieldRelevanceArguments(
            OpenSearchPPLParser.SingleFieldRelevanceFunctionContext ctx) {
        // all the arguments are defaulted to string values
        // to skip environment resolving and function signature resolving
        ImmutableList.Builder<UnresolvedExpression> builder = ImmutableList.builder();
        builder.add(
                new UnresolvedArgument(
                        "field", new QualifiedName(StringUtils.unquoteText(ctx.field.getText()))));
        builder.add(
                new UnresolvedArgument(
                        "query", new Literal(StringUtils.unquoteText(ctx.query.getText()), DataType.STRING)));
        ctx.relevanceArg()
                .forEach(
                        v ->
                                builder.add(
                                        new UnresolvedArgument(
                                                v.relevanceArgName().getText().toLowerCase(),
                                                new Literal(
                                                        StringUtils.unquoteText(v.relevanceArgValue().getText()),
                                                        DataType.STRING))));
        return builder.build();
    }

    private List<UnresolvedExpression> multiFieldRelevanceArguments(
            OpenSearchPPLParser.MultiFieldRelevanceFunctionContext ctx) {
        throw new RuntimeException("ML Command is not supported ");

    }
}

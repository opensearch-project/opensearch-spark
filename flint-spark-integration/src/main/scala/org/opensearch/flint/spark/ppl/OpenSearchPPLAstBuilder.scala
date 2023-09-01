/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.antlr.v4.runtime.tree.{ErrorNode, ParseTree, RuleNode, TerminalNode}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable
import org.opensearch.flint.spark.sql.{OpenSearchPPLParser, OpenSearchPPLParserBaseVisitor}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class OpenSearchPPLAstBuilder extends OpenSearchPPLParserBaseVisitor[LogicalPlan] {

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link #   visitChildren} on {@code ctx}.</p>
   */
  override def visitRoot(ctx: OpenSearchPPLParser.RootContext): LogicalPlan = super.visitRoot(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPplStatement(ctx: OpenSearchPPLParser.PplStatementContext): LogicalPlan = {
    println("visitPplStatement")
    UnresolvedTable(Seq("table"), "source=table ", None)
  }

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDmlStatement(ctx: OpenSearchPPLParser.DmlStatementContext): LogicalPlan = super.visitDmlStatement(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitQueryStatement(ctx: OpenSearchPPLParser.QueryStatementContext): LogicalPlan = super.visitQueryStatement(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPplCommands(ctx: OpenSearchPPLParser.PplCommandsContext): LogicalPlan = super.visitPplCommands(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitCommands(ctx: OpenSearchPPLParser.CommandsContext): LogicalPlan = super.visitCommands(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSearchFrom(ctx: OpenSearchPPLParser.SearchFromContext): LogicalPlan = super.visitSearchFrom(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSearchFromFilter(ctx: OpenSearchPPLParser.SearchFromFilterContext): LogicalPlan = super.visitSearchFromFilter(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSearchFilterFrom(ctx: OpenSearchPPLParser.SearchFilterFromContext): LogicalPlan = super.visitSearchFilterFrom(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDescribeCommand(ctx: OpenSearchPPLParser.DescribeCommandContext): LogicalPlan = super.visitDescribeCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitShowDataSourcesCommand(ctx: OpenSearchPPLParser.ShowDataSourcesCommandContext): LogicalPlan = super.visitShowDataSourcesCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitWhereCommand(ctx: OpenSearchPPLParser.WhereCommandContext): LogicalPlan = super.visitWhereCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitFieldsCommand(ctx: OpenSearchPPLParser.FieldsCommandContext): LogicalPlan = super.visitFieldsCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRenameCommand(ctx: OpenSearchPPLParser.RenameCommandContext): LogicalPlan = super.visitRenameCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitStatsCommand(ctx: OpenSearchPPLParser.StatsCommandContext): LogicalPlan = super.visitStatsCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDedupCommand(ctx: OpenSearchPPLParser.DedupCommandContext): LogicalPlan = super.visitDedupCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSortCommand(ctx: OpenSearchPPLParser.SortCommandContext): LogicalPlan = super.visitSortCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitEvalCommand(ctx: OpenSearchPPLParser.EvalCommandContext): LogicalPlan = super.visitEvalCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitHeadCommand(ctx: OpenSearchPPLParser.HeadCommandContext): LogicalPlan = super.visitHeadCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTopCommand(ctx: OpenSearchPPLParser.TopCommandContext): LogicalPlan = super.visitTopCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRareCommand(ctx: OpenSearchPPLParser.RareCommandContext): LogicalPlan = super.visitRareCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitGrokCommand(ctx: OpenSearchPPLParser.GrokCommandContext): LogicalPlan = super.visitGrokCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitParseCommand(ctx: OpenSearchPPLParser.ParseCommandContext): LogicalPlan = super.visitParseCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPatternsCommand(ctx: OpenSearchPPLParser.PatternsCommandContext): LogicalPlan = super.visitPatternsCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPatternsParameter(ctx: OpenSearchPPLParser.PatternsParameterContext): LogicalPlan = super.visitPatternsParameter(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPatternsMethod(ctx: OpenSearchPPLParser.PatternsMethodContext): LogicalPlan = super.visitPatternsMethod(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitKmeansCommand(ctx: OpenSearchPPLParser.KmeansCommandContext): LogicalPlan = super.visitKmeansCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitKmeansParameter(ctx: OpenSearchPPLParser.KmeansParameterContext): LogicalPlan = super.visitKmeansParameter(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitAdCommand(ctx: OpenSearchPPLParser.AdCommandContext): LogicalPlan = super.visitAdCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitAdParameter(ctx: OpenSearchPPLParser.AdParameterContext): LogicalPlan = super.visitAdParameter(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitMlCommand(ctx: OpenSearchPPLParser.MlCommandContext): LogicalPlan = super.visitMlCommand(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitMlArg(ctx: OpenSearchPPLParser.MlArgContext): LogicalPlan = super.visitMlArg(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitFromClause(ctx: OpenSearchPPLParser.FromClauseContext): LogicalPlan = super.visitFromClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTableSourceClause(ctx: OpenSearchPPLParser.TableSourceClauseContext): LogicalPlan = super.visitTableSourceClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRenameClasue(ctx: OpenSearchPPLParser.RenameClasueContext): LogicalPlan = super.visitRenameClasue(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitByClause(ctx: OpenSearchPPLParser.ByClauseContext): LogicalPlan = super.visitByClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitStatsByClause(ctx: OpenSearchPPLParser.StatsByClauseContext): LogicalPlan = super.visitStatsByClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitBySpanClause(ctx: OpenSearchPPLParser.BySpanClauseContext): LogicalPlan = super.visitBySpanClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSpanClause(ctx: OpenSearchPPLParser.SpanClauseContext): LogicalPlan = super.visitSpanClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSortbyClause(ctx: OpenSearchPPLParser.SortbyClauseContext): LogicalPlan = super.visitSortbyClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitEvalClause(ctx: OpenSearchPPLParser.EvalClauseContext): LogicalPlan = super.visitEvalClause(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitStatsAggTerm(ctx: OpenSearchPPLParser.StatsAggTermContext): LogicalPlan = super.visitStatsAggTerm(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitStatsFunctionCall(ctx: OpenSearchPPLParser.StatsFunctionCallContext): LogicalPlan = super.visitStatsFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitCountAllFunctionCall(ctx: OpenSearchPPLParser.CountAllFunctionCallContext): LogicalPlan = super.visitCountAllFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDistinctCountFunctionCall(ctx: OpenSearchPPLParser.DistinctCountFunctionCallContext): LogicalPlan = super.visitDistinctCountFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPercentileAggFunctionCall(ctx: OpenSearchPPLParser.PercentileAggFunctionCallContext): LogicalPlan = super.visitPercentileAggFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTakeAggFunctionCall(ctx: OpenSearchPPLParser.TakeAggFunctionCallContext): LogicalPlan = super.visitTakeAggFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitStatsFunctionName(ctx: OpenSearchPPLParser.StatsFunctionNameContext): LogicalPlan = super.visitStatsFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTakeAggFunction(ctx: OpenSearchPPLParser.TakeAggFunctionContext): LogicalPlan = super.visitTakeAggFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPercentileAggFunction(ctx: OpenSearchPPLParser.PercentileAggFunctionContext): LogicalPlan = super.visitPercentileAggFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitExpression(ctx: OpenSearchPPLParser.ExpressionContext): LogicalPlan = super.visitExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceExpr(ctx: OpenSearchPPLParser.RelevanceExprContext): LogicalPlan = super.visitRelevanceExpr(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitLogicalNot(ctx: OpenSearchPPLParser.LogicalNotContext): LogicalPlan = super.visitLogicalNot(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitBooleanExpr(ctx: OpenSearchPPLParser.BooleanExprContext): LogicalPlan = super.visitBooleanExpr(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitLogicalAnd(ctx: OpenSearchPPLParser.LogicalAndContext): LogicalPlan = super.visitLogicalAnd(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitComparsion(ctx: OpenSearchPPLParser.ComparsionContext): LogicalPlan = super.visitComparsion(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitLogicalXor(ctx: OpenSearchPPLParser.LogicalXorContext): LogicalPlan = super.visitLogicalXor(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitLogicalOr(ctx: OpenSearchPPLParser.LogicalOrContext): LogicalPlan = super.visitLogicalOr(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitCompareExpr(ctx: OpenSearchPPLParser.CompareExprContext): LogicalPlan = super.visitCompareExpr(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitInExpr(ctx: OpenSearchPPLParser.InExprContext): LogicalPlan = super.visitInExpr(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPositionFunctionCall(ctx: OpenSearchPPLParser.PositionFunctionCallContext): LogicalPlan = super.visitPositionFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitValueExpressionDefault(ctx: OpenSearchPPLParser.ValueExpressionDefaultContext): LogicalPlan = super.visitValueExpressionDefault(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitParentheticValueExpr(ctx: OpenSearchPPLParser.ParentheticValueExprContext): LogicalPlan = super.visitParentheticValueExpr(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitGetFormatFunctionCall(ctx: OpenSearchPPLParser.GetFormatFunctionCallContext): LogicalPlan = super.visitGetFormatFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitExtractFunctionCall(ctx: OpenSearchPPLParser.ExtractFunctionCallContext): LogicalPlan = super.visitExtractFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitBinaryArithmetic(ctx: OpenSearchPPLParser.BinaryArithmeticContext): LogicalPlan = super.visitBinaryArithmetic(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTimestampFunctionCall(ctx: OpenSearchPPLParser.TimestampFunctionCallContext): LogicalPlan = super.visitTimestampFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPrimaryExpression(ctx: OpenSearchPPLParser.PrimaryExpressionContext): LogicalPlan = super.visitPrimaryExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPositionFunction(ctx: OpenSearchPPLParser.PositionFunctionContext): LogicalPlan = super.visitPositionFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitBooleanExpression(ctx: OpenSearchPPLParser.BooleanExpressionContext): LogicalPlan = super.visitBooleanExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceExpression(ctx: OpenSearchPPLParser.RelevanceExpressionContext): LogicalPlan = super.visitRelevanceExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSingleFieldRelevanceFunction(ctx: OpenSearchPPLParser.SingleFieldRelevanceFunctionContext): LogicalPlan = super.visitSingleFieldRelevanceFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitMultiFieldRelevanceFunction(ctx: OpenSearchPPLParser.MultiFieldRelevanceFunctionContext): LogicalPlan = super.visitMultiFieldRelevanceFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTableSource(ctx: OpenSearchPPLParser.TableSourceContext): LogicalPlan = super.visitTableSource(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTableFunction(ctx: OpenSearchPPLParser.TableFunctionContext): LogicalPlan = super.visitTableFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitFieldList(ctx: OpenSearchPPLParser.FieldListContext): LogicalPlan = super.visitFieldList(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitWcFieldList(ctx: OpenSearchPPLParser.WcFieldListContext): LogicalPlan = super.visitWcFieldList(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSortField(ctx: OpenSearchPPLParser.SortFieldContext): LogicalPlan = super.visitSortField(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSortFieldExpression(ctx: OpenSearchPPLParser.SortFieldExpressionContext): LogicalPlan = super.visitSortFieldExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitFieldExpression(ctx: OpenSearchPPLParser.FieldExpressionContext): LogicalPlan = super.visitFieldExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitWcFieldExpression(ctx: OpenSearchPPLParser.WcFieldExpressionContext): LogicalPlan = super.visitWcFieldExpression(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitEvalFunctionCall(ctx: OpenSearchPPLParser.EvalFunctionCallContext): LogicalPlan = super.visitEvalFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDataTypeFunctionCall(ctx: OpenSearchPPLParser.DataTypeFunctionCallContext): LogicalPlan = super.visitDataTypeFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitBooleanFunctionCall(ctx: OpenSearchPPLParser.BooleanFunctionCallContext): LogicalPlan = super.visitBooleanFunctionCall(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitConvertedDataType(ctx: OpenSearchPPLParser.ConvertedDataTypeContext): LogicalPlan = super.visitConvertedDataType(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitEvalFunctionName(ctx: OpenSearchPPLParser.EvalFunctionNameContext): LogicalPlan = super.visitEvalFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitFunctionArgs(ctx: OpenSearchPPLParser.FunctionArgsContext): LogicalPlan = super.visitFunctionArgs(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitFunctionArg(ctx: OpenSearchPPLParser.FunctionArgContext): LogicalPlan = super.visitFunctionArg(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceArg(ctx: OpenSearchPPLParser.RelevanceArgContext): LogicalPlan = super.visitRelevanceArg(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceArgName(ctx: OpenSearchPPLParser.RelevanceArgNameContext): LogicalPlan = super.visitRelevanceArgName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceFieldAndWeight(ctx: OpenSearchPPLParser.RelevanceFieldAndWeightContext): LogicalPlan = super.visitRelevanceFieldAndWeight(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceFieldWeight(ctx: OpenSearchPPLParser.RelevanceFieldWeightContext): LogicalPlan = super.visitRelevanceFieldWeight(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceField(ctx: OpenSearchPPLParser.RelevanceFieldContext): LogicalPlan = super.visitRelevanceField(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceQuery(ctx: OpenSearchPPLParser.RelevanceQueryContext): LogicalPlan = super.visitRelevanceQuery(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitRelevanceArgValue(ctx: OpenSearchPPLParser.RelevanceArgValueContext): LogicalPlan = super.visitRelevanceArgValue(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitMathematicalFunctionName(ctx: OpenSearchPPLParser.MathematicalFunctionNameContext): LogicalPlan = super.visitMathematicalFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTrigonometricFunctionName(ctx: OpenSearchPPLParser.TrigonometricFunctionNameContext): LogicalPlan = super.visitTrigonometricFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDateTimeFunctionName(ctx: OpenSearchPPLParser.DateTimeFunctionNameContext): LogicalPlan = super.visitDateTimeFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitGetFormatFunction(ctx: OpenSearchPPLParser.GetFormatFunctionContext): LogicalPlan = super.visitGetFormatFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitGetFormatType(ctx: OpenSearchPPLParser.GetFormatTypeContext): LogicalPlan = super.visitGetFormatType(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitExtractFunction(ctx: OpenSearchPPLParser.ExtractFunctionContext): LogicalPlan = super.visitExtractFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSimpleDateTimePart(ctx: OpenSearchPPLParser.SimpleDateTimePartContext): LogicalPlan = super.visitSimpleDateTimePart(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitComplexDateTimePart(ctx: OpenSearchPPLParser.ComplexDateTimePartContext): LogicalPlan = super.visitComplexDateTimePart(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDatetimePart(ctx: OpenSearchPPLParser.DatetimePartContext): LogicalPlan = super.visitDatetimePart(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTimestampFunction(ctx: OpenSearchPPLParser.TimestampFunctionContext): LogicalPlan = super.visitTimestampFunction(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTimestampFunctionName(ctx: OpenSearchPPLParser.TimestampFunctionNameContext): LogicalPlan = super.visitTimestampFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitConditionFunctionBase(ctx: OpenSearchPPLParser.ConditionFunctionBaseContext): LogicalPlan = super.visitConditionFunctionBase(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSystemFunctionName(ctx: OpenSearchPPLParser.SystemFunctionNameContext): LogicalPlan = super.visitSystemFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTextFunctionName(ctx: OpenSearchPPLParser.TextFunctionNameContext): LogicalPlan = super.visitTextFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitPositionFunctionName(ctx: OpenSearchPPLParser.PositionFunctionNameContext): LogicalPlan = super.visitPositionFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitComparisonOperator(ctx: OpenSearchPPLParser.ComparisonOperatorContext): LogicalPlan = super.visitComparisonOperator(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitSingleFieldRelevanceFunctionName(ctx: OpenSearchPPLParser.SingleFieldRelevanceFunctionNameContext): LogicalPlan = super.visitSingleFieldRelevanceFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitMultiFieldRelevanceFunctionName(ctx: OpenSearchPPLParser.MultiFieldRelevanceFunctionNameContext): LogicalPlan = super.visitMultiFieldRelevanceFunctionName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitLiteralValue(ctx: OpenSearchPPLParser.LiteralValueContext): LogicalPlan = super.visitLiteralValue(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIntervalLiteral(ctx: OpenSearchPPLParser.IntervalLiteralContext): LogicalPlan = super.visitIntervalLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitStringLiteral(ctx: OpenSearchPPLParser.StringLiteralContext): LogicalPlan = super.visitStringLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIntegerLiteral(ctx: OpenSearchPPLParser.IntegerLiteralContext): LogicalPlan = super.visitIntegerLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDecimalLiteral(ctx: OpenSearchPPLParser.DecimalLiteralContext): LogicalPlan = super.visitDecimalLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitBooleanLiteral(ctx: OpenSearchPPLParser.BooleanLiteralContext): LogicalPlan = super.visitBooleanLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDatetimeLiteral(ctx: OpenSearchPPLParser.DatetimeLiteralContext): LogicalPlan = super.visitDatetimeLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitDateLiteral(ctx: OpenSearchPPLParser.DateLiteralContext): LogicalPlan = super.visitDateLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTimeLiteral(ctx: OpenSearchPPLParser.TimeLiteralContext): LogicalPlan = super.visitTimeLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTimestampLiteral(ctx: OpenSearchPPLParser.TimestampLiteralContext): LogicalPlan = super.visitTimestampLiteral(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIntervalUnit(ctx: OpenSearchPPLParser.IntervalUnitContext): LogicalPlan = super.visitIntervalUnit(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTimespanUnit(ctx: OpenSearchPPLParser.TimespanUnitContext): LogicalPlan = super.visitTimespanUnit(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitValueList(ctx: OpenSearchPPLParser.ValueListContext): LogicalPlan = super.visitValueList(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIdentsAsQualifiedName(ctx: OpenSearchPPLParser.IdentsAsQualifiedNameContext): LogicalPlan = super.visitIdentsAsQualifiedName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIdentsAsTableQualifiedName(ctx: OpenSearchPPLParser.IdentsAsTableQualifiedNameContext): LogicalPlan = super.visitIdentsAsTableQualifiedName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIdentsAsWildcardQualifiedName(ctx: OpenSearchPPLParser.IdentsAsWildcardQualifiedNameContext): LogicalPlan = super.visitIdentsAsWildcardQualifiedName(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitIdent(ctx: OpenSearchPPLParser.IdentContext): LogicalPlan = super.visitIdent(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitTableIdent(ctx: OpenSearchPPLParser.TableIdentContext): LogicalPlan = super.visitTableIdent(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitWildcard(ctx: OpenSearchPPLParser.WildcardContext): LogicalPlan = super.visitWildcard(ctx)

  /**
   * {@inheritDoc }
   *
   * <p>The default implementation returns the result of calling
   * {@link # visitChildren} on {@code ctx}.</p>
   */
  override def visitKeywordsCanBeId(ctx: OpenSearchPPLParser.KeywordsCanBeIdContext): LogicalPlan = super.visitKeywordsCanBeId(ctx)

  override def visit(tree: ParseTree): LogicalPlan = super.visit(tree)

  override def visitChildren(node: RuleNode): LogicalPlan = super.visitChildren(node)

  override def visitTerminal(node: TerminalNode): LogicalPlan = super.visitTerminal(node)

  override def visitErrorNode(node: ErrorNode): LogicalPlan = super.visitErrorNode(node)

  override def defaultResult(): LogicalPlan = super.defaultResult()

  override def aggregateResult(aggregate: LogicalPlan, nextResult: LogicalPlan): LogicalPlan = super.aggregateResult(aggregate, nextResult)

  override def shouldVisitNextChild(node: RuleNode, currentResult: LogicalPlan): Boolean = super.shouldVisitNextChild(node, currentResult)

}
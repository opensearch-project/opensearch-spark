/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.opensearch.sql.common.antlr.{CaseInsensitiveCharStream, SyntaxAnalysisErrorListener}

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos.ZERO
import org.apache.calcite.sql.{SqlBasicCall, SqlIdentifier, SqlLiteral, SqlNode, SqlNodeList, SqlSelect}


class PPLParser {
  val astBuilder = new PPLAstBuilder()

  def parseQuery(query: String): SqlNode = parse(query) { parser =>
    val ctx = parser.root().pplStatement()
    val a = astBuilder.visit(ctx)
    a
  }

  protected def parse[T](command: String)(toResult: OpenSearchPPLParser => T): T = {
    val lexer = new OpenSearchPPLLexer(new CaseInsensitiveCharStream(command))
    // lexer.removeErrorListeners()
    // lexer.addErrorListener(ParseErrorListener)
    lexer.addErrorListener(new SyntaxAnalysisErrorListener())

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new OpenSearchPPLParser(tokenStream)
    parser.addErrorListener(new SyntaxAnalysisErrorListener())
    // parser.addParseListener(PostProcessor)
    // parser.addParseListener(UnclosedCommentProcessor(command, tokenStream))
    // parser.removeErrorListeners()
    // parser.addErrorListener(ParseErrorListener)
    /*
    parser.legacy_setops_precedence_enabled = conf.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = conf.enforceReservedKeywords
    parser.double_quoted_identifiers = conf.doubleQuotedIdentifiers
     */

    // https://github.com/antlr/antlr4/issues/192#issuecomment-15238595
    // Save a great deal of time on correct inputs by using a two-stage parsing strategy.
    try {
      try {
        // first, try parsing with potentially faster SLL mode w/ SparkParserBailErrorStrategy
        // parser.setErrorHandler(new SparkParserBailErrorStrategy())
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        val a = toResult(parser)
        a
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode w/ SparkParserErrorStrategy
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          // parser.setErrorHandler(new SparkParserErrorStrategy())
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
  }
}

class PPLAstBuilder extends OpenSearchPPLParserBaseVisitor[SqlNode] {
  val functionResolver = PPLFunctionResolver();

  override def visitDmlStatement(ctx: OpenSearchPPLParser.DmlStatementContext): SqlNode = {
    visit(ctx.queryStatement())
  }

  override def visitQueryStatement(ctx: OpenSearchPPLParser.QueryStatementContext): SqlNode = {
    val source = visit(ctx.pplCommands()).asInstanceOf[SqlSelect]
    val commands = ctx.commands().asScala.map(visit).map(_.asInstanceOf[SqlSelect])
    val result: SqlSelect = commands.foldLeft(source) {(pre: SqlNode, cur: SqlSelect) =>
      cur.setFrom(pre)
      cur
    }
    result
  }

  override def visitPplCommands(ctx: OpenSearchPPLParser.PplCommandsContext): SqlNode = {
    val from = visit(ctx.searchCommand())
    new SqlSelect(ZERO, null, SqlNodeList.SINGLETON_STAR, from, null, null, null, null, null, null, null, null)
  }

  override def visitFromClause(ctx: OpenSearchPPLParser.FromClauseContext): SqlNode = {
    super.visitFromClause(ctx)
  }

  override def visitTableOrSubqueryClause(ctx: OpenSearchPPLParser.TableOrSubqueryClauseContext): SqlNode = {
    if (ctx.subSearch() != null) {
      null
    } else {
      visitTableSourceClause(ctx.tableSourceClause());
    }
  }

  override def visitTableSourceClause(ctx: OpenSearchPPLParser.TableSourceClauseContext): SqlNode = {
    var sqlNodes = Seq[SqlNode]()
    for (i <- 0 until ctx.tableSource().size) {
      sqlNodes :+= visitTableSource(ctx.tableSource(i)).asInstanceOf[SqlNode]
    }
    // val sqlNodes = ctx.tableSource().stream().map(a => visitTableSource(a)).collect(Collectors.toList)
    if (ctx.alias == null) {
      sqlNodes.head
      // sqlNodes.get(0)
    //} else new SqlBasicCall(SqlStdOperatorTable.AS, sqlNodes.toArray(new Array[SqlNode](0)), ZERO)
    } else new SqlBasicCall(SqlStdOperatorTable.AS, sqlNodes.toArray, ZERO)
  }

  override def visitIdentsAsTableQualifiedName(ctx: OpenSearchPPLParser.IdentsAsTableQualifiedNameContext): SqlNode = {
    new SqlIdentifier(ctx.tableIdent().ident().getText, ZERO)
  }

  override def visitWhereCommand(ctx: OpenSearchPPLParser.WhereCommandContext): SqlNode = {
    val where = visitChildren(ctx)
    new SqlSelect(ZERO, null, SqlNodeList.SINGLETON_STAR, null, where, null, null, null, null, null, null, null)
  }

  override def visitComparsion(ctx: OpenSearchPPLParser.ComparsionContext): SqlNode = {
    super.visitComparsion(ctx)
  }

  override def visitCompareExpr(ctx: OpenSearchPPLParser.CompareExprContext): SqlNode = {
    functionResolver.resolve(ctx.comparisonOperator.getText).createCall(null, ZERO, visit(ctx.left), visit(ctx.right))
  }

  override def visitIdentsAsQualifiedName(ctx: OpenSearchPPLParser.IdentsAsQualifiedNameContext): SqlNode = {
    new SqlIdentifier(ctx.ident().asScala.map(_.getText).reduce((a, b) => a + "." + b), ZERO)
  }

  override def visitIdent(ctx: OpenSearchPPLParser.IdentContext): SqlNode = {
    new SqlIdentifier(ctx.getText, ZERO)
  }

  override def visitIntegerLiteral(ctx: OpenSearchPPLParser.IntegerLiteralContext): SqlNode = {
    SqlLiteral.createExactNumeric(ctx.getText, ZERO)
  }

  override def visitFieldsCommand(ctx: OpenSearchPPLParser.FieldsCommandContext): SqlNode = {
    val selectExpr = visitFieldList(ctx.fieldList())
    new SqlSelect(ZERO, null, selectExpr, null, null, null, null, null, null, null, null, null)
  }

  override def visitFieldList(ctx: OpenSearchPPLParser.FieldListContext): SqlNodeList = {
    val fields = ctx.fieldExpression.asScala.map(visit)
    SqlNodeList.of(ZERO, fields.asJava)
  }

  override def visitSortCommand(ctx: OpenSearchPPLParser.SortCommandContext): SqlNode = {
    val orderByList = visitSortbyClause(ctx.sortbyClause())
    new SqlSelect(ZERO, null, SqlNodeList.SINGLETON_STAR, null, null, null, null, null, orderByList, null, null, null)
  }

  override def visitSortbyClause(ctx: OpenSearchPPLParser.SortbyClauseContext): SqlNodeList = {
    val fields = ctx.sortField().asScala.map(visit)
    SqlNodeList.of(ZERO, fields.asJava)
  }

  override def visitStatsCommand(ctx: OpenSearchPPLParser.StatsCommandContext): SqlNode = {
    val aggList = ctx.statsAggTerm.asScala.map(visit)
    val groupByList = visitStatsByClause(ctx.statsByClause())
    new SqlSelect(ZERO, null, SqlNodeList.of(ZERO, (groupByList.getList.asScala ++ aggList).asJava), null, null, groupByList, null, null, null, null, null, null)
  }

  override def visitStatsAggTerm(ctx: OpenSearchPPLParser.StatsAggTermContext): SqlNode = {
    val agg = visit(ctx.statsFunction())

    if (ctx.alias == null) agg else {
      val alias = visit(ctx.alias)
      new SqlBasicCall(SqlStdOperatorTable.AS, Seq(agg, alias).asJava.toArray(new Array[SqlNode](0)), ZERO)
    }
  }

  override def visitStatsFunctionCall(ctx: OpenSearchPPLParser.StatsFunctionCallContext): SqlNode = {
    functionResolver.resolve(ctx.statsFunctionName.getText).createCall(null, ZERO, visit(ctx.valueExpression))
  }

  override def visitStatsByClause(ctx: OpenSearchPPLParser.StatsByClauseContext): SqlNodeList = {
    SqlNodeList.of(ZERO, ctx.fieldList().fieldExpression().asScala.map(visit).asJava)
  }

  override def visitEvalCommand(ctx: OpenSearchPPLParser.EvalCommandContext): SqlNode = {
    val evalClause = ctx.evalClause().asScala
    val (identList, fieldExprList) = evalClause.map(clause => {
      val fieldExpr = visit(clause.fieldExpression().qualifiedName())
      val expr = visit(clause.expression())
      (new SqlBasicCall(SqlStdOperatorTable.AS, Seq(expr, fieldExpr).asJava, ZERO).asInstanceOf[SqlNode], fieldExpr)
    }).unzip
    identList.append(StarExcept(SqlNodeList.of(ZERO, fieldExprList.asJava))(ZERO))
    new SqlSelect(ZERO, null, SqlNodeList.of(ZERO, identList.asJava), null, null, null, null, null, null, null, null, null)
  }

  override def visitBinaryArithmetic(ctx: OpenSearchPPLParser.BinaryArithmeticContext): SqlNode = {
    functionResolver.resolve(ctx.binaryOperator.getText).createCall(null, ZERO, visit(ctx.left), visit(ctx.right))
  }


  override def visitLookupCommand(ctx: OpenSearchPPLParser.LookupCommandContext): SqlNode = {
    super.visitLookupCommand(ctx)
  }

}

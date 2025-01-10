/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import scala.+:
import scala.collection.JavaConverters._

import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.opensearch.sql.common.antlr.{CaseInsensitiveCharStream, SyntaxAnalysisErrorListener}

import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos.ZERO
import org.apache.calcite.sql._


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
  var subquery_count = 0;

  override def visitDmlStatement(ctx: OpenSearchPPLParser.DmlStatementContext): SqlNode = {
    visit(ctx.queryStatement())
  }

  override def visitQueryStatement(ctx: OpenSearchPPLParser.QueryStatementContext): SqlNode = {
    val source = visit(ctx.pplCommands()).asInstanceOf[SqlSelect]
    val commands = ctx.commands().asScala.map(visit).map(_.asInstanceOf[SqlSelect])
    val result: SqlSelect = commands.foldLeft(source) {(pre: SqlNode, cur: SqlSelect) =>
      cur.getFrom match {
        case null =>
          cur.setFrom(pre)
        case join: SqlJoin if join.getLeft.isInstanceOf[SqlBasicCall] =>
          join.getLeft.asInstanceOf[SqlBasicCall].setOperand(0, pre)
        case _ =>
      }
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
    val statsByList = visitStatsByClause(ctx.statsByClause())
    if (ctx.EVENTSTATS != null) {
      val windowDecl = SqlWindow.create(null, null, statsByList, SqlNodeList.EMPTY, SqlLiteral.createBoolean(false, ZERO), null, null, null, ZERO)
      val newAggList = aggList.map { case agg: SqlBasicCall =>
        if (agg.getKind == SqlKind.AS) {
          val aggExpr = agg.getOperandList.get(0)
          agg.setOperand(0, new SqlBasicCall(SqlStdOperatorTable.OVER, Seq(aggExpr, windowDecl).asJava.toArray(new Array[SqlNode](0)), ZERO))
          agg
        } else new SqlBasicCall(SqlStdOperatorTable.OVER, Seq(agg, windowDecl).asJava.toArray(new Array[SqlNode](0)), ZERO)
      }
      new SqlSelect(ZERO, null, SqlNodeList.of(ZERO, ((SqlIdentifier.STAR +: newAggList).asJava)), null, null, null, null, null, null, null, null, null)
    } else new SqlSelect(ZERO, null, SqlNodeList.of(ZERO, (statsByList.getList.asScala ++ aggList).asJava), null, null, statsByList, null, null, null, null, null, null)
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
    identList.append(StarExcept(SqlNodeList.of(ZERO, fieldExprList.asJava))())
    new SqlSelect(ZERO, null, SqlNodeList.of(ZERO, identList.asJava), null, null, null, null, null, null, null, null, null)
  }

  override def visitBinaryArithmetic(ctx: OpenSearchPPLParser.BinaryArithmeticContext): SqlNode = {
    functionResolver.resolve(ctx.binaryOperator.getText).createCall(null, ZERO, visit(ctx.left), visit(ctx.right))
  }


  override def visitLookupCommand(ctx: OpenSearchPPLParser.LookupCommandContext): SqlNode = {
    val right = visit(ctx.tableSource)
    require(right.isInstanceOf[SqlIdentifier], "LOOKUP table is not an ident")
    val rightTableIdent = right.asInstanceOf[SqlIdentifier]
    val leftSubqueryName = s"TEMP_SUBQUERY_$subquery_count"
    subquery_count += 1
    val leftTableIdent = new SqlIdentifier(leftSubqueryName, ZERO)
    val left = new SqlBasicCall(SqlStdOperatorTable.AS, Seq(null, leftTableIdent).asJava, ZERO)
    val conditionList = ctx.lookupMappingList.lookupPair.asScala.map { pair => {
      val rightKey = visit(pair.inputField)
      val leftKey = if (pair.outputField != null) {
        visit(pair.outputField)
      } else rightKey.clone(ZERO)
      require(leftKey.isInstanceOf[SqlIdentifier], "left join key is not an ident")
      require(rightKey.isInstanceOf[SqlIdentifier], "right join key is not an ident")
      val leftKeyIdent = leftKey.asInstanceOf[SqlIdentifier]
      val rightKeyIdent = rightKey.asInstanceOf[SqlIdentifier]
      val newLeftKey = new SqlIdentifier((leftSubqueryName +: leftKeyIdent.names.asScala).asJava, ZERO)
      val newRightKey = new SqlIdentifier((rightTableIdent.names.asScala ++ rightKeyIdent.names.asScala).asJava, ZERO)
      SqlStdOperatorTable.EQUALS.createCall(null, ZERO, newLeftKey, newRightKey)
    }}
    val conditionCombine = conditionList.reduce((a, b) => SqlStdOperatorTable.AND.createCall(null, ZERO, a, b))
    val sqlJoin = new SqlJoin(ZERO,
      left,
      SqlLiteral.createBoolean(false, ZERO),
      JoinType.LEFT.symbol(ZERO),
      right,
      JoinConditionType.ON.symbol(ZERO),
      conditionCombine)

    val fieldsPair = ctx.outputCandidateList().lookupPair.asScala.map { pair => {
      val rightField = visit(pair.inputField)
      val leftField = if (pair.outputField != null) {
        visit(pair.outputField)
      } else rightField.clone(ZERO)
      require(leftField.isInstanceOf[SqlIdentifier], "left join key is not an ident")
      require(rightField.isInstanceOf[SqlIdentifier], "right join key is not an ident")
      val leftKeyIdent = leftField.asInstanceOf[SqlIdentifier]
      val rightKeyIdent = rightField.asInstanceOf[SqlIdentifier]
      val newLeftField = new SqlIdentifier((leftSubqueryName +: leftKeyIdent.names.asScala).asJava, ZERO)
      val newRightField = new SqlIdentifier((rightTableIdent.names.asScala ++ rightKeyIdent.names.asScala).asJava, ZERO)
      (newLeftField.asInstanceOf[SqlNode], newRightField.asInstanceOf[SqlNode])
    }}
    val leftFields = fieldsPair.map(_._1)

    val selectItems = if (ctx.APPEND != null) {
      val newFields = fieldsPair.map { case (leftField, rightField) => new SqlBasicCall(SqlStdOperatorTable.AS, Seq(rightField, leftField).asJava, ZERO) }
      SqlNodeList.of(ZERO, (newFields :+ StarExcept(SqlNodeList.of(ZERO, leftFields.asJava))(leftTableIdent.names.asScala :+ "")).asJava)
    } else if (ctx.REPLACE != null) {
      val newFields = fieldsPair.map { case (leftField, rightField) => new SqlBasicCall(SqlStdOperatorTable.COALESCE, Seq(leftField, rightField).asJava, ZERO) }
      SqlNodeList.of(ZERO, (newFields :+ StarExcept(SqlNodeList.of(ZERO, leftFields.asJava))(leftTableIdent.names.asScala :+ "")).asJava)
    } else {
      SqlNodeList.SINGLETON_STAR
    }
    new SqlSelect(ZERO, null, selectItems, sqlJoin, null, null, null, null, null, null, null, null)
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.antlr.v4.runtime.{CommonTokenStream, Lexer}
import org.antlr.v4.runtime.tree.ParseTree
import org.opensearch.sql.ast.statement.Statement
import org.opensearch.sql.common.antlr.{CaseInsensitiveCharStream, Parser, SyntaxAnalysisErrorListener}
import org.opensearch.sql.ppl.parser.{AstBuilder, AstCommandDescriptionVisitor, AstExpressionBuilder, AstStatementBuilder}

class PPLSyntaxParser extends Parser {
  // Analyze the query syntax
  override def parse(query: String): ParseTree = {
    val parser = createParser(createLexer(query))
    parser.addErrorListener(new SyntaxAnalysisErrorListener())
    parser.root()
  }

  private def createParser(lexer: Lexer): OpenSearchPPLParser = {
    new OpenSearchPPLParser(new CommonTokenStream(lexer))
  }

  private def createLexer(query: String): OpenSearchPPLLexer = {
    new OpenSearchPPLLexer(new CaseInsensitiveCharStream(query))
  }

  def getParserVersion(): String = {
    s"${OpenSearchPPLParser.getGrammarVersion()} (Last updated: ${OpenSearchPPLParser.getLastUpdated()})"
  }
}

object PlaneUtils {
  def plan(parser: PPLSyntaxParser, query: String): Statement = {
    val parsedTree = parser.parse(query)

    // Create an instance of each visitor
    val expressionBuilder = new AstExpressionBuilder()
    val astBuilder = new AstBuilder(expressionBuilder, query, parser.getParserVersion())
    expressionBuilder.setAstBuilder(astBuilder)
    // description visitor
    val astDescriptionBuilder =
      new AstCommandDescriptionVisitor(expressionBuilder, query, parser.getParserVersion())
    // statement visitor
    val statementContext = AstStatementBuilder.StatementBuilderContext.builder()

    // Chain visitors
    val builder = new AstStatementBuilder(statementContext, astBuilder, astDescriptionBuilder)
    builder.visit(parsedTree)
  }
}

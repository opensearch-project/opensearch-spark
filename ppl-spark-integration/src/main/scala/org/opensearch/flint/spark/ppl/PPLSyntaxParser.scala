/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.antlr.v4.runtime.{CommonTokenStream, Lexer}
import org.antlr.v4.runtime.tree.ParseTree
import org.opensearch.sql.ast.statement.Statement
import org.opensearch.sql.common.antlr.{CaseInsensitiveCharStream, Parser, SyntaxAnalysisErrorListener}
import org.opensearch.sql.ppl.parser.{AstBuilder, AstExpressionBuilder, AstStatementBuilder}

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
}

object PlaneUtils {
  def plan(parser: PPLSyntaxParser, query: String, isExplain: Boolean): Statement = {
    val builder = new AstStatementBuilder(
      new AstBuilder(new AstExpressionBuilder(), query),
      AstStatementBuilder.StatementBuilderContext.builder())
    builder.visit(parser.parse(query))
  }
}

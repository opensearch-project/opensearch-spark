/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.antlr.v4.runtime.{CommonTokenStream, Lexer}
import org.antlr.v4.runtime.tree.ParseTree
import org.opensearch.flint.spark.sql.{OpenSearchPPLLexer, OpenSearchPPLParser}
import org.opensearch.sql.common.antlr.{CaseInsensitiveCharStream, SyntaxAnalysisErrorListener}
import org.opensearch.sql.common.antlr.Parser

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
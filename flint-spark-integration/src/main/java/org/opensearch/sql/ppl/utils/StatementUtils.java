package org.opensearch.sql.ppl.utils;

import org.opensearch.flint.spark.ppl.PPLSyntaxParser;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

public class StatementUtils {
    public static Statement plan(PPLSyntaxParser parser, String query, boolean isExplain) {
        final AstStatementBuilder builder =
                new AstStatementBuilder(
                        new AstBuilder(new AstExpressionBuilder(), query),
                        AstStatementBuilder.StatementBuilderContext.builder());
        return builder.visit(parser.parse(query));
    }
}

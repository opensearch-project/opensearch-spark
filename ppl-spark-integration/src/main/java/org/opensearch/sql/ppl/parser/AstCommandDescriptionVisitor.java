package org.opensearch.sql.ppl.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.DescribeCommand;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/** Class of building the AST. Refines the visit path and build the AST help description command for each command */
public class AstCommandDescriptionVisitor extends OpenSearchPPLParserBaseVisitor<UnresolvedPlan> {
    private String version;
    private AstExpressionBuilder expressionBuilder;
    
    public AstCommandDescriptionVisitor(AstExpressionBuilder expressionBuilder, String query, String version) {
        this.expressionBuilder = expressionBuilder;
        this.version = version;
    }

    @Override
    public UnresolvedPlan visitSearchHelp(OpenSearchPPLParser.SearchHelpContext ctx) {
        String description = "SEARCH Command:\n" +
                "\n" +
                "Syntax:\n" +
                "   (SEARCH)? fromClause\n" +
                "   | (SEARCH)? fromClause logicalExpression\n" +
                "   | (SEARCH)? logicalExpression fromClause\n" +
                "\n" +
                "Description:\n" +
                "The SEARCH command is used to retrieve data from a specified source. It can be used with or without additional filters.\n" +
                "- You can specify the data source using the FROM clause.\n" +
                "- You can add filters using logical expressions.\n" +
                "- The order of FROM clause and logical expression can be interchanged.";
        return new DescribeCommand(describeCommand(description, version));
    }


    public static String describeCommand( String description, String version) {
        StringBuilder commandSummary = new StringBuilder();
        commandSummary.append("\n\n").append(version).append(" PPL Revision:\n\n");
        commandSummary.append("Description:\n");
        commandSummary.append(description).append("\n");
        return commandSummary.toString();
    }
}

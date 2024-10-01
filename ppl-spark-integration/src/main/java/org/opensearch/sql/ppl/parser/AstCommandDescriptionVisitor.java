/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ast.tree.DescribeCommand;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

import java.util.Objects;

/** Class of building the AST. Refines the visit path and build the AST help description command for each command */
public class AstCommandDescriptionVisitor extends OpenSearchPPLParserBaseVisitor<UnresolvedPlan> {
    private String version;
    private AstExpressionBuilder expressionBuilder;
    
    public AstCommandDescriptionVisitor(AstExpressionBuilder expressionBuilder, String query, String version) {
        this.expressionBuilder = expressionBuilder;
        this.version = version;
    }

    @Override
    public UnresolvedPlan visitWhereHelp(OpenSearchPPLParser.WhereHelpContext ctx) {
        return super.visitWhereHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitCorrelateHelp(OpenSearchPPLParser.CorrelateHelpContext ctx) {
        return super.visitCorrelateHelp(ctx);
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

    @Override
    public UnresolvedPlan visitDescribeHelp(OpenSearchPPLParser.DescribeHelpContext ctx) {
        return super.visitDescribeHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitHelpCommandName(OpenSearchPPLParser.HelpCommandNameContext ctx) {
        OpenSearchPPLParser.CommandNamesContext commandedName = ctx.commandNames();
        if(!Objects.isNull(commandedName.SEARCH())) {
            return visitSearchHelp(null);
        }
        if(!Objects.isNull(commandedName.DESCRIBE())) {
            return visitDescribeHelp(null);
        }
        if(!Objects.isNull(commandedName.WHERE())) {
            return visitWhereHelp(null);
        }
        return new DescribeCommand(describeCommand("This command has no help description - please check revision for compatability", version));
    }

    public static String describeCommand( String description, String version) {
        StringBuilder commandSummary = new StringBuilder();
        commandSummary.append("\n\n").append(version).append(" PPL Revision:\n\n");
        commandSummary.append("Description:\n");
        commandSummary.append(description).append("\n");
        return commandSummary.toString();
    }
}

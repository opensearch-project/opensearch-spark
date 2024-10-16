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
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Search;
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
    public UnresolvedPlan visitRenameHelp(OpenSearchPPLParser.RenameHelpContext ctx) {
        return super.visitRenameHelp(ctx);
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
    public UnresolvedPlan visitTopHelp(OpenSearchPPLParser.TopHelpContext ctx) {
        return super.visitTopHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitEvalHelp(OpenSearchPPLParser.EvalHelpContext ctx) {
        return super.visitEvalHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitSortHelp(OpenSearchPPLParser.SortHelpContext ctx) {
        return super.visitSortHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitDedupHelp(OpenSearchPPLParser.DedupHelpContext ctx) {
        return super.visitDedupHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitStatsHelp(OpenSearchPPLParser.StatsHelpContext ctx) {
        return super.visitStatsHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitSearchHelp(OpenSearchPPLParser.SearchHelpContext ctx) {
        return new DescribeCommand(describeCommand(new Search.Helper().describe(), version));
    }

    @Override
    public UnresolvedPlan visitDescribeHelp(OpenSearchPPLParser.DescribeHelpContext ctx) {
        return super.visitDescribeHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitFieldsHelp(OpenSearchPPLParser.FieldsHelpContext ctx) {
        return super.visitFieldsHelp(ctx);
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

    @Override
    public UnresolvedPlan visitHeadHelp(OpenSearchPPLParser.HeadHelpContext ctx) {
        return super.visitHeadHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitJoinHelp(OpenSearchPPLParser.JoinHelpContext ctx) {
        return super.visitJoinHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitRareHelp(OpenSearchPPLParser.RareHelpContext ctx) {
        return super.visitRareHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitGrokHelp(OpenSearchPPLParser.GrokHelpContext ctx) {
        return super.visitGrokHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitParseHelp(OpenSearchPPLParser.ParseHelpContext ctx) {
        return new DescribeCommand(describeCommand(new Parse.Helper().describe(), version));
    }

    @Override
    public UnresolvedPlan visitPatternsHelp(OpenSearchPPLParser.PatternsHelpContext ctx) {
        return super.visitPatternsHelp(ctx);
    }

    @Override
    public UnresolvedPlan visitLookupHelp(OpenSearchPPLParser.LookupHelpContext ctx) {
        return super.visitLookupHelp(ctx);
    }

    public static String describeCommand(String description, String version) {
        StringBuilder commandSummary = new StringBuilder();
        commandSummary.append("\n\n").append(version).append(" PPL Revision:\n\n");
        commandSummary.append("Description:\n");
        commandSummary.append(description).append("\n");
        return commandSummary.toString();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ppl.parser;

import com.google.common.collect.ImmutableList;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParserBaseVisitor;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.DescribeRelation;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Build {@link Statement} from PPL Query. */

public class AstStatementBuilder extends OpenSearchPPLParserBaseVisitor<Statement> {

  private List<OpenSearchPPLParserBaseVisitor<UnresolvedPlan>> astBuilder;

  private StatementBuilderContext context;

  public AstStatementBuilder(StatementBuilderContext context, OpenSearchPPLParserBaseVisitor<UnresolvedPlan> ... astBuilders) {
    this.astBuilder = Arrays.asList(astBuilders);
    this.context = context;
  }

  @Override
  public Statement visitDmlStatement(OpenSearchPPLParser.DmlStatementContext ctx) {
    Query query = new Query(addSelectAll(visit(ctx)), context.getFetchSize());
    OpenSearchPPLParser.ExplainCommandContext explainContext = ctx.explainCommand();
    if (explainContext != null) {
      return new Explain(query, explainContext.explainMode().getText());
    }
    return query;
  }

  @Override
  protected Statement aggregateResult(Statement aggregate, Statement nextResult) {
    return nextResult != null ? nextResult : aggregate;
  }

  public StatementBuilderContext getContext() {
    return context;
  }
  
  public UnresolvedPlan visit(OpenSearchPPLParser.DmlStatementContext ctx) {
    return astBuilder.stream()
            .map(builder -> builder.visit(ctx))
            .filter(Objects::nonNull)
            .findAny().get();
  }
  
  public static class StatementBuilderContext {
    private int fetchSize;

    public StatementBuilderContext(int fetchSize) {
      this.fetchSize = fetchSize;
    }

    public static StatementBuilderContext builder() {
      //todo set the default statement builder init params configurable
      return new StatementBuilderContext(1000);
    }

    public int getFetchSize() {
      return fetchSize;
    }

    public Object build() {
      return null;
    }
  }

    private UnresolvedPlan addSelectAll(UnresolvedPlan plan) {
        if ((plan instanceof Project) && !((Project) plan).isExcluded()) {
            return plan;
        } else if (plan instanceof DescribeRelation) {
            return plan;
        } else {
            return new Project(ImmutableList.of(AllFields.of())).attach(plan);
        }
    }
}

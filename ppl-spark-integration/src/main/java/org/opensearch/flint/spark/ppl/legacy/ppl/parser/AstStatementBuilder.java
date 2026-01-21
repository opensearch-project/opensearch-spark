/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.flint.spark.ppl.legacy.ppl.parser;

import com.google.common.collect.ImmutableList;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParser;
import org.opensearch.flint.spark.ppl.OpenSearchPPLParserBaseVisitor;
import org.opensearch.flint.spark.ppl.legacy.ast.expression.AllFields;
import org.opensearch.flint.spark.ppl.legacy.ast.statement.Explain;
import org.opensearch.flint.spark.ppl.legacy.ast.statement.Query;
import org.opensearch.flint.spark.ppl.legacy.ast.statement.Statement;
import org.opensearch.flint.spark.ppl.legacy.ast.tree.DescribeRelation;
import org.opensearch.flint.spark.ppl.legacy.ast.tree.Project;
import org.opensearch.flint.spark.ppl.legacy.ast.tree.UnresolvedPlan;

/** Build {@link Statement} from PPL Query. */

public class AstStatementBuilder extends OpenSearchPPLParserBaseVisitor<Statement> {

  private AstBuilder astBuilder;

  private StatementBuilderContext context;

  public AstStatementBuilder(AstBuilder astBuilder, StatementBuilderContext context) {
    this.astBuilder = astBuilder;
    this.context = context;
  }

  @Override
  public Statement visitDmlStatement(OpenSearchPPLParser.DmlStatementContext ctx) {
    Query query = new Query(addSelectAll(astBuilder.visit(ctx)), context.getFetchSize());
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

  public AstBuilder builder() {
    return astBuilder;
  }

  public StatementBuilderContext getContext() {
    return context;
  }

  public static class StatementBuilderContext {
    public static final int FETCH_SIZE = 1000;
    private int fetchSize;

    public StatementBuilderContext(int fetchSize) {
      this.fetchSize = fetchSize;
    }

    public static StatementBuilderContext builder() {
      return new StatementBuilderContext(FETCH_SIZE);
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

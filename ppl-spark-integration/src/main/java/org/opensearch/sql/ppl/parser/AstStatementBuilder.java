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
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

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
    return context.isExplain ? new Explain(query) : query;
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
    private boolean isExplain;
    private int fetchSize;

    public StatementBuilderContext(boolean isExplain, int fetchSize) {
      this.isExplain = isExplain;
      this.fetchSize = fetchSize;
    }

    public static StatementBuilderContext builder() {
      //todo set the default statement builder init params configurable
      return new StatementBuilderContext(false,1000);
    }

    public StatementBuilderContext explain(boolean isExplain) {
      this.isExplain = isExplain;
      return this;
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
    } else {
      return new Project(ImmutableList.of(AllFields.of())).attach(plan);
    }
  }
}

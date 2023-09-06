/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;

import java.util.List;

/** AST node represent Dedupe operation. */
public class Dedupe extends UnresolvedPlan {
  private UnresolvedPlan child;
  private List<Argument> options;
  private List<Field> fields;

  public Dedupe(UnresolvedPlan child, List<Argument> options, List<Field> fields) {
    this.child = child;
    this.options = options;
    this.fields = fields;
  }
  public Dedupe(List<Argument> options, List<Field> fields) {
    this.options = options;
    this.fields = fields;
  }

  @Override
  public Dedupe attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  public List<Argument> getOptions() {
    return options;
  }

  public List<Field> getFields() {
    return fields;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitDedupe(this, context);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Map;

import java.util.List;

/** AST node represent Lookup operation. */

public class Lookup extends UnresolvedPlan {
  private UnresolvedPlan child;
  private final String tableName;
  private final List<Map> matchFieldList;
  private final List<Argument> options;
  private final List<Alias> copyFieldList;

  public Lookup(UnresolvedPlan child, String tableName, List<Map> matchFieldList, List<Argument> options, List<Alias> copyFieldList) {
    this.child = child;
    this.tableName = tableName;
    this.matchFieldList = matchFieldList;
    this.options = options;
    this.copyFieldList = copyFieldList;
  }

  public Lookup(String tableName, List<Map> matchFieldList, List<Argument> options, List<Alias> copyFieldList) {
    this.tableName = tableName;
    this.matchFieldList = matchFieldList;
    this.options = options;
    this.copyFieldList = copyFieldList;
  }

  @Override
  public Lookup attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Map> getMatchFieldList() {
    return matchFieldList;
  }

  public List<Argument> getOptions() {
    return options;
  }

  public List<Alias> getCopyFieldList() {
    return copyFieldList;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitLookup(this, context);
  }
}

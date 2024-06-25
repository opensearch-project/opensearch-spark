/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Map;

import java.util.List;

/** AST node represent Lookup operation. */

public class Lookup extends UnresolvedPlan {
  private UnresolvedPlan child;
  private final String indexName;
  private final List<Map> matchFieldList;
  private final List<Argument> options;
  private final List<Map> copyFieldList;

  public Lookup(UnresolvedPlan child, String indexName, List<Map> matchFieldList, List<Argument> options, List<Map> copyFieldList) {
    this.child = child;
    this.indexName = indexName;
    this.matchFieldList = matchFieldList;
    this.options = options;
    this.copyFieldList = copyFieldList;
  }

  public Lookup(String indexName, List<Map> matchFieldList, List<Argument> options, List<Map> copyFieldList) {
    this.indexName = indexName;
    this.matchFieldList = matchFieldList;
    this.options = options;
    this.copyFieldList = copyFieldList;
  }

  @Override
  public Lookup attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  public String getIndexName() {
    return indexName;
  }

  public List<Map> getMatchFieldList() {
    return matchFieldList;
  }

  public List<Argument> getOptions() {
    return options;
  }

  public List<Map> getCopyFieldList() {
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

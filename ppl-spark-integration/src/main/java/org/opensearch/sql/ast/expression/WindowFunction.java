/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.tree.Sort.SortOption;

import java.util.List;

public class WindowFunction extends UnresolvedExpression {
  private UnresolvedExpression function;
  private List<UnresolvedExpression> partitionByList;
  private List<Pair<SortOption, UnresolvedExpression>> sortList;

  public WindowFunction(UnresolvedExpression function, List<UnresolvedExpression> partitionByList, List<Pair<SortOption, UnresolvedExpression>> sortList) {
    this.function = function;
    this.partitionByList = partitionByList;
    this.sortList = sortList;
  }

  @Override
  public List<? extends Node> getChild() {
    ImmutableList.Builder<UnresolvedExpression> children = ImmutableList.builder();
    children.add(function);
    children.addAll(partitionByList);
    sortList.forEach(pair -> children.add(pair.getRight()));
    return children.build();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitWindowFunction(this, context);
  }
}

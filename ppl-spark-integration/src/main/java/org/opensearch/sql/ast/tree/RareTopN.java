/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.Collections;
import java.util.List;

/** AST node represent RareTopN operation. */

public class RareTopN extends UnresolvedPlan {

  private UnresolvedPlan child;
  private CommandType commandType;
  private List<Argument> noOfResults;
  private List<Field> fields;
  private List<UnresolvedExpression> groupExprList;

  public RareTopN( CommandType commandType, List<Argument> noOfResults, List<Field> fields, List<UnresolvedExpression> groupExprList) {
    this.commandType = commandType;
    this.noOfResults = noOfResults;
    this.fields = fields;
    this.groupExprList = groupExprList;
  }

  @Override
  public RareTopN attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  public CommandType getCommandType() {
    return commandType;
  }

  public List<Argument> getNoOfResults() {
    return noOfResults;
  }

  public List<Field> getFields() {
    return fields;
  }

  public List<UnresolvedExpression> getGroupExprList() {
    return groupExprList;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return Collections.singletonList(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRareTopN(this, context);
  }

  public enum CommandType {
    TOP,
    RARE
  }
}

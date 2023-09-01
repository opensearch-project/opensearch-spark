/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

import java.util.List;
import java.util.Map;

/** AST node represent Parse with regex operation. */

public class Parse extends UnresolvedPlan {
  /** Method used to parse a field. */
  private  ParseMethod parseMethod;

  /** Field. */
  private  UnresolvedExpression sourceField;

  /** Pattern. */
  private  Literal pattern;

  /** Optional arguments. */
  private  Map<String, Literal> arguments;

  /** Child Plan. */
  private UnresolvedPlan child;

  public Parse(ParseMethod parseMethod, UnresolvedExpression sourceField, Literal pattern, Map<String, Literal> arguments, UnresolvedPlan child) {
    this.parseMethod = parseMethod;
    this.sourceField = sourceField;
    this.pattern = pattern;
    this.arguments = arguments;
    this.child = child;
  }

  public Parse(ParseMethod parseMethod, UnresolvedExpression sourceField, Literal pattern, Map<String, Literal> arguments) {

    this.parseMethod = parseMethod;
    this.sourceField = sourceField;
    this.pattern = pattern;
    this.arguments = arguments;
  }

  public ParseMethod getParseMethod() {
    return parseMethod;
  }

  public UnresolvedExpression getSourceField() {
    return sourceField;
  }

  public Literal getPattern() {
    return pattern;
  }

  public Map<String, Literal> getArguments() {
    return arguments;
  }

  @Override
  public Parse attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitParse(this, context);
  }
}

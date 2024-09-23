/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * Alias abstraction that associate an unnamed expression with a name and an optional alias. The
 * name and alias information preserved is useful for semantic analysis and response formatting
 * eventually. This can avoid restoring the info in toString() method which is inaccurate because
 * original info is already lost.
 */
public class Alias extends UnresolvedExpression {

  /** Original field name. */
  private String name;

  /** Expression aliased. */
  private UnresolvedExpression delegated;

  /** Optional field alias. */
  private String alias;

  public Alias(String name, UnresolvedExpression delegated, String alias) {
    this.name = name;
    this.delegated = delegated;
    this.alias = alias;
  }

  public Alias(String name, UnresolvedExpression delegated) {
    this.name = name;
    this.delegated = delegated;
  }

  public String getName() {
    return name;
  }

  public UnresolvedExpression getDelegated() {
    return delegated;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitAlias(this, context);
  }
}

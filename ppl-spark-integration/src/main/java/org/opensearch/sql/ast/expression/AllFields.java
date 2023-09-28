/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

import java.util.Collections;
import java.util.List;

/** Represent the All fields which is been used in SELECT *. */
public class AllFields extends UnresolvedExpression {
  public static final AllFields INSTANCE = new AllFields();

  private AllFields() {}

  public static AllFields of() {
    return INSTANCE;
  }

  @Override
  public List<? extends Node> getChild() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAllFields(this, context);
  }
}

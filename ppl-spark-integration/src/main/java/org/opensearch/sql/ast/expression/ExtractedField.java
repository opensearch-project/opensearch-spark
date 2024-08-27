/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

public class ExtractedField extends UnresolvedExpression {
  private final QualifiedName field;
  private final Literal extractPath;

  /** Constructor of ExtractedField. */
  public ExtractedField(QualifiedName field, Literal extractPath) {
    this.field = field;
    this.extractPath = extractPath;
  }

  public QualifiedName getField() {
    return field;
  }

  public Literal getExtractPath() {
    return extractPath;
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of(this.field);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitExtractedField(this, context);
  }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Field extends UnresolvedExpression {
  private final QualifiedName field;
  private final List<Argument> fieldArgs;

  /** Constructor of Field. */
  public Field(QualifiedName field) {
    this(field, Collections.emptyList());
  }

  /** Constructor of Field. */
  public Field(QualifiedName field, List<Argument> fieldArgs) {
    this.field = field;
    this.fieldArgs = fieldArgs;
  }

  public QualifiedName getField() {
    return field;
  }

  public List<Argument> getFieldArgs() {
    return fieldArgs;
  }

  public boolean hasArgument() {
    return !fieldArgs.isEmpty();
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of(this.field);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitField(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Field field1 = (Field) o;
    return Objects.equals(field, field1.field) && Objects.equals(fieldArgs, field1.fieldArgs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, fieldArgs);
  }

  @Override
  public String toString() {
    return "Field(" +
        "field=" + field +
        ", fieldArgs=" + fieldArgs +
        ')';
  }
}

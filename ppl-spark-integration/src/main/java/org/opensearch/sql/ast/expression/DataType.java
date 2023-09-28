/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import org.opensearch.sql.data.type.ExprCoreType;

/** The DataType defintion in AST. Question, could we use {@link ExprCoreType} directly in AST? */

public enum DataType {
  TYPE_ERROR(ExprCoreType.UNKNOWN),
  NULL(ExprCoreType.UNDEFINED),

  INTEGER(ExprCoreType.INTEGER),
  LONG(ExprCoreType.LONG),
  SHORT(ExprCoreType.SHORT),
  FLOAT(ExprCoreType.FLOAT),
  DOUBLE(ExprCoreType.DOUBLE),
  STRING(ExprCoreType.STRING),
  BOOLEAN(ExprCoreType.BOOLEAN),

  DATE(ExprCoreType.DATE),
  TIME(ExprCoreType.TIME),
  TIMESTAMP(ExprCoreType.TIMESTAMP),
  INTERVAL(ExprCoreType.INTERVAL);

   private final ExprCoreType coreType;

  DataType(ExprCoreType type) {
      this.coreType = type;
  }

  public ExprCoreType getCoreType() {
    return coreType;
  }
}

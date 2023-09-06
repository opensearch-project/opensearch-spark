/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/**
 * Expression node of literal type Params include literal value (@value) and literal data type
 * (@type) which can be selected from {@link DataType}.
 */

public class Literal extends UnresolvedExpression {

    private Object value;
    private DataType type;

    public Literal(Object value, DataType dataType) {
        this.value = value;
        this.type = dataType;
    }

    @Override
    public List<UnresolvedExpression> getChild() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitLiteral(this, context);
    }

    public Object getValue() {
        return value;
    }

    public DataType getType() {
        return type;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}

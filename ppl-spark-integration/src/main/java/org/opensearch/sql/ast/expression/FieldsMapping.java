/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

@Getter
@RequiredArgsConstructor
public class FieldsMapping extends UnresolvedExpression {

    private final List<UnresolvedExpression> fieldsMappingList;

    public List<UnresolvedExpression> getChild() {
        return fieldsMappingList;
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitCorrelationMapping(this, context);
    }
}

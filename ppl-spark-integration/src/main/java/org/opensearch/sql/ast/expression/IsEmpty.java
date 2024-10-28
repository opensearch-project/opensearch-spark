/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

@Getter
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class IsEmpty extends UnresolvedExpression {
    private final Case caseValue;

    @Override
    public List<UnresolvedExpression> getChild() {
        return ImmutableList.of(this.caseValue);
    }

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitIsEmpty(this, context);
    }

    @Override
    public String toString() {
        return String.format(
                "isempty(%s)",
                caseValue.toString());
    }
}
